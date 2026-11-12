#!/usr/bin/env python3
"""AgentRoom outbound connector. No public port, webhook key, or idle AI runs.

Keep this next to room_client.py. The locally configured command only requests
a wake; the agent must fetch, handle and report through its existing client.
"""
import argparse
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID, uuid4

import httpx

try:
    from .room_client import ClientError, Credentials, InboxStore, RoomClient
except ImportError:
    from room_client import ClientError, Credentials, InboxStore, RoomClient

VERSION = '1.7'


class WakeStore(InboxStore):
    def __init__(self, path, url, member_id):
        super().__init__(path,url,member_id)
        self.conn.execute('''CREATE TABLE IF NOT EXISTS outbound_wakes (
            event_id TEXT PRIMARY KEY,payload TEXT NOT NULL,attempted_at REAL,hook_status INTEGER)''')
        self.conn.commit()

    def save_wake(self, value):
        # Treat all network input as untrusted metadata. Never execute it.
        if not isinstance(value,dict) or set(value)!={'event_id','delivery_id','cycle','member_id','seq','type'}:
            raise ClientError('Invalid wake metadata',3)
        for key in ['event_id','delivery_id']:
            UUID(value[key])
        if (value['member_id']!=self.member_id or type(value['cycle']) is not int or value['cycle']<0
                or (value['seq'] is not None and (type(value['seq']) is not int or value['seq']<=0))
                or value['type'] not in {'message.available','webhook.test'}):
            raise ClientError('Invalid wake identity or metadata',3)
        encoded=json.dumps(value,sort_keys=True,separators=(',',':'))
        with self.conn:
            prior=self.conn.execute('SELECT payload FROM outbound_wakes WHERE event_id=?',(value['event_id'],)).fetchone()
            if prior and prior['payload']!=encoded:
                raise ClientError('Wake event changed; stop for inspection',3)
            self.conn.execute('INSERT OR IGNORE INTO outbound_wakes(event_id,payload) VALUES(?,?)',(value['event_id'],encoded))

    def trigger(self, command, event_ids):
        if not event_ids:
            return
        rows=self.conn.execute('SELECT event_id,payload FROM outbound_wakes WHERE attempted_at IS NULL AND event_id IN ('+
                               ','.join('?' for _ in event_ids)+')',event_ids).fetchall()
        if not rows:
            return
        # Persist the attempt before calling the hook. A crash after launch cannot
        # repeatedly start AI sessions. Server recovery issues a fresh bounded wake.
        with self.conn:
            self.conn.executemany('UPDATE outbound_wakes SET attempted_at=? WHERE event_id=?',[(time.time(),r['event_id']) for r in rows])
        try:
            result=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL,timeout=30,check=False,shell=False)
            status=result.returncode
        except (OSError,subprocess.TimeoutExpired):
            status=-1
        with self.conn:
            self.conn.executemany('UPDATE outbound_wakes SET hook_status=? WHERE event_id=?',[(status,r['event_id']) for r in rows])
        print('wake hook accepted' if status==0 else 'wake hook failed; server recovery remains responsible',file=sys.stderr,flush=True)


def call(client, method, path, body=None):
    with client.credentials.locked():
        client.data=client.credentials.load()
        client.check_identity()
        return client.request(method,path,body,retries=not path.endswith('/received'))


def flush(client, sid, store, command):
    rows=store.conn.execute('SELECT event_id,payload FROM outbound_wakes WHERE attempted_at IS NULL LIMIT 100').fetchall()
    accepted=[]
    for row in rows:
        value=json.loads(row['payload'])
        try:
            call(client,'POST','/v1/connector/outbound/'+sid+'/received',
                 {k:value[k] for k in ['delivery_id','event_id','cycle']})
            accepted.append(row['event_id'])
        except ClientError as error:
            if error.exit_code!=4:
                raise
            # Already handled, cancelled or superseded; don't launch a stale hint.
            with store.conn:
                store.conn.execute('UPDATE outbound_wakes SET attempted_at=?,hook_status=-2 WHERE event_id=?',
                                   (time.time(),row['event_id']))
    store.trigger(command,accepted)


def stream_once(client, sid, store, command):
    with client.credentials.locked():
        client.data=client.credentials.load()
        client.check_identity()
        if client.data.get('auth_failed'):
            raise ClientError('Sign-in needs repair; keep the saved credentials',3)
        if client.data.get('expires_at',0)<=time.time()+300:
            client.refresh()
        token=client.data['access_token']
    url=client.data['url']+'/v1/connector/outbound/'+sid+'/events'
    # Honor the host's HTTPS proxy. Never follow a redirect with room credentials.
    with client.http.stream('GET',url,headers={'Authorization':'Bearer '+token,'Accept':'text/event-stream'},
                            timeout=httpx.Timeout(60,connect=15)) as response:
        if response.status_code==401:
            client.refresh()
            return False
        if response.status_code in {403,404}:
            raise ClientError('Connector paused, revoked, or removed; keep recovery and inspect connector-status',3)
        if response.status_code!=200:
            raise ClientError('Connector temporarily unavailable')
        if 'text/event-stream' not in response.headers.get('content-type',''):
            raise ClientError('Expected an event stream; inspect the network proxy',3)
        started=time.monotonic()
        event=None
        count=0
        for line in response.iter_lines():
            if len(line)>8192:
                raise ClientError('Oversized wake metadata',3)
            if line.startswith('event: '):
                event=line[7:]
            elif line.startswith('data: ') and event=='wake':
                value=json.loads(line[6:])
                store.save_wake(value)  # fsync before the transport receipt
                count+=1
                if count>=100:
                    flush(client,sid,store,command); count=0
            elif line==': heartbeat':
                flush(client,sid,store,command)  # Coalesce a backlog into one local wake.
                count=0
            elif event=='unavailable':
                return False
        flush(client,sid,store,command)
        return time.monotonic()-started>=20


def run(client, config, state):
    sid=config['subscription_id']
    command=config['wake_command']
    store=WakeStore(state,client.data['url'],client.data['member_id'])
    try:
        with store.listener_lock():
            failures=0
            while True:
                try:
                    healthy=stream_once(client,sid,store,command)
                    failures=0 if healthy else failures+1
                except ClientError as error:
                    if error.exit_code in {2,3,4}:
                        raise
                    failures+=1
                except (httpx.TransportError,ValueError,KeyError):
                    failures+=1
                # Refresh/reconnect never starts an AI session.
                delay=1 if not failures else min(60,2**min(failures,6))
                time.sleep(random.uniform(delay,delay*1.3))
    finally:
        store.close()


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version',action='version',version=VERSION)
    parser.add_argument('--credentials')
    parser.add_argument('--config',default='~/.config/agent-room/connector.json')
    commands=parser.add_subparsers(dest='command',required=True)
    init=commands.add_parser('init',help='Link this host; does not invoke its wake hook')
    init.add_argument('--wake-command',nargs=argparse.REMAINDER,required=True)
    commands.add_parser('run')
    commands.add_parser('status')
    args=parser.parse_args(argv)
    client=None
    try:
        credentials=Credentials(args.credentials)
        with credentials.locked(): credentials.load()
        client=RoomClient(credentials)
        if not all(client.data.get(k) for k in ['url','member_id','access_token']):
            raise ClientError('Connect this agent with room_client.py first',3)
        parsed=httpx.URL(client.data['url'])
        if parsed.scheme!='https' or parsed.userinfo or parsed.query or parsed.fragment or parsed.path not in {'','/'}:
            raise ClientError('Use a plain HTTPS room URL',3)
        client.bound_identity=(client.data['url'].rstrip('/'),client.data['member_id'])
        configuration=Credentials(args.config)
        with configuration.locked():
            config=configuration.load()
            if config and (config.get('url'),config.get('member_id'))!=client.bound_identity:
                raise ClientError('Connector belongs to another room or member',3)
            if args.command=='init':
                command=args.wake_command
                if not command or not Path(command[0]).is_absolute() or not os.access(command[0],os.X_OK):
                    raise ClientError('Give the actual local wake hook executable as an absolute path, followed by its fixed arguments',2)
                if not config:
                    config.update(url=client.bound_identity[0],member_id=client.bound_identity[1],device_id=str(uuid4()))
                    configuration.save()  # Stable device ID before an idempotent registration.
                result=call(client,'POST','/v1/connector/outbound',{'device_id':config['device_id']})
                config.update(subscription_id=result['subscription_id'],wake_command=command)
                configuration.save()
                print(json.dumps({'status':'configured','subscription_id':result['subscription_id'],'next':'Supervise the run command; keep existing recovery until live verification.'}))
                return
        if args.command=='status':
            print(json.dumps(call(client,'GET','/v1/connector/status'),indent=2))
            return
        if not config.get('subscription_id') or not config.get('wake_command'):
            raise ClientError('Run init with your actual local wake hook first',2)
        run(client,config,configuration.path.with_suffix('.sqlite3'))
    except ClientError as error:
        print(str(error),file=sys.stderr)
        return error.exit_code
    except KeyboardInterrupt:
        return 0
    except (OSError,ValueError,KeyError,TypeError):
        print('Invalid private connector configuration; preserve it and inspect locally',file=sys.stderr)
        return 2
    finally:
        if client: client.close()
    return 0


if __name__=='__main__':
    sys.exit(main())

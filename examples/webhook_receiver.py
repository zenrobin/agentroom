#!/usr/bin/env python3
"""Signed webhook ingress for a persistent host behind your existing HTTPS proxy.

Saves notifications durably; room_client listen --transport webhook reconciles
the authorized inbox. This program does not execute messages or wake an LLM.
"""
import argparse
import os
from pathlib import Path
import re
import stat
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from room_client import ClientError, Credentials, InboxStore, verify_webhook
except ModuleNotFoundError:
    from clients.room_client import ClientError, Credentials, InboxStore, verify_webhook


def read_secret(path):
    path=Path(path).expanduser()
    fd=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(fd) as f:
        info=os.fstat(f.fileno())
        if info.st_uid!=os.getuid() or info.st_mode&0o077 or not stat.S_ISREG(info.st_mode) or path.parent.stat().st_mode&0o077:
            raise ClientError('Webhook secret must be owned by you, regular, mode 0600, in a private directory',3)
        secret=f.read(256).strip()
    if not re.fullmatch(r'arwh_[A-Za-z0-9_-]{43}',secret):
        raise ClientError('Invalid webhook signing secret file',3)
    return secret


class Receiver(BaseHTTPRequestHandler):
    def log_message(self,*args):
        pass  # No request body, tokens, secrets, or URL query logging.

    def reply(self,code):
        self.send_response(code)
        self.send_header('Content-Length','0')
        self.send_header('Cache-Control','no-store')
        self.end_headers()

    def do_GET(self):
        self.reply(204 if self.path=='/healthz' else 404)

    def do_POST(self):
        if self.path!='/agent-room':
            return self.reply(404)
        try:
            length=int(self.headers.get('Content-Length','-1'))
            if self.headers.get('Transfer-Encoding') or not 0<length<=8192:
                return self.reply(413)
            if self.headers.get('Content-Type','').split(';')[0].strip().lower()!='application/json':
                return self.reply(415)
            self.connection.settimeout(5)
            raw=self.rfile.read(length)
            if len(raw)!=length:
                return self.reply(400)
            payload=verify_webhook(read_secret(self.server.secret_file),dict(self.headers),raw)
            store=InboxStore(self.server.spool,self.server.room_url,self.server.member_id)
            try:
                store.notify(payload)
            finally:
                store.close()
            return self.reply(204)
        except ClientError:
            return self.reply(401)
        except Exception:
            return self.reply(503)  # No 2xx unless the notification was committed.


def server(bind,port,spool,room_url,member_id,secret_file):
    read_secret(secret_file)
    store=InboxStore(spool,room_url,member_id);store.close()
    http=ThreadingHTTPServer((bind,port),Receiver)
    http.spool,http.room_url,http.member_id,http.secret_file=spool,room_url,member_id,secret_file
    http.daemon_threads=True
    return http


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--secret-file',required=True)
    parser.add_argument('--spool')
    parser.add_argument('--bind',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8787)
    args=parser.parse_args()
    creds=Credentials()
    with creds.locked():
        data=creds.load()
    http=server(args.bind,args.port,args.spool or creds.path.parent/'inbox.sqlite3',data['url'],data['member_id'],args.secret_file)
    try:
        http.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        http.server_close()


if __name__=='__main__':
    main()

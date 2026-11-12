import hashlib
import hmac
import json
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from clients import room_client as cli
from examples.webhook_receiver import server
from tests.conftest import live_enroll, message


def credentials(tmp_path, member, url):
    creds=cli.Credentials(tmp_path/'agent'/'credentials.json')
    creds.data={'url':url,'member_id':member['member_id'],'access_token':member['access_token'],
                'refresh_token':member['refresh_token'],'expires_at':time.time()+900}
    creds.save()
    return creds


def test_inbox_is_private_identity_bound_idempotent_and_never_reopens_completed(tmp_path):
    path=tmp_path/'private'/'inbox.sqlite3'
    store=cli.InboxStore(path,'https://room.test','a')
    item={**message(),'seq':7}
    assert store.save([item])==7
    store.complete(item['id']);store.save([item])
    assert store.pending()==[]
    with pytest.raises(cli.ClientError,match='Conflicting'):
        store.save([{**item,'body':{'text':'changed'}}])
    assert path.stat().st_mode&0o777==0o600
    with pytest.raises(cli.ClientError,match='another room'):
        cli.InboxStore(path,'https://other.test','a')
    with store.listener_lock():
        other=cli.InboxStore(path,'https://room.test','a')
        with pytest.raises(cli.ClientError,match='Another listener'):
            with other.listener_lock():pass
        other.close()
    store.close()
    link=path.parent/'link.sqlite3';link.symlink_to(path)
    with pytest.raises(OSError):cli.InboxStore(link,'https://room.test','a')


def test_save_entire_page_rolls_back_if_later_item_is_invalid(tmp_path):
    store=cli.InboxStore(tmp_path/'inbox'/'q.db','https://room.test','m')
    with pytest.raises(cli.ClientError):store.save([{**message(),'seq':1},{**message(),'seq':0}])
    assert store.pending()==[]
    store.close()


def test_sender_scoped_message_ids_cannot_block_the_inbox(tmp_path):
    store=cli.InboxStore(tmp_path/'inbox'/'q.db','https://room.test','m')
    original={**message(),'seq':1,'from':'first'}
    copied={**original,'seq':2,'from':'other','body':{'text':'Different sender using the same UUID'}}
    store.save([original,copied]);assert len(store.pending())==2
    with pytest.raises(cli.ClientError,match='shared by different senders'):store.complete(original['id'])
    store.complete(seq=1);assert store.pending()==[copied]
    store.close()


@pytest.mark.parametrize('force_refresh',[False,True])
def test_listener_never_acks_or_refreshes_another_identity_after_unlocked_poll(tmp_path,force_refresh):
    creds=credentials(tmp_path,{'member_id':'a','access_token':'a-access','refresh_token':'a-refresh'},'https://room.test')
    store=cli.InboxStore(creds.path.parent/'inbox.sqlite3','https://room.test','a')
    requests=[]
    item={**message(),'seq':4}
    def transport(request):
        requests.append((str(request.url),request.headers.get('authorization')))
        assert request.url.path=='/v1/inbox'
        # Another legitimate process changed the file while this GET had no lock.
        replacement=cli.Credentials(creds.path)
        replacement.data={'url':'https://other.test','member_id':'b','access_token':'b-access','refresh_token':'b-refresh','expires_at':0}
        with replacement.locked():replacement.save()
        return httpx.Response(401 if force_refresh else 200,json={'items':[item],'has_more':False})
    agent=cli.RoomClient(creds,transport=httpx.MockTransport(transport))
    with pytest.raises(cli.ClientError,match='changed room or identity'):
        agent.listen(cli.parser().parse_args(['listen','--once']),store)
    assert requests==[('https://room.test/v1/inbox?limit=200&wait=120','Bearer a-access')]
    saved=json.loads(creds.path.read_text());assert saved['member_id']=='b' and 'auth_failed' not in saved
    assert store.pending()==([] if force_refresh else [item])
    agent.close();store.close()


def test_listener_survives_lost_ack_without_duplicate_local_work(tmp_path):
    item={**message(),'seq':1};acked=False;ack_attempts=0
    creds=credentials(tmp_path,{'member_id':'a','access_token':'access','refresh_token':'refresh'},'https://room.test')
    store=cli.InboxStore(creds.path.parent/'inbox.sqlite3','https://room.test','a')
    def transport(request):
        nonlocal acked,ack_attempts
        if request.url.path=='/v1/inbox':return httpx.Response(200,json={'items':[] if acked else [item],'has_more':False})
        if request.url.path=='/v1/connector/receipts':
            assert store.pending()==[] and acked
            assert json.loads(request.content)['receipts']==[{'seq':1,'message_id':item['id'],'outcome':'handled'}]
            return httpx.Response(200,json={'accepted':[1],'unavailable':[]})
        assert store.pending()==[item]  # The durable transaction completed before ack.
        ack_attempts+=1
        if ack_attempts==1:raise httpx.ConnectError('lost ack',request=request)
        acked=True
        return httpx.Response(200,json={'acked_seq':1})
    client=cli.RoomClient(creds,transport=httpx.MockTransport(transport),sleep=lambda _:None)
    client.listen(cli.parser().parse_args(['listen','--once']),store)
    assert len(store.pending())==1 and acked
    store.complete(item['id']);client.listen(cli.parser().parse_args(['listen','--once']),store)
    assert store.pending()==[]
    client.close();store.close()


def test_local_disk_failure_never_acknowledges(tmp_path,monkeypatch):
    creds=credentials(tmp_path,{'member_id':'a','access_token':'access','refresh_token':'refresh'},'https://room.test')
    store=cli.InboxStore(creds.path.parent/'inbox.sqlite3','https://room.test','a')
    requests=[]
    def transport(request):
        requests.append(request.url.path)
        return httpx.Response(200,json={'items':[{**message(),'seq':1}],'has_more':False})
    client=cli.RoomClient(creds,transport=httpx.MockTransport(transport))
    monkeypatch.setattr(store,'save',lambda _:(_ for _ in ()).throw(OSError('disk full')))
    with pytest.raises(OSError):client.listen(cli.parser().parse_args(['listen','--once']),store)
    assert requests==['/v1/inbox']
    client.close();store.close()


def test_listener_does_not_hold_credentials_lock_during_longpoll(live,owner,tmp_path,monkeypatch):
    a,b=live_enroll(live,owner,'listener-a'),live_enroll(live,owner,'listener-b')
    creds=credentials(tmp_path,a,str(live.base_url).rstrip('/'))
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS',str(creds.path))
    assert cli.main(['listen','--once','--wait','0'])==0
    with ThreadPoolExecutor(1) as pool:
        waiting=pool.submit(cli.main,['listen','--once','--wait','2'])
        time.sleep(.05)
        started=time.monotonic()
        assert cli.main(['send','--to','listener-b','--text','Listener must not block sending'])==0
        assert time.monotonic()-started<1
        live.post('/v1/messages',headers=b['headers'],json=message(to=[a['member_id']]))
        assert waiting.result(timeout=3)==0


def test_signed_receiver_persists_deduplicates_and_rejects_wrong_member(tmp_path):
    path=tmp_path/'receiver';path.mkdir(mode=0o700)
    secret='arwh_'+secrets.token_urlsafe(32);key=path/'secret';key.write_text(secret);key.chmod(0o600)
    http=server('127.0.0.1',0,path/'inbox.db','https://room.test','member-a',key)
    thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
    url='http://127.0.0.1:'+str(http.server_port)+'/agent-room'
    payload={'event_id':'event-1','type':'message.available','member_id':'member-a','message':{'seq':1}}
    def signed(payload):
        raw=json.dumps(payload).encode();stamp=str(int(time.time()))
        headers={'Content-Type':'application/json','X-AR-Timestamp':stamp,'X-AR-Event-Id':payload['event_id'],
                 'X-AR-Signature':'sha256='+hmac.new(secret.encode(),stamp.encode()+b'.'+raw,hashlib.sha256).hexdigest()}
        return raw,headers
    try:
        raw,headers=signed(payload)
        assert httpx.post(url,content=raw,headers={'Content-Type':'application/json'}).status_code==401
        for _ in range(2):assert httpx.post(url,content=raw,headers=headers).status_code==204
        other,other_headers=signed({**payload,'member_id':'member-b'})
        assert httpx.post(url,content=other,headers=other_headers).status_code==401
        store=cli.InboxStore(path/'inbox.db','https://room.test','member-a')
        assert store.notifications()==['event-1']
        store.handled(['event-1']);store.notify(payload)
        assert store.notifications()==[]
        store.close()
    finally:
        http.shutdown();http.server_close();thread.join(3)

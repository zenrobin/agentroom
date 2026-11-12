import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app import db, watchdog, webhooks
from tests.test_providers import grok


@pytest.fixture
def pilot(client,enroll,send,conn,settings,monkeypatch):
    monkeypatch.setattr(webhooks,'post',AsyncMock(return_value=200))
    monkeypatch.setattr(watchdog,'kick',AsyncMock())
    a,b=enroll(),enroll()
    sid=grok(client,b)['subscription_id']
    item=send(a,to=[b['member_id']]).json()
    return a,b,sid,item


def advance(monkeypatch,seconds):
    now=db.utcnow()+timedelta(seconds=seconds)
    monkeypatch.setattr(db,'utcnow',lambda:now)


def report(conn,member,item,outcome='handled'):
    conn.execute('INSERT INTO processing_receipts VALUES(?,?,?,?,?)',(member['member_id'],item['seq'],item['id'],outcome,db.stamp()))


def work(conn):
    return db.rows(conn,'SELECT * FROM connector_work ORDER BY message_seq')


@pytest.mark.parametrize('outcome',['handled','awaiting_approval'])
def test_receipt_stops_recovery_and_idle_checks_never_wake(pilot,conn,settings,monkeypatch,outcome):
    _,b,_,item=pilot
    report(conn,b,item,outcome)
    advance(monkeypatch,301)
    for _ in range(3): watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['status']=='resolved'
    assert work(conn)[0]['retries']==0
    assert db.one(conn,'SELECT next_at FROM connector_watchdogs')['next_at'] is None
    assert not db.one(conn,'SELECT * FROM connector_retry_wakes')


def test_missing_receipts_coalesce_and_duplicate_jobs_do_not_spend_budget(pilot,conn,send,settings,monkeypatch):
    a,b,_,_=pilot
    for _ in range(2): assert send(a,to=[b['member_id']]).status_code==201
    original=[json.loads(r['payload'])['event_id'] for r in db.rows(conn,'SELECT payload FROM webhook_deliveries')]
    advance(monkeypatch,301)
    def run():
        c=db.connect(settings.db_path)
        try: watchdog.process(c,b['member_id'])
        finally: c.close()
    with ThreadPoolExecutor(max_workers=3) as pool: list(pool.map(lambda _:run(),range(3)))
    assert [w['retries'] for w in work(conn)]==[1,1,1]
    assert len(db.rows(conn,'SELECT * FROM connector_retry_wakes'))==1
    pending=db.rows(conn,"SELECT * FROM webhook_deliveries WHERE status='pending'")
    assert len(pending)==1 and json.loads(pending[0]['payload'])['event_id'] not in original
    assert 'body' not in json.loads(pending[0]['payload'])


def test_retry_is_suppressed_when_receipt_arrives_after_queueing(pilot,conn,settings,monkeypatch):
    _,b,_,item=pilot
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    row=db.one(conn,"SELECT * FROM webhook_deliveries WHERE status='pending'")
    report(conn,b,item)
    webhooks.post.reset_mock()
    asyncio.run(webhooks.deliver(settings,row['delivery_id']))
    webhooks.post.assert_not_called()
    assert db.one(conn,'SELECT last_error FROM webhook_deliveries')['last_error']=='handling_resolved'


def test_transport_retry_is_suppressed_after_handling(pilot,client,conn,settings,monkeypatch):
    _,b,_,item=pilot
    conn.execute("UPDATE webhook_deliveries SET status='pending',attempts=1,completed_at=NULL,last_status=503,next_at=?",(db.stamp(),))
    report(conn,b,item)
    webhooks.post.reset_mock()
    row=db.one(conn,'SELECT * FROM webhook_deliveries')
    asyncio.run(webhooks.deliver(settings,row['delivery_id']))
    webhooks.post.assert_not_called()
    assert db.one(conn,'SELECT * FROM webhook_deliveries')['last_error']=='handling_resolved'
    assert client.post('/v1/connector/ready',headers=b['headers'],json={'provider':'grok','recovery_interval_s':1800,
        'recovery_verified_at':db.stamp()}).status_code==200
    assert client.get('/v1/connector/status',headers=b['headers']).json()['state']=='processing_reported'


@pytest.mark.parametrize('late',['receipt','busy'])
def test_receipt_or_busy_lease_arriving_between_claim_and_send(pilot,conn,settings,monkeypatch,late):
    _,b,_,_=pilot
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    row=db.one(conn,"SELECT * FROM webhook_deliveries WHERE status='pending'")
    gate='handling_resolved' if late=='receipt' else db.stamp(db.utcnow()+timedelta(seconds=900))
    from unittest.mock import Mock
    monkeypatch.setattr(watchdog,'before_delivery',Mock(side_effect=[None,gate]))
    webhooks.post.reset_mock()
    result=asyncio.run(webhooks.deliver(settings,row['delivery_id']))
    webhooks.post.assert_not_called()
    after=db.one(conn,'SELECT * FROM webhook_deliveries')
    if late=='receipt':
        assert after['status']=='cancelled' and after['last_error']=='handling_resolved'
    else:
        assert after['status']=='pending' and after['attempts']==0 and after['next_at']==gate
        assert result['_waiting']


def test_retry_budget_exhausts_and_never_resets_itself(pilot,conn,settings,monkeypatch):
    _,b,_,_=pilot
    for expected in range(1,4):
        advance(monkeypatch,1900)
        watchdog.process(conn,b['member_id'])
        row=db.one(conn,'SELECT * FROM webhook_deliveries')
        assert work(conn)[0]['retries']==expected
        asyncio.run(webhooks.deliver(settings,row['delivery_id']))
    advance(monkeypatch,1900)
    for _ in range(3): watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['status']=='attention' and work(conn)[0]['retries']==3
    assert watchdog.status(conn,b['member_id'])['needs_attention']==1
    assert db.one(conn,'SELECT next_at FROM connector_watchdogs')['next_at'] is None


@pytest.mark.parametrize('change',[
    "UPDATE webhook_subscriptions SET enabled=0",
    "UPDATE webhook_subscriptions SET generation=generation+1",
    "UPDATE webhook_subscriptions SET expires_at='2000-01-01T00:00:00.000Z'",
    "UPDATE members SET status='removed' WHERE member_id IN (SELECT member_id FROM connector_watchdogs)"])
def test_current_access_rechecked_before_retry(pilot,conn,monkeypatch,change):
    _,b,_,_=pilot
    conn.execute(change);advance(monkeypatch,301)
    watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['status']=='cancelled'
    assert not db.one(conn,'SELECT * FROM connector_retry_wakes')


def test_transport_still_pending_gets_no_second_wake_and_grace_starts_at_acceptance(pilot,conn,monkeypatch):
    _,b,_,_=pilot
    conn.execute("UPDATE webhook_deliveries SET status='pending',completed_at=NULL")
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['retries']==0
    advance(monkeypatch,301)
    conn.execute("UPDATE webhook_deliveries SET status='delivered',completed_at=?",(db.stamp(),))
    watchdog.process(conn,b['member_id']);assert work(conn)[0]['retries']==0
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['retries']==1


def test_processing_lease_is_owner_scoped_and_recovers_after_crash(pilot,client,conn,monkeypatch):
    a,b,_,_=pilot
    run,other=str(uuid4()),str(uuid4())
    path='/v1/connector/activity'
    assert client.post(path,headers=a['headers'],json={'run_id':run,'active':True}).status_code==409
    assert client.post(path,headers=b['headers'],json={'run_id':run,'active':True}).status_code==200
    assert client.post(path,headers=b['headers'],json={'run_id':other,'active':True}).status_code==409
    assert client.post(path,headers=b['headers'],json={'run_id':other,'active':False}).status_code==200
    assert db.one(conn,'SELECT busy_run_id FROM connector_watchdogs')['busy_run_id']==run
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['retries']==0
    advance(monkeypatch,601);watchdog.process(conn,b['member_id'])
    assert work(conn)[0]['retries']==1


def test_failed_publish_remains_recoverable_and_sweep_republishes_overdue(pilot,conn,settings,monkeypatch):
    _,b,_,_=pilot
    state=db.one(conn,'SELECT * FROM connector_watchdogs')
    remote=replace(settings,serverless=True)
    send=AsyncMock(side_effect=RuntimeError('queue unavailable'))
    monkeypatch.setattr('vercel.queue.QueueClient.send',send)
    with pytest.raises(RuntimeError): asyncio.run(watchdog.dispatch(remote,state))
    assert db.one(conn,'SELECT enqueued_revision FROM connector_watchdogs')['enqueued_revision']==-1
    send.side_effect=None
    asyncio.run(watchdog.dispatch(remote,state))
    assert db.one(conn,'SELECT enqueued_revision FROM connector_watchdogs')['enqueued_revision']==state['revision']
    ordinary=send.call_args.kwargs['idempotency_key']
    advance(monkeypatch,700)
    asyncio.run(watchdog.dispatch(remote,state,rescue=True))
    assert send.call_args.kwargs['idempotency_key']!=ordinary
    assert send.call_args.args[1]=={'member_id':b['member_id']}


def test_server_heartbeat_is_one_delayed_code_job_not_an_agent_wake(client,conn,settings,monkeypatch):
    sender=AsyncMock()
    monkeypatch.setattr('vercel.queue.QueueClient.send',sender)
    remote=replace(settings,serverless=True)
    asyncio.run(watchdog.ensure_sweep(remote));asyncio.run(watchdog.ensure_sweep(remote))
    assert sender.call_count==1
    assert sender.call_args.args==(watchdog.TOPIC,{'sweep':True})
    assert 0<sender.call_args.kwargs['delay']<=300
    advance(monkeypatch,301)
    asyncio.run(watchdog.consume(remote,{'sweep':True}))
    assert sender.call_count==2
    assert db.one(conn,"SELECT * FROM operations_leases WHERE name='watchdog_sweep_observed'")


def test_server_mode_requires_fresh_handling_server_health_and_full_coverage(pilot,client,conn):
    _,b,_,item=pilot
    path='/v1/connector/server-recovery'
    assert client.post(path,headers=b['headers'],json={}).status_code==409
    report(conn,b,item)
    assert client.post(path,headers=b['headers'],json={}).status_code==409
    conn.execute("INSERT INTO operations_leases VALUES('watchdog_sweep_observed',?)",(db.stamp(),))
    result=client.post(path,headers=b['headers'],json={})
    assert result.status_code==200,result.text
    assert result.json()['recovery']['can_disable_scheduled_recovery']
    conn.execute("UPDATE webhook_subscriptions SET types='[\"chat\"]'")
    assert not watchdog.status(conn,b['member_id'])['can_disable_scheduled_recovery']
    assert client.post(path,headers=b['headers'],json={}).status_code==409


def test_migration_pruning_and_recovery_preserve_identity(pilot,conn,settings):
    _,b,_,item=pilot
    before=db.member(conn,b['member_id'])
    db.initialize(settings.db_path);db.initialize(settings.db_path)
    assert db.ready(conn) and db.member(conn,b['member_id'])==before
    conn.execute('DELETE FROM messages WHERE seq=?',(item['seq'],))
    assert work(conn)==[]
    watchdog.process(conn,b['member_id'])
    with db.transaction(conn): db.revoke_member(conn,b['member_id'])
    assert not db.one(conn,'SELECT * FROM connector_watchdogs')


@pytest.mark.parametrize('change',['configure','pause_resume','replace'])
def test_changed_routine_requires_new_current_generation_proof(pilot,client,conn,change):
    from tests.test_providers import ROUTINE,KEY
    _,b,sid,item=pilot
    report(conn,b,item)
    conn.execute("INSERT INTO operations_leases VALUES('watchdog_sweep_observed',?)",(db.stamp(),))
    path='/v1/connector/server-recovery'
    assert client.post(path,headers=b['headers'],json={}).status_code==200
    base='/v1/owner/webhooks/'+sid
    owner={'Authorization':'Bearer '+client.app.state.settings.owner_key}
    if change=='configure':
        assert client.post(base+'/configure',headers=owner,json={'url':ROUTINE,'authorization':KEY}).status_code==200
    else:
        assert client.post(base+('/delete' if change=='replace' else '/pause'),headers=owner,json={}).status_code==200
        if change=='replace': grok(client,b)
        else: assert client.post(base+'/resume',headers=owner,json={}).status_code==200
    assert not watchdog.status(conn,b['member_id'])['can_disable_scheduled_recovery']
    assert client.post(path,headers=b['headers'],json={}).status_code==409


def test_readiness_cannot_transfer_to_an_untested_second_grant(pilot,client,conn):
    _,b,sid,item=pilot
    grok(client,b)
    report(conn,b,item)
    conn.execute("INSERT INTO operations_leases VALUES('watchdog_sweep_observed',?)",(db.stamp(),))
    path='/v1/connector/server-recovery'
    assert client.post(path,headers=b['headers'],json={}).status_code==200
    conn.execute("UPDATE webhook_subscriptions SET expires_at='2000-01-01T00:00:00.000Z' WHERE subscription_id=?",(sid,))
    state=watchdog.status(conn,b['member_id'])
    assert state['full_inbox_coverage'] and not state['can_disable_scheduled_recovery']
    assert client.post(path,headers=b['headers'],json={}).status_code==409


def test_handling_suppresses_other_grok_grants_transport_retry(pilot,client,conn,send,settings):
    a,b,_,_=pilot
    second=grok(client,b)['subscription_id']
    item=send(a,to=[b['member_id']]).json()
    row=db.one(conn,'SELECT * FROM webhook_deliveries WHERE subscription_id=? AND message_seq=?',(second,item['seq']))
    conn.execute("UPDATE webhook_deliveries SET status='pending',attempts=1,last_status=503,next_at=? WHERE delivery_id=?",(db.stamp(),row['delivery_id']))
    report(conn,b,item)
    webhooks.post.reset_mock()
    asyncio.run(webhooks.deliver(settings,row['delivery_id']))
    webhooks.post.assert_not_called()


@pytest.mark.parametrize('mode,expected_retries',[('recover',1),('already_handled',0)])
def test_sandbox_signed_probe_exercises_real_recovery_without_an_ai(settings,monkeypatch,mode,expected_retries):
    from fastapi.testclient import TestClient
    from app.main import create_app
    import hashlib,hmac
    config=replace(settings,profile='sandbox',webhook_url=None,webhook_secret=None,serverless=True)
    monkeypatch.setattr('vercel.queue.QueueClient.send',AsyncMock())
    with TestClient(create_app(config)) as client:
        owner={'Authorization':'Bearer '+config.owner_key}
        started=client.post('/v1/owner/delivery/probes',headers=owner,json={'watchdog_mode':mode})
        assert started.status_code==202,started.text
        cid=started.json()['check_id'];conn=db.connect(config.db_path)
        probe=db.one(conn,'SELECT * FROM delivery_probes WHERE check_id=?',(cid,))
        async def receiver(settings,sub,payload):
            secret=webhooks.cipher(settings).decrypt(sub['secret'].encode());ts=str(int(db.utcnow().timestamp()))
            headers={'Content-Type':'application/json','X-AR-Timestamp':ts,'X-AR-Event-Id':json.loads(payload)['event_id'],
                     'X-AR-Signature':'sha256='+hmac.new(secret,ts.encode()+b'.'+payload.encode(),hashlib.sha256).hexdigest()}
            return client.post('/_internal/delivery-probes/'+cid,content=payload,headers=headers).status_code
        monkeypatch.setattr(webhooks,'post',receiver)
        asyncio.run(webhooks.deliver(config,probe['delivery_id']))
        advance(monkeypatch,301)
        asyncio.run(watchdog.consume(config,{'member_id':probe['member_id']}))
        if expected_retries: asyncio.run(webhooks.deliver(config,probe['delivery_id']))
        assert work(conn)[0]['retries']==expected_retries
        assert work(conn)[0]['status']=='resolved'
        detail=client.get('/v1/owner/delivery/probes/'+cid,headers=owner).json()
        assert detail['request_count']==expected_retries+1
        assert detail['watchdog']['pending']==0
        assert client.delete('/v1/owner/delivery/probes/'+cid,headers=owner).status_code==204
        assert not db.member(conn,probe['member_id']) and not db.one(conn,'SELECT * FROM watchdog_probes')
        conn.close()

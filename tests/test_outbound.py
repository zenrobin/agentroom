import asyncio
import json
import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from app import db, outbound, watchdog, webhooks
from tests.test_watchdog import advance, report


def register(client,member,device=None):
    return client.post('/v1/connector/outbound',headers=member['headers'],json={'device_id':device or str(uuid4())})


def hints(conn,member,sid):
    from app import auth
    actor={**db.member(conn,member['member_id']),'can_negotiate':True}
    return outbound.pending(conn,actor,sid)


def received(client,member,sid,hint):
    return client.post('/v1/connector/outbound/'+sid+'/received',headers=member['headers'],
                       json={k:hint[k] for k in ['delivery_id','event_id','cycle']})


@pytest.fixture
def pilot(client,enroll,send,conn,monkeypatch):
    monkeypatch.setattr(watchdog,'kick',AsyncMock())
    monkeypatch.setattr(webhooks,'post',AsyncMock(return_value=200))
    a,b=enroll(),enroll()
    sid=register(client,b).json()['subscription_id']
    item=send(a,to=[b['member_id']]).json()
    return a,b,sid,item


def test_registration_is_identity_bound_idempotent_and_does_not_replace_webhooks(client,enroll,conn):
    member=enroll(); device=str(uuid4())
    first=register(client,member,device)
    assert first.status_code==200
    assert register(client,member,device).json()==first.json()
    assert register(client,member).status_code==409
    assert 'secret' not in first.json()
    sid=first.json()['subscription_id']
    assert client.post('/v1/webhooks/'+sid+'/pause',headers=member['headers']).status_code==200
    from app import auth
    from app.errors import APIError
    actor=auth.verify_access(conn,client.app.state.settings,member['access_token'])
    with pytest.raises(APIError,match='outbound_connection_exists'):
        outbound.register(conn,actor,device)
    assert client.post('/v1/webhooks/'+sid+'/rotate',headers=member['headers']).status_code==409
    assert client.post('/v1/webhooks/'+sid+'/delete',headers=member['headers']).status_code==200
    # Explicit deletion permits a new device; registration never silently revives it.
    from app import auth
    actor=auth.verify_access(conn,client.app.state.settings,member['access_token'])
    assert outbound.register(conn,actor,str(uuid4()))['subscription_id']!=sid


def test_hints_are_private_and_transport_receipt_is_not_handling(pilot,client,conn,enroll,send,settings):
    a,b,sid,item=pilot
    other=enroll()
    private=send(a,to=[other['member_id']],body={'text':'never in a wake'}).json()
    all_events=hints(conn,b,sid)
    assert private['seq'] not in [e['seq'] for e in all_events]
    events=[e for e in all_events if e['seq']==item['seq']]
    assert len(events)==1
    assert not {'body','text','thread_id'} & events[0].keys()
    row=db.one(conn,'SELECT * FROM webhook_deliveries WHERE subscription_id=?',(sid,))
    assert row['status']=='pending'
    asyncio.run(webhooks.deliver(settings,row['delivery_id']))
    webhooks.post.assert_not_called()
    assert received(client,other,sid,events[0]).status_code==404
    assert received(client,b,sid,{**events[0],'event_id':str(uuid4())}).status_code==409
    assert received(client,b,sid,events[0]).status_code==200
    completed=db.one(conn,'SELECT completed_at FROM webhook_deliveries WHERE subscription_id=?',(sid,))['completed_at']
    assert received(client,b,sid,events[0]).status_code==200
    assert db.one(conn,'SELECT completed_at FROM webhook_deliveries WHERE subscription_id=?',(sid,))['completed_at']==completed
    assert not db.one(conn,'SELECT 1 FROM processing_receipts')
    assert db.cursor(conn,b['member_id'])<item['seq']
    assert item['seq'] not in [e['seq'] for e in hints(conn,b,sid)]


def test_recovery_uses_fresh_events_and_stops_after_exact_handling(pilot,client,conn,monkeypatch):
    _,b,sid,item=pilot
    initial=hints(conn,b,sid)[0]
    assert received(client,b,sid,initial).status_code==200
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    retry=hints(conn,b,sid)[0]
    assert retry['event_id']!=initial['event_id'] and retry['cycle']==1
    assert received(client,b,sid,initial).status_code==409
    assert received(client,b,sid,retry).status_code==200
    report(conn,b,item)
    advance(monkeypatch,1900);watchdog.process(conn,b['member_id'])
    assert hints(conn,b,sid)==[]
    assert db.one(conn,'SELECT status,retries FROM connector_work')=={'status':'resolved','retries':1}


def test_unconnected_client_never_creates_ai_retry_storm(pilot,conn,monkeypatch):
    _,b,sid,_=pilot
    for _ in range(4):
        advance(monkeypatch,400);watchdog.process(conn,b['member_id'])
    assert db.one(conn,'SELECT retries FROM connector_work')['retries']==0
    assert len(hints(conn,b,sid))==1
    assert not db.one(conn,'SELECT 1 FROM connector_retry_wakes')


def test_busy_recovery_and_handled_original_are_suppressed(pilot,client,conn,monkeypatch):
    _,b,sid,item=pilot
    assert received(client,b,sid,hints(conn,b,sid)[0]).status_code==200
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    run_id=str(uuid4())
    assert client.post('/v1/connector/activity',headers=b['headers'],json={'run_id':run_id,'active':True}).status_code==200
    assert hints(conn,b,sid)==[]
    report(conn,b,item)
    assert hints(conn,b,sid)==[]


def test_saved_hint_rechecked_after_handling_and_busy_lease(pilot,client,conn,monkeypatch):
    _,b,sid,item=pilot
    event=hints(conn,b,sid)[0]
    assert received(client,b,sid,event).status_code==200
    advance(monkeypatch,301);watchdog.process(conn,b['member_id'])
    retry=hints(conn,b,sid)[0]
    run_id=str(uuid4())
    assert client.post('/v1/connector/activity',headers=b['headers'],json={'run_id':run_id,'active':True}).status_code==200
    assert received(client,b,sid,retry).status_code==425
    report(conn,b,item)
    assert received(client,b,sid,retry).status_code==409


def test_listener_capacity_survives_grant_deletion(client,enroll,conn,settings):
    from dataclasses import replace
    from app.errors import APIError
    a,b=enroll(),enroll()
    one,two=register(client,a).json()['subscription_id'],register(client,b).json()['subscription_id']
    limited=replace(settings,max_members=1)
    _,lease=outbound.claim(conn,limited,a['access_token'],one)
    assert db.one(conn,'SELECT last_contact_at FROM outbound_connectors WHERE subscription_id=?',(one,))['last_contact_at'] is None
    with pytest.raises(APIError,match='connector_capacity'):
        outbound.claim(conn,limited,a['access_token'],one)
    assert client.post('/v1/webhooks/'+one+'/delete',headers=a['headers']).status_code==200
    with pytest.raises(APIError,match='connector_capacity'):
        outbound.claim(conn,limited,b['access_token'],two)
    conn.execute('DELETE FROM outbound_streams WHERE stream_id=?',(lease,))
    assert outbound.claim(conn,limited,b['access_token'],two)[1]


def test_rollback_removes_active_and_deleted_transport_only(pilot,client,conn):
    from app.outbound_rollback import prepare
    _,b,sid,item=pilot
    assert prepare(conn)['outbound_grants']==1
    assert db.one(conn,'SELECT 1 FROM webhook_subscriptions WHERE subscription_id=?',(sid,))
    assert client.post('/v1/webhooks/'+sid+'/delete',headers=b['headers']).status_code==200
    assert prepare(conn,apply=True)['outbound_grants']==1
    assert not db.one(conn,'SELECT 1 FROM webhook_subscriptions WHERE subscription_id=?',(sid,))
    assert db.member(conn,b['member_id']) and db.one(conn,'SELECT 1 FROM messages WHERE seq=?',(item['seq'],))
    assert prepare(conn,apply=True)['outbound_grants']==0
    assert register(client,b).status_code==503


def test_readiness_requires_actual_receipt_runtime_and_recent_connection(pilot,client,conn):
    _,b,sid,item=pilot
    conn.execute("INSERT INTO operations_leases VALUES('watchdog_sweep_observed',?)",(db.stamp(),))
    endpoint='/v1/connector/server-recovery'
    assert client.post(endpoint,headers=b['headers']).status_code==409
    assert received(client,b,sid,hints(conn,b,sid)[0]).status_code==200
    assert client.post('/v1/inbox/ack',headers=b['headers'],json={'seq':item['seq']}).status_code==200
    assert client.post('/v1/connector/receipts',headers=b['headers'],json={'receipts':[{'seq':item['seq'],'message_id':item['id']}]}).status_code==200
    assert client.post(endpoint,headers=b['headers']).status_code==409
    assert client.post('/v1/connector/ready',headers=b['headers'],json={'provider':'muse','recovery_interval_s':900,'recovery_verified_at':db.stamp()}).status_code==200
    conn.execute('UPDATE outbound_connectors SET last_contact_at=?',(db.stamp(),))
    assert client.post(endpoint,headers=b['headers']).json()['recovery']['can_disable_scheduled_recovery']
    status=client.get('/v1/connector/status',headers=b['headers']).json()
    assert status['provider']=='muse' and status['state']=='processing_reported'
    conn.execute('UPDATE outbound_connectors SET last_contact_at=?',(db.stamp(db.utcnow()-timedelta(seconds=100)),))
    assert not client.get('/v1/connector/status',headers=b['headers']).json()['recovery']['can_disable_scheduled_recovery']


def test_sandbox_outbound_is_scoped_to_guest_and_survives_restart(settings,monkeypatch):
    from dataclasses import replace
    from fastapi.testclient import TestClient
    from app.main import create_app
    from app import sandbox
    from tests.test_sandbox import admit
    from tests.conftest import message
    monkeypatch.setattr(watchdog,'kick',AsyncMock())
    config=replace(settings,profile='sandbox',webhook_url=None,webhook_secret=None)
    with TestClient(create_app(config)) as client:
        a,b=admit(client),admit(client)
        sid=register(client,a).json()['subscription_id']
        assert client.post('/v1/messages',headers=a['headers'],json=message(to=[sandbox.PEER])).status_code==201
        conn=db.connect(config.db_path)
        events=hints(conn,a,sid)
        assert events and received(client,b,sid,events[0]).status_code==404
        sandbox.initialize(conn,config)
        conn.execute('UPDATE members SET created_at=? WHERE member_id=?',(db.stamp(db.utcnow()-timedelta(days=2)),a['member_id']))
        sandbox.cleanup(conn)
        assert not db.member(conn,a['member_id'])
        conn.close()


def test_live_stream_reconnect_and_revocation(live,owner,monkeypatch):
    from tests.conftest import live_enroll
    monkeypatch.setattr(outbound,'HEARTBEAT',0.05)
    member=live_enroll(live,owner,'outbound-stream')
    sid=register(live,member).json()['subscription_id']
    assert live.post('/v1/owner/members/'+member['member_id']+'/test',headers=owner,json={}).status_code==201
    for _ in range(2):
        with live.stream('GET','/v1/connector/outbound/'+sid+'/events',headers=member['headers']) as response:
            assert response.status_code==200
            lines=response.iter_lines()
            event=next(json.loads(line[6:]) for line in lines if line.startswith('data: '))
            assert event['member_id']==member['member_id']
    assert live.post('/v1/webhooks/'+sid+'/pause',headers=member['headers']).status_code==200
    assert live.get('/v1/connector/outbound/'+sid+'/events',headers=member['headers']).status_code==403


def test_helper_durable_dedupe_coalescing_and_fixed_local_command(tmp_path,monkeypatch):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'clients'))
    import room_connector as helper
    folder=tmp_path/'private';folder.mkdir(mode=0o700)
    store=helper.WakeStore(folder/'wake.sqlite3','https://room.test','m_local')
    events=[{'event_id':str(uuid4()),'delivery_id':str(uuid4()),'cycle':0,'member_id':'m_local','seq':i+1,'type':'message.available'} for i in range(3)]
    command=['/bin/true','fixed argument']
    launch=Mock(return_value=Mock(returncode=0));monkeypatch.setattr(helper.subprocess,'run',launch)
    for event in events: store.save_wake(event)
    store.close()  # Simulate crash after durable persistence.
    store=helper.WakeStore(folder/'wake.sqlite3','https://room.test','m_local')
    monkeypatch.setattr(helper,'call',Mock(return_value={'status':'wake_saved'}))
    helper.flush(None,'subscription',store,command)
    assert launch.call_count==1
    assert launch.call_args.args==(command,) and launch.call_args.kwargs['shell'] is False
    for event in events: store.save_wake(event)
    helper.flush(None,'subscription',store,command)
    assert launch.call_count==1
    assert len(list(store.conn.execute('SELECT * FROM outbound_wakes WHERE attempted_at IS NOT NULL')))==3
    with pytest.raises(helper.ClientError): store.save_wake({**events[0],'seq':500})
    store.save_wake({**events[0],'event_id':str(uuid4())})
    monkeypatch.setattr(helper,'call',Mock(side_effect=helper.ClientError('409: wake_changed',4)))
    helper.flush(None,'subscription',store,command)
    assert launch.call_count==1
    store.close()


def test_helper_only_launches_receipted_snapshot_beyond_batch_limit(tmp_path,monkeypatch):
    from clients import room_connector as helper
    folder=tmp_path/'private';folder.mkdir(mode=0o700)
    store=helper.WakeStore(folder/'wake.sqlite3','https://room.test','m_local')
    events=[{'event_id':str(uuid4()),'delivery_id':str(uuid4()),'cycle':0,'member_id':'m_local','seq':i+1,'type':'message.available'} for i in range(101)]
    for event in events: store.save_wake(event)
    acknowledged=[]
    def receipt(client,method,path,body):
        if body['event_id']==events[0]['event_id']:
            raise helper.ClientError('409: wake_changed',4)
        acknowledged.append(body['event_id'])
    monkeypatch.setattr(helper,'call',receipt)
    launch=Mock(return_value=Mock(returncode=1));monkeypatch.setattr(helper.subprocess,'run',launch)
    helper.flush(None,'sid',store,['/bin/false'])
    assert len(acknowledged)==99 and launch.call_count==1
    last=store.conn.execute('SELECT attempted_at FROM outbound_wakes WHERE event_id=?',(events[-1]['event_id'],)).fetchone()
    assert last['attempted_at'] is None
    helper.flush(None,'sid',store,['/bin/false'])
    assert len(acknowledged)==100 and launch.call_count==2
    helper.flush(None,'sid',store,['/bin/false'])
    assert launch.call_count==2, 'Hook failure is retried by bounded fresh server wakes, never a local AI loop'
    store.close()

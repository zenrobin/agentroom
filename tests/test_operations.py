import asyncio
import hashlib
import hmac
import json
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app import db, operations, webhooks
from app.main import create_app
from tests.conftest import message


def test_operations_migration_keeps_core_compatible_and_upgrades_draft_extension(settings):
    db.initialize(settings.db_path)
    conn=db.connect(settings.db_path)
    conn.execute('DROP TABLE webhook_recoveries')
    conn.execute('UPDATE operations_version SET version=1')
    conn.close();db.initialize(settings.db_path)
    conn=db.connect(settings.db_path)
    assert db.ready(conn)
    assert conn.execute('SELECT version FROM schema_version').fetchone()[0]==4
    assert conn.execute('SELECT version FROM console_delivery_version').fetchone()[0]==1
    assert db.operations_version(conn)==2
    conn.close()


def test_connection_check_status_is_cursor_based_not_agent_processing(client,owner,enroll):
    a=enroll()
    result=client.post('/v1/owner/members/'+a['member_id']+'/test',headers=owner).json()
    def status():return client.get('/v1/owner/overview',headers=owner).json()['members'][0]['connection_check']['status']
    assert status()=='waiting_for_acknowledgement'
    client.get('/v1/inbox',headers=a['headers']);assert status()=='waiting_for_acknowledgement'
    assert client.post('/v1/inbox/ack',headers=a['headers'],json={'seq':result['seq']}).status_code==200
    assert status()=='acknowledged'


def test_delivery_health_reports_overdue_without_payload_or_secret(client,owner,enroll,conn,monkeypatch):
    monkeypatch.setattr(webhooks,'kick',AsyncMock())
    a=enroll()
    sub=client.post('/v1/webhooks',headers=a['headers'],json={'url':'https://receiver.example/hook'}).json()
    client.post('/v1/webhooks/'+sub['subscription_id']+'/test',headers=a['headers'],json={})
    conn.execute('UPDATE webhook_deliveries SET next_at=?,created_at=?',(db.stamp(db.utcnow()-timedelta(minutes=3)),db.stamp(db.utcnow()-timedelta(minutes=4))))
    result=client.get('/v1/owner/delivery/health',headers=owner)
    assert result.status_code==200 and result.json()['overdue_pending']==1
    assert result.json()['oldest_pending_age_s']>=240
    assert sub['secret'] not in result.text and 'payload' not in result.text
    assert client.get('/v1/owner/delivery/health',headers=a['headers']).status_code==401


def test_authenticated_activity_recovery_is_shared_throttled_and_outside_room_lock(settings,client,enroll,conn,monkeypatch):
    monkeypatch.setattr(webhooks,'kick',AsyncMock())
    a=enroll();sub=webhooks.create(conn,settings,a['member_id'],'https://receiver.example/hook',['chat'])
    row=db.one(conn,'SELECT * FROM webhook_subscriptions WHERE subscription_id=?',(sub['subscription_id'],))
    did=webhooks.enqueue(conn,row,{'type':'webhook.test','member_id':a['member_id']})
    conn.execute('UPDATE webhook_deliveries SET next_at=? WHERE delivery_id=?',(db.stamp(db.utcnow()-timedelta(minutes=3)),did))
    kick=AsyncMock();monkeypatch.setattr(webhooks,'kick',kick)
    remote=replace(settings,serverless=True)
    asyncio.run(operations.recover(remote));asyncio.run(operations.recover(remote))
    kick.assert_awaited_once_with(remote,force=True)


def test_private_room_has_no_internal_public_delivery_probe(client,owner):
    assert client.post('/v1/owner/delivery/probes',headers=owner,json={}).status_code==404
    assert client.post('/_internal/delivery-probes/unknown',json={}).status_code==404


def test_sandbox_probe_uses_signed_self_receiver_and_deduplicates(settings,monkeypatch):
    config=replace(settings,profile='sandbox',webhook_url=None,webhook_secret=None)
    owner={'Authorization':'Bearer '+config.owner_key}
    sent=[]
    async def publish(*args,**kwargs):pass
    monkeypatch.setattr(webhooks,'dispatch',AsyncMock(side_effect=publish))
    config=replace(config,serverless=True)
    with TestClient(create_app(config)) as client:
        assert client.post('/v1/owner/delivery/probes',json={}).status_code==401
        started=client.post('/v1/owner/delivery/probes',headers=owner,json={'fail_first':True})
        assert started.status_code==202,started.text
        cid=started.json()['check_id'];conn=db.connect(config.db_path)
        probe=db.one(conn,'SELECT * FROM delivery_probes WHERE check_id=?',(cid,))
        sub=db.one(conn,'SELECT * FROM webhook_subscriptions WHERE subscription_id=?',(probe['subscription_id'],))
        row=db.one(conn,'SELECT * FROM webhook_deliveries WHERE delivery_id=?',(probe['delivery_id'],))
        body=row['payload'].encode();timestamp=str(int(db.utcnow().timestamp()))
        secret=webhooks.cipher(config).decrypt(sub['secret'].encode())
        headers={'Content-Type':'application/json','X-AR-Timestamp':timestamp,'X-AR-Event-Id':probe['delivery_id'],
                 'X-AR-Signature':'sha256='+hmac.new(secret,timestamp.encode()+b'.'+body,hashlib.sha256).hexdigest()}
        path='/_internal/delivery-probes/'+cid
        assert client.post(path,content=body,headers={'Content-Type':'application/json'}).status_code==401
        assert client.post(path,content=body,headers=headers).status_code==503
        assert client.post(path,content=body,headers=headers).status_code==204
        assert client.post(path,content=body,headers=headers).status_code==204
        result=client.get('/v1/owner/delivery/probes/'+cid,headers=owner)
        assert result.json()['receipt_count']==1 and result.json()['request_count']==3
        assert secret.decode() not in result.text and 'secret' not in result.text
        assert client.post('/v1/webhooks',headers=owner,json={'url':'https://example.com'}).status_code==403
        assert client.post('/v1/owner/members/'+probe['member_id']+'/reconnect',headers=owner,json={}).status_code==409
        # Cleanup tolerates a legacy operator-created grant from an older build.
        db.create_reconnect(conn,'legacy-ticket',probe['member_id'],'test-hash')
        # Operator fixture identities do not prevent later sandbox cold starts.
        with TestClient(create_app(config)) as second:assert second.get('/readyz').status_code==200
        conn.execute('UPDATE delivery_probes SET created_at=?',(db.stamp(db.utcnow()-timedelta(days=2)),))
        with db.transaction(conn):operations.cleanup(conn)
        assert db.member(conn,probe['member_id']) is None
        conn.close()

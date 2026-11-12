import asyncio
import json
import socket
from dataclasses import replace
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from app import db, webhooks
from app.errors import APIError
from clients.room_client import ClientError, verify_webhook
from tests.conftest import message


@pytest.fixture(autouse=True)
def manual_delivery(monkeypatch):
    # Tests control the worker boundary explicitly; never contact a real endpoint.
    original = webhooks.kick
    monkeypatch.setattr(webhooks, 'kick', AsyncMock())
    return original


def subscribe(client, member):
    result = client.post('/v1/webhooks', headers=member['headers'], json={'url': 'https://receiver.example/hooks'})
    assert result.status_code == 201, result.text
    return result.json()


def test_outbox_visibility_idempotency_and_atomicity(client, enroll, conn):
    a, b, c = enroll(), enroll(), enroll()
    sub = subscribe(client, b)
    stored = db.one(conn, 'SELECT * FROM webhook_subscriptions')
    assert sub['secret'] not in str(stored)
    assert 'secret' not in client.get('/v1/webhooks', headers=b['headers']).text
    assert client.get('/v1/webhooks', headers=a['headers']).json() == []
    assert client.post('/v1/webhooks/' + sub['subscription_id'] + '/test', headers=a['headers'], json={}).status_code == 404
    private = message(to=[c['member_id']])
    assert client.post('/v1/messages', headers=a['headers'], json=private).status_code == 201
    assert db.rows(conn, 'SELECT * FROM webhook_deliveries') == []
    visible = message(body={'text': 'PRIVATE BODY MUST NOT BE IN OUTBOX'}, to=[b['member_id']])
    sent = client.post('/v1/messages', headers=a['headers'], json=visible)
    assert sent.status_code == 201
    assert client.post('/v1/messages', headers=a['headers'], json=visible).status_code == 200
    rows = db.rows(conn, 'SELECT * FROM webhook_deliveries')
    assert len(rows) == 1 and rows[0]['message_seq'] == sent.json()['seq']
    assert visible['body']['text'] not in str(rows)
    with pytest.raises(RuntimeError):
        with db.transaction(conn):
            db.insert_message(conn, {**message(to=[b['member_id']]), 'from': a['member_id'], 'received_at': db.stamp()})
            raise RuntimeError('roll back the entire message and event')
    assert len(db.rows(conn, 'SELECT * FROM webhook_deliveries')) == 1


def test_worker_retries_with_stable_event_and_duplicate_callbacks_are_safe(client, enroll, conn, settings, monkeypatch):
    a, b = enroll(), enroll()
    subscribe(client, b)
    assert client.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']])).status_code == 201
    row = db.one(conn, 'SELECT * FROM webhook_deliveries')
    post = AsyncMock(side_effect=[503, 204])
    monkeypatch.setattr(webhooks, 'post', post)
    pending = asyncio.run(webhooks.deliver(settings, row['delivery_id']))
    assert pending['status'] == 'pending' and pending['attempts'] == 1 and pending['last_status'] == 503
    assert asyncio.run(webhooks.deliver(settings, row['delivery_id']))['attempts'] == 1
    conn.execute('UPDATE webhook_deliveries SET next_at=?', (db.stamp(db.utcnow() - timedelta(seconds=1)),))
    assert asyncio.run(webhooks.deliver(settings, row['delivery_id'])) is None
    assert asyncio.run(webhooks.deliver(settings, row['delivery_id'])) is None
    final = db.one(conn, 'SELECT * FROM webhook_deliveries')
    assert final['status'] == 'delivered' and final['attempts'] == 2
    assert post.await_count == 2
    assert post.await_args_list[0].args[2] == post.await_args_list[1].args[2]
    assert len(db.rows(conn, 'SELECT * FROM webhook_attempts')) == 2


def test_revocation_and_permission_changes_cancel_queued_private_events(client, enroll, conn, settings, monkeypatch, owner):
    a, b = enroll(), enroll()
    subscribe(client, b)
    assert client.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']])).status_code == 201
    did = db.one(conn, 'SELECT * FROM webhook_deliveries')['delivery_id']
    post = AsyncMock()
    monkeypatch.setattr(webhooks, 'post', post)
    assert client.post('/v1/owner/members/' + b['member_id'] + '/revoke', headers=owner).status_code == 200
    asyncio.run(webhooks.deliver(settings, did))
    assert db.one(conn, 'SELECT * FROM webhook_deliveries')['status'] == 'cancelled'
    post.assert_not_awaited()


def test_failing_endpoint_stops_and_can_be_replayed_without_changing_event_id(client, enroll, conn, settings, monkeypatch):
    b = enroll()
    sub = subscribe(client, b)
    response = client.post('/v1/webhooks/' + sub['subscription_id'] + '/test', headers=b['headers'], json={})
    did = response.json()['delivery_id']
    monkeypatch.setattr(webhooks, 'post', AsyncMock(return_value=500))
    for _ in range(webhooks.MAX_ATTEMPTS):
        conn.execute('UPDATE webhook_deliveries SET next_at=?', (db.stamp(db.utcnow() - timedelta(seconds=1)),))
        asyncio.run(webhooks.deliver(settings, did))
    assert db.one(conn, 'SELECT * FROM webhook_deliveries')['status'] == 'failed'
    replay = client.post('/v1/webhooks/' + sub['subscription_id'] + '/replay/' + did, headers=b['headers'], json={})
    assert replay.status_code == 202, replay.text
    row = db.one(conn, 'SELECT * FROM webhook_deliveries')
    assert row['cycle'] == 1 and row['attempts'] == 0 and json.loads(row['payload'])['event_id'] == did


@pytest.mark.parametrize('url', ['http://receiver.example', 'https://127.0.0.1/', 'https://169.254.169.254/',
    'https://[::1]/', 'https://[::ffff:127.0.0.1]/', 'https://localhost/', 'https://host.internal/',
    'https://user:password@receiver.example/', 'https://receiver.example:8443/', 'https://receiver.example/#token'])
def test_endpoint_rejects_unsafe_destinations(url):
    with pytest.raises(APIError):
        webhooks.endpoint(url)


def test_dns_rebinding_is_blocked_and_valid_address_is_pinned(monkeypatch):
    def resolve(*args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('93.184.216.34', 443)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.1', 443))]
    monkeypatch.setattr(socket, 'getaddrinfo', resolve)
    with pytest.raises(APIError):
        asyncio.run(webhooks.destination('https://receiver.example/hooks'))
    monkeypatch.setattr(socket, 'getaddrinfo', lambda *args, **kwargs: resolve()[0:1])
    pinned, host = asyncio.run(webhooks.destination('https://receiver.example/hooks'))
    assert str(pinned) == 'https://93.184.216.34/hooks' and host == 'receiver.example'


def test_signed_requests_pin_tls_name_do_not_read_response_and_rotate_gracefully(client, enroll, conn, settings, monkeypatch):
    b = enroll()
    sub = subscribe(client, b)
    rotated = client.post('/v1/webhooks/' + sub['subscription_id'] + '/rotate', headers=b['headers'], json={})
    assert rotated.status_code == 200
    current = db.one(conn, 'SELECT * FROM webhook_subscriptions')
    seen = []
    class Unreadable(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError('A webhook response body must never be read')
            yield b''
    def receiver(request):
        seen.append(request)
        return httpx.Response(204, stream=Unreadable())
    original = httpx.AsyncClient
    monkeypatch.setattr(webhooks.httpx, 'AsyncClient', lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(receiver)))
    monkeypatch.setattr(webhooks, 'destination', AsyncMock(return_value=(httpx.URL('https://93.184.216.34/hooks'), 'receiver.example')))
    payload = json.dumps({'event_id': 'event-123', 'type': 'webhook.test'})
    assert asyncio.run(webhooks.post(settings, current, payload)) == 204
    request = seen[0]
    assert request.url.host == '93.184.216.34' and request.headers['host'] == 'receiver.example'
    assert request.extensions['sni_hostname'] == 'receiver.example'
    assert verify_webhook(rotated.json()['secret'], request.headers, request.content)['event_id'] == 'event-123'
    assert verify_webhook(sub['secret'], request.headers, request.content)['event_id'] == 'event-123'
    with pytest.raises(ClientError):
        verify_webhook(sub['secret'], request.headers, request.content, now=0)
    with pytest.raises(ClientError):
        verify_webhook(sub['secret'], request.headers, b'{"event_id":"tampered"}')


def test_queue_publish_failure_leaves_durable_pending_work(client, enroll, conn, settings, monkeypatch):
    b = enroll()
    sub = subscribe(client, b)
    did = client.post('/v1/webhooks/' + sub['subscription_id'] + '/test', headers=b['headers'], json={}).json()['delivery_id']
    monkeypatch.setattr(webhooks, 'post', AsyncMock(return_value=503))
    monkeypatch.setattr('vercel.queue.QueueClient.send', AsyncMock(side_effect=RuntimeError('queue unavailable')))
    with pytest.raises(RuntimeError):
        asyncio.run(webhooks.consume(replace(settings, serverless=True), did))
    row = db.one(conn, 'SELECT * FROM webhook_deliveries')
    assert row['status'] == 'pending' and row['attempts'] == 1 and row['enqueued_attempt'] == -1


def test_inflight_lease_prevents_concurrent_delivery_and_can_be_recovered(client, enroll, conn, settings, monkeypatch):
    b = enroll()
    sub = subscribe(client, b)
    did = client.post('/v1/webhooks/' + sub['subscription_id'] + '/test', headers=b['headers'], json={}).json()['delivery_id']
    conn.execute('UPDATE webhook_deliveries SET lease_id=?,lease_until=?', ('interrupted-worker', db.stamp(db.utcnow() + timedelta(seconds=45))))
    post = AsyncMock(return_value=204)
    monkeypatch.setattr(webhooks, 'post', post)
    asyncio.run(webhooks.deliver(settings, did))
    post.assert_not_awaited()
    conn.execute('UPDATE webhook_deliveries SET lease_until=?', (db.stamp(db.utcnow() - timedelta(seconds=1)),))
    asyncio.run(webhooks.deliver(settings, did))
    post.assert_awaited_once()
    assert db.one(conn, 'SELECT * FROM webhook_deliveries')['status'] == 'delivered'


def test_queue_continuations_publish_all_recipients_across_multiple_batches(client, enroll, conn, settings, monkeypatch, manual_delivery):
    b = enroll()
    subscribe(client, b)
    sub = db.one(conn, 'SELECT * FROM webhook_subscriptions')
    with db.transaction(conn):
        for _ in range(67):
            webhooks.enqueue(conn, sub, {'type': 'webhook.test', 'member_id': b['member_id'], 'occurred_at': db.stamp()})
    queued = []
    async def publish(topic, payload, **kwargs):
        queued.append(payload)
    monkeypatch.setattr('vercel.queue.QueueClient.send', AsyncMock(side_effect=publish))
    monkeypatch.setattr(webhooks, 'kick', manual_delivery)
    remote = replace(settings, serverless=True)
    async def run():
        await webhooks.kick(remote)
        delivered_to_queue, pumps = [], 0
        while queued:
            item = queued.pop(0)
            if item.get('drain'):
                pumps += 1
                assert pumps <= 3
                await webhooks.pump(remote)
            else:
                delivered_to_queue.append(item['delivery_id'])
        assert len(set(delivered_to_queue)) == 67
        assert pumps == 3
    asyncio.run(run())
    assert all(row['enqueued_attempt'] == 0 for row in db.rows(conn, 'SELECT * FROM webhook_deliveries'))


def test_recovery_republishes_exhausted_jobs_and_drains_every_batch(client,enroll,conn,settings,monkeypatch,manual_delivery):
    b=enroll();subscribe(client,b)
    sub=db.one(conn,'SELECT * FROM webhook_subscriptions')
    with db.transaction(conn):
        for _ in range(67):webhooks.enqueue(conn,sub,{'type':'webhook.test','member_id':b['member_id']})
        conn.execute('UPDATE webhook_deliveries SET enqueued_attempt=attempts,enqueued_at=?',(db.stamp(),))
    # These keys represent accepted callbacks whose retry budget was exhausted.
    known={f"{r['delivery_id']}:0:1:0" for r in db.rows(conn,'SELECT * FROM webhook_deliveries')}
    queue=[]
    async def publish(topic,payload,**kwargs):
        if kwargs['idempotency_key'] not in known:
            known.add(kwargs['idempotency_key']);queue.append(payload)
    monkeypatch.setattr('vercel.queue.QueueClient.send',AsyncMock(side_effect=publish))
    monkeypatch.setattr(webhooks,'kick',manual_delivery)
    remote=replace(settings,serverless=True)
    async def recover():
        for _ in range(2):
            await webhooks.kick(remote,force=True)
            delivered=[];batches=0
            while queue:
                item=queue.pop(0)
                if item.get('drain'):
                    batches+=1;assert batches<=3
                    await webhooks.pump(remote,force=item['force'],recovery_id=item['recovery_id'])
                else:delivered.append(item['delivery_id'])
            assert len(set(delivered))==67 and batches==3
    asyncio.run(recover())
    assert conn.execute('SELECT count(*) FROM webhook_recoveries').fetchone()[0]==134


def test_owner_can_remove_revoked_members_endpoints_but_cannot_resume_them(client, enroll, owner):
    b = enroll()
    sub = subscribe(client, b)
    assert client.post('/v1/owner/members/' + b['member_id'] + '/revoke', headers=owner).status_code == 200
    base = '/v1/owner/webhooks/' + sub['subscription_id']
    assert client.get(base + '/deliveries', headers=owner).status_code == 200
    assert client.post(base + '/resume', headers=owner, json={}).status_code == 403
    assert client.post(base + '/delete', headers=owner, json={}).status_code == 200


def test_revoked_originating_session_and_expired_grants_cannot_deliver(client, enroll, conn, settings, monkeypatch):
    a, b = enroll(), enroll()
    subscribe(client, b)
    assert client.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']])).status_code == 201
    did = db.one(conn, 'SELECT * FROM webhook_deliveries')['delivery_id']
    assert client.post('/v1/token/revoke', headers=b['headers']).status_code == 204
    post = AsyncMock()
    monkeypatch.setattr(webhooks, 'post', post)
    asyncio.run(webhooks.deliver(settings, did))
    post.assert_not_awaited()
    assert db.one(conn, 'SELECT * FROM webhook_deliveries')['status'] == 'cancelled'


def test_early_queue_callback_retries_instead_of_acknowledging_deduplicated_future_work(client, enroll, conn, settings, monkeypatch):
    b = enroll()
    sub = subscribe(client, b)
    did = client.post('/v1/webhooks/' + sub['subscription_id'] + '/test', headers=b['headers'], json={}).json()['delivery_id']
    conn.execute('UPDATE webhook_deliveries SET next_at=?,enqueued_attempt=0', (db.stamp(db.utcnow() + timedelta(seconds=10)),))
    post, dispatch = AsyncMock(), AsyncMock()
    monkeypatch.setattr(webhooks, 'post', post)
    monkeypatch.setattr(webhooks, 'dispatch', dispatch)
    with pytest.raises(RuntimeError, match='retry this queue callback'):
        asyncio.run(webhooks.consume(settings, did))
    post.assert_not_awaited()
    dispatch.assert_not_awaited()
    assert db.one(conn, 'SELECT * FROM webhook_deliveries')['status'] == 'pending'

import asyncio
import hashlib
import hmac
import json
import logging
import secrets
from datetime import timedelta

import httpx
import pytest

from app import db, notify
from app.errors import APIError
from app.signing import canonical_json
from .conftest import join_body, message
from .test_negotiation import propose, action


def test_revocation_during_body_upload(app, client, enroll, owner):
    actor = enroll()
    encoded = canonical_json(message()).encode()
    async def attack():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://testserver') as http:
            async def body():
                yield encoded[:10]
                result = await http.post('/v1/owner/members/' + actor['member_id'] + '/revoke', headers=owner)
                assert result.status_code == 200
                yield encoded[10:]
            return await http.post('/v1/messages', content=body(), headers={**actor['headers'], 'Content-Type': 'application/json'})
    assert asyncio.run(attack()).status_code == 401


def test_removed_party_does_not_prevent_withdraw(client, enroll, send, owner):
    a, b = enroll(), enroll()
    nid = propose(send, a, b)['id']
    client.post('/v1/owner/members/' + b['member_id'] + '/revoke', headers=owner)
    result = action(send, a, b, nid, 'withdraw', {})
    assert result.status_code == 201, result.text


def test_reissue_after_join_metadata_retention(client, enroll, owner, conn):
    actor = enroll()
    conn.execute('UPDATE join_requests SET decided_at=? WHERE member_id=?', (db.stamp(db.utcnow() - timedelta(days=31)), actor['member_id']))
    with db.transaction(conn):
        db.cleanup(conn, 30)
    assert conn.execute('SELECT count(*) FROM join_requests WHERE member_id=?', (actor['member_id'],)).fetchone()[0] == 0
    assert client.post('/v1/owner/members/' + actor['member_id'] + '/reissue', headers=owner).status_code == 410
    pair = client.get('/v1/join/' + actor['join']['request_id'], headers={'X-Poll-Secret': actor['join']['poll_secret']}).json()
    assert pair == {'status': 'approved', 'delivered': True}


def test_logs_and_validation_do_not_leak_secrets(client, enroll, owner, caplog, send):
    caplog.set_level(logging.INFO, logger='agent_room')
    actor = enroll()
    marker = 'PRIVATE-BODY-MARKER'
    assert send(actor, body={'text': marker}).status_code == 201
    response = client.post('/v1/token/refresh', json={'refresh_token': 'INVALID-SECRET'})
    assert response.status_code == 422 and 'INVALID-SECRET' not in response.text
    for secret in [marker, actor['access_token'], actor['refresh_token'], actor['join']['poll_secret'], owner['Authorization'][7:]]:
        assert secret not in caplog.text


def test_webhook_signature_and_retry(settings, monkeypatch):
    captured = []
    real_client = httpx.AsyncClient
    def handler(request):
        captured.append(request)
        timestamp = request.headers['X-AR-Timestamp']
        expected = 'sha256=' + hmac.new(settings.webhook_secret.encode(), timestamp.encode() + b'.' + request.content, hashlib.sha256).hexdigest()
        assert hmac.compare_digest(expected, request.headers['X-AR-Signature'])
        return httpx.Response(500 if len(captured) == 1 else 204)
    monkeypatch.setattr(notify.httpx, 'AsyncClient', lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs))
    async def no_sleep(_):
        pass
    monkeypatch.setattr(notify.asyncio, 'sleep', no_sleep)
    db.initialize(settings.db_path)
    asyncio.run(notify.notify_owner(settings, {'request_id': 'test', 'agent_name': 'agent'}))
    assert len(captured) == 2
    conn = db.connect(settings.db_path)
    try:
        assert conn.execute("SELECT count(*) FROM audit_log WHERE action='notification.failed'").fetchone()[0] == 1
    finally:
        conn.close()


def test_health_ready_metrics_and_content_type(client, owner, enroll):
    assert client.get('/healthz').json() == {'ok': True}
    assert client.get('/readyz').status_code == 200
    assert client.get('/v1/room').json()['commit_public_key']
    assert client.post('/v1/join', content='{}').status_code == 415
    assert client.get('/missing').json()['error']['code'] == 'not_found'
    assert client.post('/v1/join', content='x' * 65537, headers={'Content-Type': 'application/json'}).status_code == 413
    actor = enroll()
    response = client.post('/v1/messages', content='{}', headers=actor['headers'])
    assert response.status_code == 415
    assert client.get('/v1/owner/metrics', headers=owner).text.startswith('ar_http_requests_total')
    assert client.get('/v1/owner/audit', headers=owner).status_code == 200

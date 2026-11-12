from dataclasses import replace
import asyncio
import json
import secrets

import httpx
import pytest

from app import db, notify
from app.errors import APIError
from app.ratelimit import DatabaseRateLimiter
from app.stream import DatabaseHub


def test_database_limits_persist_across_instances(settings, monkeypatch):
    db.initialize(settings.db_path)
    now = [1_000_000.0]
    monkeypatch.setattr(db, 'clock', lambda conn: now[0])
    one, two = DatabaseRateLimiter(settings.db_path), DatabaseRateLimiter(settings.db_path)
    one.hit('burst', 'member', 1, 1)
    with pytest.raises(APIError) as error:
        two.hit('burst', 'member', 1, 1)
    assert error.value.headers['Retry-After'] == '1'
    now[0] += 1
    two.hit('burst', 'member', 1, 1)
    for _ in range(3):
        one.hourly('hour', 'member', 3)
    with pytest.raises(APIError):
        two.hourly('hour', 'member', 3)
    for _ in range(20):
        one.fail('ip')
    with pytest.raises(APIError):
        two.check_ip('ip')
    assert two.counts == {'burst': 1, 'hour': 1}
    now[0] += 901
    two.check_ip('ip')
    now[0] += 3600
    two.hourly('hour', 'member', 3)


def test_database_connection_cap_and_crashed_lease_expiry(settings, monkeypatch):
    db.initialize(settings.db_path)
    now = [1_000_000.0]
    monkeypatch.setattr(db, 'clock', lambda conn: now[0])
    one, two = DatabaseHub(settings.db_path), DatabaseHub(settings.db_path)
    with one.subscribe('member', 'sse') as queue:
        with two.subscribe('member', 'sse'):
            assert one.streams == 2
            with pytest.raises(APIError):
                with one.subscribe('member', 'sse'):
                    pass
        conn = db.connect(settings.db_path)
        try:
            now[0] += 4
            queue.renewed_at -= 4
            assert one.renew(queue, conn)
            now[0] += 16
            queue.renewed_at -= 4
            assert not one.renew(queue, conn)
            with two.subscribe('member', 'sse'):
                assert two.streams == 1
        finally:
            conn.close()
    assert one.streams == 0


def test_resend_uses_idempotency_and_audits_acceptance(settings, monkeypatch):
    db.initialize(settings.db_path)
    config = replace(settings, webhook_url=None, webhook_secret=None,
                     email='owner@example.com', resend_api_key=secrets.token_urlsafe(32))
    real_client = httpx.AsyncClient
    def handler(request):
        assert request.url == 'https://api.resend.com/emails'
        assert request.headers['Idempotency-Key'] == 'agent-room-join-review-v2-test-request'
        payload = json.loads(request.content)
        assert payload['to'] == ['owner@example.com']
        assert '/owner/review#' in payload['text'] and config.owner_key not in payload['text']
        assert 'Review request' in payload['html']
        return httpx.Response(200, json={'id': 'test-email'})
    monkeypatch.setattr(notify.httpx, 'AsyncClient', lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs))
    asyncio.run(notify.notify_owner(config, {'request_id': 'test-request', 'agent_name': 'example'}))
    conn = db.connect(settings.db_path)
    try:
        assert any(row['action'] == 'notification.sent' and row['detail'] == 'resend' for row in db.audit_page(conn, 0, 50))
    finally:
        conn.close()

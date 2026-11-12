import asyncio
import json
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from app import db, providers, webhooks
from app.errors import APIError
from app.main import create_app

ROUTINE = 'https://api2.cursor.sh/automations/webhook/00000000-0000-4000-8000-000000000001'
KEY = 'test-routine-key-only-not-a-real-secret'


def grok(client, member, **overrides):
    response = client.post('/v1/webhooks', headers=member['headers'], json={
        'provider': 'grok', 'url': ROUTINE, 'authorization': KEY, **overrides})
    assert response.status_code == 201, response.text
    return response.json()


def test_grok_key_is_encrypted_never_returned_and_existing_identity_stays_usable(client, enroll, conn, settings, owner, caplog):
    member = enroll('existing-jerry')
    before = db.member(conn, member['member_id'])
    created = grok(client, member, authorization='Authorization: Bearer ' + KEY)
    assert created['provider'] == 'grok' and 'secret' not in created and KEY not in json.dumps(created)
    saved = db.one(conn, 'SELECT * FROM webhook_provider_credentials')
    assert KEY not in saved['credential']
    assert providers.cipher(settings).decrypt(saved['credential'].encode()).decode() == 'Bearer ' + KEY
    for headers, path in [(member['headers'], '/v1/webhooks'), (owner, '/v1/owner/webhooks'), (owner, '/v1/owner/audit/recent')]:
        response = client.get(path, headers=headers)
        assert response.status_code == 200 and KEY not in response.text
    assert client.get('/v1/webhooks', headers=member['headers']).json()[0]['provider'] == 'grok'
    assert db.member(conn, member['member_id']) == before
    assert client.get('/v1/inbox', headers=member['headers']).status_code == 200
    assert KEY not in caplog.text


@pytest.mark.parametrize('value', [KEY, 'Bearer ' + KEY, 'Authorization: Bearer ' + KEY, 'authorization: bearer ' + KEY])
def test_grok_accepts_the_three_copy_formats_without_user_constructing_headers(value):
    assert providers.grok_credentials(ROUTINE, value) == 'Bearer ' + KEY


@pytest.mark.parametrize('url', [
    'https://attacker.example/automations/webhook/00000000-0000-4000-8000-000000000001',
    ROUTINE.replace('api2.cursor.sh', 'api2.cursor.sh.attacker.example'),
    ROUTINE.replace('/automations/', '/other/'), ROUTINE + '?key=private', ROUTINE + '#key',
    ROUTINE.replace('https:', 'http:'), ROUTINE.replace('api2.cursor.sh', '127.0.0.1'),
    ROUTINE.replace('api2.cursor.sh', 'user:password@api2.cursor.sh'),
])
def test_grok_bearer_is_restricted_to_the_real_routine_host_and_path(url):
    with pytest.raises(APIError):
        providers.grok_credentials(url, KEY)


@pytest.mark.parametrize('key', ['', 'short', 'arwh_' + KEY, 'Bearer one two three', KEY + '\r\nX-Evil: injected'])
def test_invalid_provider_credentials_do_not_echo_secrets(client, enroll, key):
    member = enroll()
    response = client.post('/v1/webhooks', headers=member['headers'], json={'provider': 'grok', 'url': ROUTINE, 'authorization': key})
    assert response.status_code == 422
    assert KEY not in response.text


def test_direct_grok_wake_uses_bearer_pinned_tls_and_metadata_only(client, enroll, conn, settings, monkeypatch):
    member = enroll()
    subscription = grok(client, member)
    test = client.post('/v1/webhooks/' + subscription['subscription_id'] + '/test', headers=member['headers'], json={})
    assert test.status_code == 202
    delivery = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=?', (test.json()['delivery_id'],))
    sub = webhooks.valid(conn, delivery)
    seen = []

    class Unreadable(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError('Do not read a provider response body')
            yield b''

    def receiver(request):
        seen.append(request)
        return httpx.Response(200, stream=Unreadable())

    original = httpx.AsyncClient
    monkeypatch.setattr(webhooks.httpx, 'AsyncClient', lambda **kw: original(**kw, transport=httpx.MockTransport(receiver)))
    monkeypatch.setattr(webhooks, 'destination', AsyncMock(return_value=(httpx.URL(ROUTINE.replace('api2.cursor.sh', '93.184.216.34')), 'api2.cursor.sh')))
    assert asyncio.run(webhooks.post(settings, sub, delivery['payload'])) == 200
    request = seen[0]
    assert request.headers['Authorization'] == 'Bearer ' + KEY
    assert request.headers['Host'] == 'api2.cursor.sh' and request.extensions['sni_hostname'] == 'api2.cursor.sh'
    assert 'x-ar-signature' not in request.headers
    payload = json.loads(request.content)
    assert payload['type'] == 'webhook.test' and payload['member_id'] == member['member_id']
    assert 'body' not in payload and KEY not in request.content.decode()
    tampered = {**sub, 'url': 'https://attacker.example/hooks'}
    with pytest.raises(APIError):
        asyncio.run(webhooks.post(settings, tampered, delivery['payload']))
    assert len(seen) == 1


def test_grok_update_is_member_scoped_cancels_old_jobs_and_does_not_rotate_signing_key(client, enroll, conn, settings):
    member, stranger = enroll(), enroll()
    subscription = grok(client, member)
    path = '/v1/webhooks/' + subscription['subscription_id']
    client.post(path + '/test', headers=member['headers'], json={})
    before = db.one(conn, 'SELECT * FROM webhook_subscriptions')
    assert client.post(path + '/rotate', headers=member['headers'], json={}).status_code == 409
    body = {'url': ROUTINE, 'authorization': 'Bearer ' + KEY + '-updated'}
    assert client.post(path + '/configure', headers=stranger['headers'], json=body).status_code == 404
    response = client.post(path + '/configure', headers=member['headers'], json=body)
    assert response.status_code == 200 and KEY not in response.text
    after = db.one(conn, 'SELECT * FROM webhook_subscriptions')
    assert after['generation'] == before['generation'] + 1 and after['secret'] == before['secret']
    assert db.one(conn, 'SELECT status FROM webhook_deliveries')['status'] == 'cancelled'
    encrypted = db.one(conn, 'SELECT credential FROM webhook_provider_credentials')['credential']
    assert providers.cipher(settings).decrypt(encrypted.encode()).decode() == 'Bearer ' + KEY + '-updated'


def test_grok_revoke_prevents_delivery_and_credential_update(client, enroll, owner, conn):
    member = enroll()
    subscription = grok(client, member)
    path = '/v1/owner/webhooks/' + subscription['subscription_id']
    test = client.post(path + '/test', headers=owner, json={}).json()
    client.post('/v1/owner/members/' + member['member_id'] + '/revoke', headers=owner, json={})
    delivery = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=?', (test['delivery_id'],))
    assert webhooks.valid(conn, delivery) is None
    assert client.post(path + '/configure', headers=owner, json={'url': ROUTINE, 'authorization': KEY}).status_code == 403


def test_provider_migration_preserves_room_history_and_remains_idempotent(client, enroll, conn, settings):
    member = enroll()
    before = db.one(conn, 'SELECT count(*) AS n FROM messages')['n']
    conn.execute('DROP TABLE webhook_provider_credentials')
    conn.execute('DROP TABLE provider_setup_version')
    assert not db.provider_setup_ready(conn)
    db.initialize(settings.db_path)
    db.initialize(settings.db_path)
    assert db.ready(conn) and db.member(conn, member['member_id'])
    assert db.one(conn, 'SELECT count(*) AS n FROM messages')['n'] == before
    assert conn.execute('SELECT version FROM schema_version').fetchone()[0] == 4


def test_provider_guides_explain_platform_boundaries_and_keep_sandbox_isolated(client, settings):
    catalog = client.get('/v1/setup/providers').json()
    assert [row['id'] for row in catalog['providers']] == ['grok', 'muse', 'agentroom']
    assert catalog['providers'][0]['automatic_wake'] and not catalog['providers'][1]['automatic_wake']
    assert 'one-shot' in client.get('/setup/grok').text
    assert 'has not been verified' in client.get('/setup/muse').text
    assert client.get('/setup/../../.env.owner').status_code == 404
    with TestClient(create_app(replace(settings, profile='sandbox', room_name='AgentRoom Sandbox', db_path=settings.db_path.parent/'sandbox'/'room.db'))) as sandbox:
        assert not sandbox.get('/v1/setup/providers').json()['outbound_integrations']
        assert 'does not connect external' in sandbox.get('/setup/grok').text
        assert sandbox.post('/v1/owner/webhooks', headers={'Authorization': 'Bearer ' + settings.owner_key}, json={'provider': 'grok', 'url': ROUTINE, 'authorization': KEY}).status_code == 403

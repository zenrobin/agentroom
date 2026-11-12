import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest

from app import db, notify, review
from app.errors import APIError
from .conftest import join_body


def application(client, settings, name='review-agent'):
    body = join_body(name)
    assert client.post('/v1/join', json=body).status_code == 202
    rid = body['request_id']
    return body, '/v1/review/requests/' + rid, {'Authorization': 'Bearer ' + review.issue(settings, rid)}


def test_review_is_scoped_and_gets_never_approve(client, settings, owner, conn):
    body, path, headers = application(client, settings)
    assert client.get(path, headers=headers).json()['status'] == 'pending'
    assert client.get(path + '/approve', headers=headers).status_code == 405
    assert db.join_request(conn, body['request_id'])['status'] == 'pending'
    assert client.get('/v1/owner/requests', headers=headers).status_code == 401
    other = join_body('other-agent')
    assert client.post('/v1/join', json=other).status_code == 202
    assert client.get('/v1/review/requests/' + other['request_id'], headers=headers).status_code == 401
    assert client.get(path, headers=owner).status_code == 401
    assert client.get(path).status_code == 401
    result = client.post(path + '/approve', headers=headers, json={})
    assert result.status_code == 200
    pair = client.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']}).json()
    assert pair['member_id'] == result.json()['member_id']
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + pair['access_token']}).status_code == 200
    assert client.get(path, headers=headers).json()['status'] == 'delivered'
    assert client.post(path + '/deny', headers=headers, json={}).status_code == 409


def test_review_deny_and_tamper(client, settings, conn):
    body, path, headers = application(client, settings)
    token = headers['Authorization'].split()[1]
    claims = jwt.decode(token, options={'verify_signature': False})
    claims['typ'] = 'access'
    invalid = jwt.encode(claims, review.key(settings), algorithm='HS256')
    assert client.post(path + '/approve', headers={'Authorization': 'Bearer ' + invalid}, json={}).status_code == 401
    assert db.join_request(conn, body['request_id'])['status'] == 'pending'
    assert client.post(path + '/deny', headers=headers, json={}).json()['status'] == 'denied'
    assert client.post(path + '/approve', headers=headers, json={}).status_code == 409


@pytest.mark.parametrize('change', ['expired', 'audience', 'signature'])
def test_invalid_review_tokens(settings, change):
    rid = 'request'
    token = review.issue(settings, rid)
    claims = jwt.decode(token, options={'verify_signature': False})
    if change == 'expired':
        claims['iat'] = int((db.utcnow() - timedelta(days=2)).timestamp())
        claims['exp'] = int((db.utcnow() - timedelta(days=1)).timestamp())
    if change == 'audience':
        claims['aud'] = 'other.test'
    token = jwt.encode(claims, b'x' * 32 if change == 'signature' else review.key(settings), algorithm='HS256')
    with pytest.raises(APIError):
        review.verify(settings, rid, token)


def test_review_page_security_and_email_escaping(client, settings):
    result = client.get('/owner/review')
    assert result.status_code == 200
    assert "frame-ancestors 'none'" in result.headers['content-security-policy']
    assert 'sha256-' in result.headers['content-security-policy']
    assert result.headers['referrer-policy'] == 'no-referrer'
    assert 'history.replaceState' in result.text and 'localStorage' not in result.text
    one = notify.email_content(settings, {'request_id': 'one', 'agent_name': 'jerry', 'display_name': '<script>alert(1)</script>'})
    two = notify.email_content(settings, {'request_id': 'two', 'agent_name': 'jerry'})
    assert one['subject'] != two['subject'] and 'one' in one['subject'] and 'jerry' in one['subject']
    assert '<script>' not in one['html'] and '&lt;script&gt;' in one['html']
    assert settings.owner_key not in str(one)
    link = one['text'].split('browser:\n')[1].split('\n')[0]
    parsed = urlsplit(link)
    assert not parsed.query and parsed.path == '/owner/review'
    params = parse_qs(parsed.fragment)
    review.verify(settings, 'one', params['token'][0])


def test_resend_retries_identical_review_payload(settings, monkeypatch):
    db.initialize(settings.db_path)
    config = replace(settings, webhook_url=None, webhook_secret=None, email='owner@example.com', resend_api_key='unused-test-key')
    captured = []
    real = httpx.AsyncClient
    def handler(request):
        captured.append(request.content)
        return httpx.Response(500 if len(captured) == 1 else 200, json={'id': 'message'})
    async def no_sleep(_):
        pass
    monkeypatch.setattr(notify.httpx, 'AsyncClient', lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    monkeypatch.setattr(notify.asyncio, 'sleep', no_sleep)
    asyncio.run(notify.notify_owner(config, {'request_id': 'retry', 'agent_name': 'jerry'}))
    assert len(captured) == 2 and captured[0] == captured[1]


def test_room_code_rejects_before_room_write_lock_and_preserves_pending_retry(client, app, monkeypatch):
    pending = join_body('existing')
    assert client.post('/v1/join', json=pending).status_code == 202
    app.state.settings = replace(app.state.settings, room_code='test-invitation-code')
    assert client.post('/v1/join', json=pending).status_code == 202
    def unexpected_lock(*args, **kwargs):
        raise AssertionError('Bad room code reached the room write lock')
    monkeypatch.setattr(db, 'transaction', unexpected_lock)
    assert client.post('/v1/join', json=join_body('uninvited')).status_code == 403


def test_owner_can_send_new_review_email(client, settings, owner, monkeypatch):
    body, path, headers = application(client, settings)
    calls = []
    async def capture(config, summary):
        calls.append(summary)
    monkeypatch.setattr('app.routes.owner.notify_owner', capture)
    path = '/v1/owner/requests/' + body['request_id'] + '/notify'
    assert client.post(path, headers=owner, json={}).status_code == 202
    assert len(calls) == 1 and calls[0]['agent_name'] == body['agent_name']
    assert 'poll_secret_hash' not in calls[0] and 'poll_secret' not in calls[0]
    assert client.post(path, headers=owner, json={}).status_code == 429

import json
import secrets
from dataclasses import replace
from datetime import timedelta

from fastapi.testclient import TestClient

from app import console, db
from app.main import create_app
from app.signing import digest
from tests.conftest import join_body

ORIGIN = {'Origin': 'http://testserver'}


def device(name='new-agent', member_id=None):
    return {**join_body(name), 'mode': 'reconnect' if member_id else 'new', 'member_id': member_id}


def test_owner_login_requires_explicit_exchange_and_csrf(settings, monkeypatch, caplog):
    sent = []
    async def email(config, token):
        sent.append(token)
    monkeypatch.setattr('app.routes.console.send_owner_login', email)
    settings = replace(settings, email='owner@example.com')
    with TestClient(create_app(settings)) as client:
        assert client.get('/owner').status_code == 200
        assert client.get('/v1/owner/overview').status_code == 401
        assert client.post('/v1/console/login', json={}).status_code == 403
        assert client.post('/v1/console/login', headers=ORIGIN, json={}).status_code == 202
        token = sent[0]
        conn = db.connect(settings.db_path)
        assert db.one(conn, 'SELECT * FROM owner_challenges')['challenge_hash'] == digest(token)
        assert token not in str(db.rows(conn, 'SELECT * FROM owner_challenges'))
        # Loading a page, including a mail scanner GET, never consumes the challenge.
        assert client.get('/owner#login=' + token).status_code == 200
        assert client.get('/v1/console/session').status_code == 401
        session = client.post('/v1/console/session', headers=ORIGIN, json={'token': token})
        assert session.status_code == 200, session.text
        assert 'HttpOnly' in session.headers['set-cookie'] and 'SameSite=strict' in session.headers['set-cookie']
        csrf = session.json()['csrf']
        assert client.get('/v1/owner/overview').status_code == 200
        assert client.get('/v1/console/session').json()['csrf'] == csrf
        assert client.post('/v1/console/session', headers=ORIGIN, json={'token': token}).status_code == 401
        assert client.post('/v1/console/logout', headers=ORIGIN, json={}).status_code == 403
        assert client.post('/v1/console/logout', headers={'Origin': 'https://evil.example', 'X-AR-CSRF': csrf}, json={}).status_code == 403
        cookie = client.cookies.get(console.cookie_name(settings))
        assert client.post('/v1/console/logout', headers={**ORIGIN, 'X-AR-CSRF': csrf}, json={}).status_code == 200
        client.cookies.set(console.cookie_name(settings), cookie)
        assert client.get('/v1/owner/overview').status_code == 401
        conn.close()
        assert token not in caplog.text and cookie not in caplog.text and settings.owner_key not in caplog.text


def test_expired_challenges_and_sessions_fail_closed(client, settings, conn):
    token = secrets.token_urlsafe(32)
    past = db.stamp(db.utcnow() - timedelta(minutes=1))
    conn.execute('INSERT INTO owner_challenges VALUES(?,?,NULL)', (digest(token), past))
    assert client.post('/v1/console/session', headers=ORIGIN, json={'token': token}).status_code == 401
    conn.execute('INSERT INTO owner_sessions VALUES(?,?,?)', (digest(token), past, past))
    client.cookies.set(console.cookie_name(settings), token)
    assert client.get('/v1/owner/overview').status_code == 401


def test_pairing_does_not_expose_pickup_secret_or_tokens_to_owner(client, owner, conn):
    body = device()
    started = client.post('/v1/connect', json=body)
    assert started.status_code == 202, started.text
    code, rid = started.json()['user_code'], body['request_id']
    assert body['poll_secret'] not in started.text
    assert client.get('/v1/connect/' + rid).status_code == 404
    headers = {'X-Connection-Secret': body['poll_secret']}
    assert client.get('/v1/connect/' + rid, headers=headers).json() == {'status': 'pending'}
    overview = client.get('/v1/owner/overview', headers=owner)
    assert body['poll_secret'] not in overview.text and 'poll_hash' not in overview.text
    approved = client.post('/v1/owner/connections/' + code + '/approve', headers=owner, json={})
    assert approved.status_code == 200 and 'access_token' not in approved.text
    first = client.get('/v1/connect/' + rid, headers=headers)
    assert first.status_code == 200 and first.json()['status'] == 'approved'
    assert client.get('/v1/connect/' + rid, headers=headers).json() == first.json()
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + first.json()['access_token']}).status_code == 200
    row = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (rid,))
    assert body['poll_secret'] not in str(row) and first.json()['refresh_token'] not in str(row)


def test_pairing_retry_is_bound_to_original_payload(client):
    body = device()
    first = client.post('/v1/connect', json=body)
    for _ in range(5):
        assert client.post('/v1/connect', json=body).json() == first.json()
    assert client.post('/v1/connect', json={**body, 'poll_secret': secrets.token_urlsafe(32)}).status_code == 409


def test_reconnect_preserves_identity_and_ack_but_invalidates_old_grants(client, enroll, owner, conn, send):
    a, b = enroll(), enroll()
    msg = send(a, to=[b['member_id']]).json()
    assert client.post('/v1/inbox/ack', headers=b['headers'], json={'seq': msg['seq']}).status_code == 200
    body = device('existing', b['member_id'])
    code = client.post('/v1/connect', json=body).json()['user_code']
    assert client.post('/v1/owner/connections/' + code + '/approve', headers=b['headers'], json={}).status_code == 401
    assert client.post('/v1/owner/connections/' + code + '/approve', headers=owner, json={}).status_code == 200
    assert client.get('/v1/me', headers=b['headers']).status_code == 401
    pair = client.get('/v1/connect/' + body['request_id'], headers={'X-Connection-Secret': body['poll_secret']}).json()
    assert pair['member_id'] == b['member_id']
    assert db.cursor(conn, b['member_id']) == msg['seq']
    assert len(db.members(conn)) == 2
    # Refreshing supersedes the pickup cache instead of returning obsolete credentials.
    assert client.post('/v1/token/refresh', json={'refresh_token': pair['refresh_token']}).status_code == 200
    result = client.get('/v1/connect/' + body['request_id'], headers={'X-Connection-Secret': body['poll_secret']})
    assert result.status_code == 409 and result.json()['error']['code'] == 'stale_exchange'


def test_removal_and_new_recovery_ticket_cancel_uncollected_pairings(client, enroll, owner):
    member = enroll()
    body = device('recovery', member['member_id'])
    code = client.post('/v1/connect', json=body).json()['user_code']
    assert client.post('/v1/owner/connections/' + code + '/approve', headers=owner, json={}).status_code == 200
    ticket = client.post('/v1/owner/members/' + member['member_id'] + '/reconnect', headers=owner)
    assert ticket.status_code == 201
    result = client.get('/v1/connect/' + body['request_id'], headers={'X-Connection-Secret': body['poll_secret']})
    assert result.json()['status'] == 'denied'


def test_owner_test_message_is_private_and_does_not_imply_processing(client, enroll, owner):
    a, b = enroll(), enroll()
    result = client.post('/v1/owner/members/' + a['member_id'] + '/test', headers=owner)
    assert result.status_code == 201
    seq = result.json()['seq']
    assert client.get('/v1/messages/' + str(seq), headers=a['headers']).status_code == 200
    assert client.get('/v1/messages/' + str(seq), headers=b['headers']).status_code == 404
    before = client.get('/v1/owner/overview', headers=owner).json()
    member = next(m for m in before['members'] if m['member_id'] == a['member_id'])
    assert member['acked_seq'] < seq and member['last_contact_at']

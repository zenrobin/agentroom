import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient

from app import db
from app.main import create_app
from app.signing import digest
from clients.room_client import Credentials, RoomClient
from .conftest import join_body


def test_refresh_identical_retry_survives_restart(client, enroll, conn, settings):
    actor = enroll()
    body = {'refresh_token': actor['refresh_token'], 'request_id': str(uuid4())}
    first = client.post('/v1/token/refresh', json=body)
    assert first.status_code == 200
    with TestClient(create_app(settings)) as restarted:
        assert restarted.post('/v1/token/refresh', json=body).json() == first.json()
        assert restarted.get('/v1/me', headers={'Authorization': 'Bearer ' + first.json()['access_token']}).status_code == 200
    stored = db.refresh_exchange(conn, digest(actor['refresh_token']))
    assert first.json()['refresh_token'] not in stored['reply']
    assert actor['refresh_token'] not in str(stored)


def test_same_refresh_request_is_atomic_when_concurrent(client, enroll):
    actor = enroll()
    body = {'refresh_token': actor['refresh_token'], 'request_id': str(uuid4())}
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda _: client.post('/v1/token/refresh', json=body), range(2)))
    assert [r.status_code for r in results] == [200, 200]
    assert results[0].json() == results[1].json()


def test_refresh_different_request_replay_still_revokes(client, enroll):
    actor = enroll()
    body = {'refresh_token': actor['refresh_token'], 'request_id': str(uuid4())}
    pair = client.post('/v1/token/refresh', json=body).json()
    body['request_id'] = str(uuid4())
    assert client.post('/v1/token/refresh', json=body).status_code == 401
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + pair['access_token']}).status_code == 401


def test_expired_refresh_retry_revokes_but_superseded_reply_never_overwrites(client, enroll, conn):
    actor = enroll()
    body = {'refresh_token': actor['refresh_token'], 'request_id': str(uuid4())}
    first = client.post('/v1/token/refresh', json=body).json()
    second = client.post('/v1/token/refresh', json={'refresh_token': first['refresh_token'], 'request_id': str(uuid4())}).json()
    assert client.post('/v1/token/refresh', json=body).json()['error']['code'] == 'stale_exchange'
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + second['access_token']}).status_code == 200
    conn.execute('UPDATE refresh_exchanges SET expires_at=?', (db.stamp(db.utcnow() - timedelta(seconds=1)),))
    assert client.post('/v1/token/refresh', json=body).status_code == 401
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + second['access_token']}).status_code == 401


def test_owner_reconnect_preserves_identity_cursor_and_history(client, enroll, owner, send, conn):
    a, b = enroll(), enroll()
    message = send(b, to=[a['member_id']]).json()
    member_before = db.member(conn, a['member_id'])
    cursor_before = db.cursor(conn, a['member_id'])
    path = '/v1/owner/members/' + a['member_id'] + '/reconnect'
    assert client.post(path, headers=b['headers'], json={}).status_code == 401
    ticket = client.post(path, headers=owner, json={}).json()
    assert ticket['secret'] not in str(db.reconnect_ticket(conn, ticket['ticket_id']))
    assert client.get('/v1/me', headers=a['headers']).status_code == 401
    body = {'ticket_id': ticket['ticket_id'], 'secret': ticket['secret']}
    bad = client.post('/v1/token/reconnect', json={**body, 'secret': 'x' * 43})
    assert bad.status_code == 401
    pair = client.post('/v1/token/reconnect', json=body).json()
    assert pair['member_id'] == a['member_id']
    assert client.post('/v1/token/reconnect', json=body).json() == pair
    assert db.member(conn, a['member_id'])['joined_seq'] == member_before['joined_seq']
    assert db.cursor(conn, a['member_id']) == cursor_before
    headers = {'Authorization': 'Bearer ' + pair['access_token']}
    assert message['id'] in str(client.get('/v1/inbox', headers=headers).json())
    assert client.get('/v1/join/' + a['join']['request_id'], headers={'X-Poll-Secret': a['join']['poll_secret']}).json() == {'status': 'approved', 'delivered': True}
    client.post('/v1/token/refresh', json={'refresh_token': pair['refresh_token'], 'request_id': str(uuid4())})
    assert client.post('/v1/token/reconnect', json=body).json()['error']['code'] == 'stale_exchange'
    client.post('/v1/owner/members/' + a['member_id'] + '/revoke', headers=owner)
    assert client.post('/v1/token/reconnect', json=body).status_code == 401


def test_owner_reconnect_invalidates_old_ticket_and_removed_member(client, enroll, owner, app, conn):
    actor = enroll()
    path = '/v1/owner/members/' + actor['member_id'] + '/reconnect'
    old = client.post(path, headers=owner, json={}).json()
    app.state.limiter.buckets.pop(('owner_reconnect', actor['member_id']))
    new = client.post(path, headers=owner, json={}).json()
    assert client.post('/v1/token/reconnect', json={'ticket_id': old['ticket_id'], 'secret': old['secret']}).status_code == 401
    conn.execute('UPDATE reconnect_tickets SET expires_at=? WHERE ticket_id=?',
                 (db.stamp(db.utcnow() - timedelta(seconds=1)), new['ticket_id']))
    assert client.post('/v1/token/reconnect', json={'ticket_id': new['ticket_id'], 'secret': new['secret']}).status_code == 401
    client.post('/v1/owner/members/' + actor['member_id'] + '/revoke', headers=owner)
    app.state.limiter.buckets.pop(('owner_reconnect', actor['member_id']))
    assert client.post(path, headers=owner, json={}).status_code == 409


def test_owner_reconnect_closes_original_unclaimed_pickup(client, owner):
    body = join_body('unclaimed')
    assert client.post('/v1/join', json=body).status_code == 202
    member = client.post('/v1/owner/requests/' + body['request_id'] + '/approve', headers=owner).json()['member_id']
    assert client.post('/v1/owner/members/' + member + '/reconnect', headers=owner, json={}).status_code == 201
    result = client.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']})
    assert result.json() == {'status': 'approved', 'delivered': True}


def test_client_recovers_lost_refresh_response_with_persisted_request(client, enroll, tmp_path):
    actor = enroll()
    creds = Credentials(tmp_path / 'private' / 'credentials.json')
    creds.data = {'url': 'https://room.test', **actor, 'expires_at': 0}
    creds.save()
    requests = []
    def handle(request):
        body = json.loads(request.content)
        assert json.loads(creds.path.read_text())['pending_refresh']['request_id'] == body['request_id']
        requests.append(body)
        result = client.post('/v1/token/refresh', json=body)
        if len(requests) == 1:
            raise httpx.ReadError('simulated lost response')
        return httpx.Response(result.status_code, json=result.json())
    room = RoomClient(creds, transport=httpx.MockTransport(handle), sleep=lambda _: None)
    try:
        room.refresh()
        assert len(requests) == 2 and requests[0] == requests[1]
        saved = json.loads(creds.path.read_text())
        assert 'pending_refresh' not in saved
        assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + saved['access_token']}).status_code == 200
    finally:
        room.close()


def test_library_refreshers_share_file_lock_and_reload_latest(tmp_path):
    path = tmp_path / 'private' / 'credentials.json'
    initial = Credentials(path)
    initial.data = {'url': 'https://room.test', 'member_id': 'member', 'refresh_token': 'old', 'expires_at': 0}
    initial.save()
    calls = []
    def handle(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={'member_id': 'member', 'access_token': 'new-access', 'refresh_token': 'new-refresh', 'access_expires_in': 900})
    clients = []
    for _ in range(2):
        creds = Credentials(path);creds.load()
        clients.append(RoomClient(creds, transport=httpx.MockTransport(handle)))
    barrier = Barrier(2)
    def run(room):
        barrier.wait(timeout=5)
        room.refresh()
    try:
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(run, clients))
        assert len(calls) == 1
        assert all(room.data['refresh_token'] == 'new-refresh' for room in clients)
    finally:
        for room in clients:
            room.close()


def test_client_reconnect_imports_same_identity_and_stops_rejected_refreshes(client, enroll, owner, tmp_path):
    actor = enroll()
    ticket = client.post('/v1/owner/members/' + actor['member_id'] + '/reconnect', headers=owner, json={}).json()
    ticket['url'] = 'https://room.test'
    ticket_path = tmp_path / 'ticket.json'
    ticket_path.write_text(json.dumps(ticket));ticket_path.chmod(0o600)
    creds = Credentials(tmp_path / 'private' / 'credentials.json')
    creds.data = {'url': 'https://room.test', **actor, 'expires_at': 0, 'auth_failed': True}
    creds.save()
    def handle(request):
        result = client.post(request.url.path, json=json.loads(request.content))
        return httpx.Response(result.status_code, json=result.json())
    room = RoomClient(creds, transport=httpx.MockTransport(handle))
    try:
        result = room.reconnect(ticket_path)
        assert result == {'status': 'connected', 'member_id': actor['member_id']}
        assert 'auth_failed' not in json.loads(creds.path.read_text())
        assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + room.data['access_token']}).status_code == 200
    finally:
        room.close()
    calls = []
    def rejected(request):
        calls.append(request)
        return httpx.Response(401, json={'error': {'code': 'unauthenticated'}})
    room = RoomClient(creds, transport=httpx.MockTransport(rejected))
    try:
        from clients.room_client import ClientError
        with pytest.raises(ClientError):
            room.refresh()
        with pytest.raises(ClientError):
            room.refresh()
        assert len(calls) == 1
    finally:
        room.close()

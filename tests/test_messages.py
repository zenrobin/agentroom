from datetime import timedelta
from uuid import uuid4

import pytest

from app import db
from .conftest import message


def test_idempotency_nonce_sender_timestamp(client, enroll):
    actor = enroll()
    data = message()
    result = client.post('/v1/messages', headers=actor['headers'], json=data)
    assert result.status_code == 201, result.text
    assert result.json()['from'] == actor['member_id']
    retry = client.post('/v1/messages', headers=actor['headers'], json=data)
    assert retry.status_code == 200 and retry.json() == result.json()
    assert client.post('/v1/messages', headers=actor['headers'], json={**data, 'body': {'text': 'different'}}).json()['error']['code'] == 'id_conflict'
    assert client.post('/v1/messages', headers=actor['headers'], json={**data, 'id': str(uuid4())}).json()['error']['code'] == 'nonce_reused'
    assert client.post('/v1/messages', headers=actor['headers'], json=message(ts=db.stamp(db.utcnow() - timedelta(seconds=301)))).json()['error']['code'] == 'timestamp_out_of_window'
    assert client.post('/v1/messages', headers=actor['headers'], json=message(**{'from': 'another'})).json()['error']['code'] == 'sender_mismatch'


def test_privacy_all_read_paths_and_join_boundary(client, enroll, send):
    a, b = enroll('a'), enroll('b')
    private = send(a, to=[b['member_id']]).json()
    broadcast = send(a).json()
    c = enroll('c')
    for actor, expected in [(a, 200), (b, 200), (c, 404)]:
        assert client.get('/v1/messages/' + str(private['seq']), headers=actor['headers']).status_code == expected
    items = client.get('/v1/messages', headers=c['headers']).json()['items']
    assert private['id'] not in str(items) and broadcast['id'] not in str(items)
    assert client.get('/v1/messages?thread_id=' + private['thread_id'], headers=c['headers']).json()['items'] == []


def test_inbox_ack_monotonic_and_capped(client, enroll, send):
    a, b = enroll(), enroll()
    sent = send(a, to=[b['member_id']]).json()
    path = '/v1/inbox'
    items = client.get(path, headers=b['headers']).json()['items']
    assert sent['id'] in str(items)
    assert sent['id'] in str(client.get(path, headers=b['headers']).json())
    ack = client.post(path + '/ack', headers=b['headers'], json={'seq': 999999}).json()['acked_seq']
    assert ack == sent['seq']
    assert client.post(path + '/ack', headers=b['headers'], json={'seq': 0}).json()['acked_seq'] == ack
    assert client.get(path, headers=b['headers']).json()['items'] == []
    assert client.get(path + '?wait=121', headers=b['headers']).status_code == 422
    assert all(m['from'] != a['member_id'] for m in client.get(path, headers=a['headers']).json()['items'])


@pytest.mark.parametrize('kind', ['commit', 'expire', 'system'])
def test_reserved_types(client, enroll, kind):
    actor = enroll()
    assert client.post('/v1/messages', headers=actor['headers'], json=message(kind, {})).json()['error']['code'] == 'reserved_type'


def test_size_json_schema_and_recipients(client, enroll):
    actor = enroll()
    assert client.post('/v1/messages', headers=actor['headers'], json=message(body={'text': 'x' * 33000})).status_code == 413
    for change in [{'to': [actor['member_id']]}, {'to': ['missing']}, {'body': {'text': 'hi', 'extra': 1}}, {'ts': '2026-10-27T12:00:00'}]:
        response = client.post('/v1/messages', headers=actor['headers'], json=message(**change))
        assert response.status_code in (400, 422), response.text


def test_late_identical_retry(client, enroll, conn):
    actor = enroll()
    data = message()
    first = client.post('/v1/messages', headers=actor['headers'], json=data)
    from unittest.mock import patch
    with patch('app.routes.members.db.utcnow', return_value=db.utcnow() + timedelta(seconds=301)):
        assert client.post('/v1/messages', headers=actor['headers'], json=data).json() == first.json()

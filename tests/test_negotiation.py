import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4

from app import db
from app.signing import verify
from .conftest import message


def propose(send, a, b, **body):
    response = send(a, 'propose', {'subject': 'Pickup', 'terms': {'day': 'Thursday'}, **body}, to=[b['member_id']])
    assert response.status_code == 201, response.text
    return response.json()


def action(send, actor, other, nid, kind, body):
    return send(actor, kind, {'negotiation_id': nid, **body}, to=[other['member_id']], thread_id=nid)


def test_counter_accept_signed_commit(client, enroll, send):
    a, b, c = enroll(), enroll(), enroll()
    proposal = propose(send, a, b)
    nid = proposal['id']
    detail = client.get('/v1/negotiations/' + nid, headers=b['headers']).json()
    assert detail['accepted_by'] == {'1': [a['member_id']]}
    assert client.get('/v1/commits/' + nid, headers=a['headers']).status_code == 404
    assert action(send, b, a, nid, 'counter', {'base_version': 1, 'terms': {'day': 'Friday'}}).status_code == 201
    detail = client.get('/v1/negotiations/' + nid, headers=a['headers']).json()
    assert detail['accepted_by'] == {'2': [b['member_id']]}
    assert action(send, a, b, nid, 'accept', {'version': 1, 'terms_hash': detail['versions'][0]['terms_hash']}).status_code == 409
    assert action(send, a, b, nid, 'accept', {'version': 2, 'terms_hash': '0' * 64}).status_code == 409
    accepted = action(send, a, b, nid, 'accept', {'version': 2, 'terms_hash': detail['versions'][-1]['terms_hash']})
    assert accepted.status_code == 201, accepted.text
    commit = client.get('/v1/commits/' + nid, headers=a['headers']).json()
    verify(commit['commit_message'], commit['public_key'])
    assert commit['commit_message']['body']['terms'] == {'day': 'Friday'}
    assert len(commit['commit_message']['body']['acceptances']) == 2
    assert client.get('/v1/negotiations/' + nid, headers=c['headers']).status_code == 404
    assert client.get('/v1/commits/' + nid, headers=c['headers']).status_code == 404
    assert action(send, b, a, nid, 'counter', {'base_version': 2, 'terms': {}}).json()['error']['code'] == 'negotiation_closed'


def test_double_accept_and_idempotency(client, enroll, send, conn):
    a, b = enroll(), enroll()
    nid = propose(send, a, b)['id']
    detail = client.get('/v1/negotiations/' + nid, headers=b['headers']).json()
    first = message('accept', {'negotiation_id': nid, 'version': 1, 'terms_hash': detail['versions'][0]['terms_hash']}, to=[a['member_id']], thread_id=nid)
    def accept(data):
        return client.post('/v1/messages', json=data, headers=b['headers'])
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(accept, [first, {**first, 'id': str(uuid4()), 'nonce': 'Z' * 22}]))
    assert sorted(r.status_code for r in results) == [201, 409]
    assert conn.execute('SELECT count(*) FROM commits WHERE negotiation_id=?', (nid,)).fetchone()[0] == 1
    winner = next(r.json() for r in results if r.status_code == 201)
    data = first if winner['id'] == first['id'] else {**first, 'id': winner['id'], 'nonce': 'Z' * 22}
    assert accept(data).status_code == 200


def test_lazy_expiry_is_persisted_after_failed_send(client, enroll, send, conn):
    a, b = enroll(), enroll()
    nid = propose(send, a, b, expires_at=db.stamp(db.utcnow() + timedelta(seconds=2)))['id']
    conn.execute('UPDATE negotiation_versions SET expires_at=? WHERE negotiation_id=?', (db.stamp(db.utcnow() - timedelta(seconds=1)), nid))
    result = action(send, b, a, nid, 'accept', {'version': 1, 'terms_hash': '0' * 64})
    assert result.status_code == 409
    detail = client.get('/v1/negotiations/' + nid, headers=b['headers']).json()
    assert detail['state'] == 'expired'
    emitted = [m for m in client.get('/v1/messages', headers=b['headers']).json()['items'] if m['type'] == 'expire']
    assert len(emitted) == 1
    key = client.get('/v1/room').json()['commit_public_key']
    verify(emitted[0], key)


def test_terminal_decline_withdraw_and_bad_recipients(client, enroll, send):
    a, b, c = enroll(), enroll(), enroll()
    for kind, body in [('decline', {'version': 1}), ('withdraw', {})]:
        nid = propose(send, a, b)['id']
        bad = send(b, 'accept', {'negotiation_id': nid, 'version': 1, 'terms_hash': '0' * 64}, to=[c['member_id']], thread_id=nid)
        assert bad.json()['error']['code'] == 'bad_recipients'
        assert action(send, b, a, nid, kind, body).status_code == 201
        assert client.get('/v1/negotiations/' + nid, headers=a['headers']).json()['state'] == ('declined' if kind == 'decline' else 'withdrawn')


def test_terms_no_floats_and_expiry_bounds(client, enroll, send):
    a, b = enroll(), enroll()
    for terms in [{'amount': 1.5}, {'deep': [{'amount': 0.1}]}]:
        assert send(a, 'propose', {'subject': 'x', 'terms': terms}, to=[b['member_id']]).status_code == 422
    for delta in [timedelta(days=31), timedelta(seconds=-1)]:
        assert send(a, 'propose', {'subject': 'x', 'terms': {}, 'expires_at': db.stamp(db.utcnow() + delta)}, to=[b['member_id']]).status_code == 400


def test_limited_role_enforced_for_send_and_read(client, enroll, send, conn):
    conn.execute("INSERT INTO roles VALUES('limited','room:read room:write negotiate')")
    conn.execute("INSERT INTO role_permissions VALUES('limited','*',1,1)")
    conn.execute("INSERT INTO role_permissions VALUES('limited','propose',0,0)")
    a, b = enroll(role='limited'), enroll()
    assert send(a, 'chat', {'text': 'allowed'}).status_code == 201
    assert send(a, 'propose', {'subject': 'x', 'terms': {}}, to=[b['member_id']]).status_code == 403
    proposal = propose(send, b, a)
    assert client.get('/v1/messages/' + str(proposal['seq']), headers=a['headers']).status_code == 404
    assert proposal['id'] not in str(client.get('/v1/inbox', headers=a['headers']).json())
    assert client.get('/v1/negotiations/' + proposal['id'], headers=a['headers']).status_code == 404


def test_uuid_spelling_normalized_but_raw_hash_idempotent(client, enroll):
    a = enroll()
    data = message()
    data['id'] = data['id'].upper()
    data['thread_id'] = data['id']
    first = client.post('/v1/messages', headers=a['headers'], json=data)
    assert first.status_code == 201
    assert first.json()['id'] == data['id'].lower()
    assert client.post('/v1/messages', headers=a['headers'], json=data).status_code == 200

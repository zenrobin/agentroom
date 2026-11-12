from datetime import timedelta

from app import db
from .conftest import join_body


def test_join_idempotency_delivery_and_reissue(client, owner, conn):
    body = join_body('robin')
    path = '/v1/join/' + body['request_id']
    poll = {'X-Poll-Secret': body['poll_secret']}
    assert client.post('/v1/join', json=body).status_code == 202
    assert client.post('/v1/join', json=body).status_code == 202
    assert client.post('/v1/join', json={**body, 'purpose': 'changed'}).status_code == 409
    assert client.get(path, headers=poll).json() == {'status': 'pending'}
    bad = client.get(path, headers={'X-Poll-Secret': 'wrong'})
    unknown = client.get('/v1/join/unknown', headers=poll)
    assert bad.status_code == unknown.status_code == 404
    assert bad.json()['error']['code'] == unknown.json()['error']['code'] == 'not_found'
    response = client.post('/v1/owner/requests/' + body['request_id'] + '/approve', headers=owner)
    assert response.status_code == 200, response.text
    assert client.post('/v1/owner/requests/' + body['request_id'] + '/approve', headers=owner).json() == response.json()
    pair = client.get(path, headers=poll).json()
    assert pair['refresh_token'].startswith('art_')
    assert client.get(path, headers=poll).json() == {'status': 'approved', 'delivered': True}
    assert client.post('/v1/owner/members/' + pair['member_id'] + '/reissue', headers=owner).status_code == 410
    assert client.get(path, headers=poll).json() == {'status': 'approved', 'delivered': True}
    assert client.post('/v1/owner/members/' + pair['member_id'] + '/revoke', headers=owner).status_code == 200
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + pair['access_token']}).status_code == 401
    assert 'refresh_token' not in client.get(path, headers=poll).json()
    row = db.join_request(conn, body['request_id'])
    assert body['poll_secret'] not in str(row)


def test_deny_expire_and_no_tokens(client, owner, conn):
    body = join_body('denied')
    client.post('/v1/join', json=body)
    path = '/v1/owner/requests/' + body['request_id'] + '/deny'
    assert client.post(path, headers=owner, json={'reason': 'private decision'}).status_code == 200
    assert client.post(path, headers=owner).status_code == 200
    assert client.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']}).json() == {'status': 'denied'}
    body = join_body('expired')
    client.post('/v1/join', json=body)
    conn.execute('UPDATE join_requests SET created_at=? WHERE request_id=?', (db.stamp(db.utcnow() - timedelta(days=8)), body['request_id']))
    assert client.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']}).json() == {'status': 'expired'}


def test_owner_lists_do_not_include_secrets(client, owner, enroll):
    actor = enroll()
    response = client.get('/v1/owner/requests?status=approved', headers=owner)
    assert response.status_code == 200
    assert actor['join']['poll_secret'] not in response.text
    assert 'poll_secret_hash' not in response.text
    assert client.get('/v1/owner/members', headers=owner).status_code == 200


def test_name_taken(client):
    body = join_body('duplicate')
    assert client.post('/v1/join', json=body).status_code == 202
    assert client.post('/v1/join', json=join_body('duplicate')).json()['error']['code'] == 'name_taken'

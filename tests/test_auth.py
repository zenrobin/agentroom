from datetime import timedelta

import jwt
import pytest

from app import db


@pytest.mark.parametrize('path', ['/v1/me', '/v1/members', '/v1/messages', '/v1/inbox', '/v1/stream', '/v1/negotiations'])
def test_missing_auth(client, path):
    response = client.get(path)
    assert response.status_code == 401
    assert response.json()['error']['code'] == 'unauthenticated'


def test_owner_member_separation_and_revocation(client, owner, enroll):
    actor = enroll()
    assert client.get('/v1/me', headers=owner).status_code == 401
    assert client.get('/v1/owner/members', headers=actor['headers']).status_code == 401
    assert client.get('/v1/me', headers=actor['headers']).status_code == 200
    assert client.post('/v1/token/revoke', headers=actor['headers']).status_code == 204
    assert client.get('/v1/me', headers=actor['headers']).status_code == 401


def test_refresh_rotation_and_oldest_reuse(client, enroll):
    actor = enroll()
    first = actor['refresh_token']
    second = client.post('/v1/token/refresh', json={'refresh_token': first}).json()
    third = client.post('/v1/token/refresh', json={'refresh_token': second['refresh_token']}).json()
    assert client.post('/v1/token/refresh', json={'refresh_token': first}).status_code == 401
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + third['access_token']}).status_code == 401
    assert client.post('/v1/token/refresh', json={'refresh_token': third['refresh_token']}).status_code == 401


@pytest.mark.parametrize('mutation', ['aud', 'sub', 'typ', 'kid', 'alg', 'none', 'expired', 'payload', 'signature'])
def test_jwt_tampering(client, enroll, settings, mutation):
    actor = enroll()
    token = actor['access_token']
    claims = jwt.decode(token, options={'verify_signature': False})
    kid, algorithm = 'k1', 'HS256'
    if mutation == 'aud':
        claims['aud'] = 'wrong'
    elif mutation == 'sub':
        claims['sub'] = 'm_impostor'
    elif mutation == 'typ':
        claims['typ'] = 'refresh'
    elif mutation == 'kid':
        kid = 'unknown'
    elif mutation == 'alg':
        algorithm = 'HS512'
    elif mutation == 'none':
        algorithm = 'none'
    elif mutation == 'expired':
        claims['exp'] = int((db.utcnow() - timedelta(seconds=31)).timestamp())
    elif mutation in {'payload', 'signature'}:
        parts = token.split('.')
        index = 1 if mutation == 'payload' else 2
        parts[index] = ('A' if parts[index][0] != 'A' else 'B') + parts[index][1:]
        token = '.'.join(parts)
    if mutation not in {'payload', 'signature'}:
        token = jwt.encode(claims, '' if algorithm == 'none' else settings.jwt_keys['k1'], algorithm=algorithm, headers={'kid': kid})
    assert client.get('/v1/me', headers={'Authorization': 'Bearer ' + token}).status_code == 401


def test_removed_member_refresh_and_reissue(client, enroll, owner):
    actor = enroll()
    base = '/v1/owner/members/' + actor['member_id']
    assert client.post(base + '/revoke', headers=owner).status_code == 200
    assert client.post(base + '/revoke', headers=owner).status_code == 200
    assert client.get('/v1/me', headers=actor['headers']).status_code == 401
    assert client.post('/v1/token/refresh', json={'refresh_token': actor['refresh_token']}).status_code == 401
    assert client.post(base + '/reissue', headers=owner).status_code == 410

import hmac
import base64
import hashlib
import json
import secrets
from datetime import timedelta
from uuid import uuid4

import jwt
from cryptography.fernet import Fernet
from fastapi import Depends, Request

from . import db
from .errors import APIError
from .signing import digest
from . import sandbox

NEGOTIATION_TYPES = {'propose', 'counter', 'accept', 'decline', 'withdraw', 'commit', 'expire'}


def bearer(request):
    scheme, _, token = request.headers.get('authorization', '').partition(' ')
    if scheme.lower() != 'bearer' or not token:
        raise APIError(401, 'unauthenticated')
    return token


def issue_access(settings, member, session_id, scopes):
    now = db.utcnow()
    kid = next(iter(settings.jwt_keys))
    expires = min(now + timedelta(minutes=15), sandbox.expires(member)) if sandbox.guest(member) else now + timedelta(minutes=15)
    if expires <= now:
        raise APIError(401, 'unauthenticated')
    claims = {'iss': 'agent-room', 'aud': settings.domain, 'sub': member['member_id'], 'sid': session_id,
              'role': member['role'], 'scope': scopes, 'typ': 'access', 'iat': int(now.timestamp()),
              'exp': int(expires.timestamp()), 'jti': str(uuid4())}
    return jwt.encode(claims, settings.jwt_keys[kid], algorithm='HS256', headers={'kid': kid})


def new_tokens(conn, settings, member):
    if member['role']=='sandbox_peer':
        raise APIError(403,'internal_identity','Simulated peers do not receive agent credentials.')
    sid, refresh = str(uuid4()), 'art_' + secrets.token_urlsafe(32)
    db.create_session(conn, sid, member['member_id'], digest(refresh))
    if sandbox.guest(member):
        conn.execute('UPDATE sessions SET expires_at=? WHERE session_id=?', (db.stamp(sandbox.expires(member)), sid))
    return token_pair(conn, settings, member, sid, refresh)


def token_pair(conn, settings, member, sid, refresh):
    token = issue_access(settings, member, sid, db.role_scopes(conn, member['role']))
    return {'member_id': member['member_id'], 'access_token': token,
            'refresh_token': refresh, 'access_expires_in': max(0, jwt.decode(token, options={'verify_signature': False})['exp'] - int(db.utcnow().timestamp())),
            'access_expires_at': jwt.decode(token, options={'verify_signature': False})['exp']}


def sealed_reply(settings, pair):
    key = hmac.new(settings.signing_key, b'agent-room-auth-retry-v1', hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(key)).encrypt(json.dumps(pair).encode()).decode()


def opened_reply(settings, sealed):
    key = hmac.new(settings.signing_key, b'agent-room-auth-retry-v1', hashlib.sha256).digest()
    return json.loads(Fernet(base64.urlsafe_b64encode(key)).decrypt(sealed.encode()))


def retry_pair(conn, settings, session, member, sealed, expires_at):
    if not session or session['revoked_at'] or db.parse(session['expires_at']) <= db.utcnow() or not member or member['status'] != 'active':
        raise APIError(401, 'unauthenticated')
    if db.parse(expires_at) <= db.utcnow():
        raise APIError(409, 'auth_retry_expired', 'The authentication retry window expired; reconnect through the owner.')
    if sandbox.guest(member) and (settings.profile != 'sandbox' or sandbox.expires(member) <= db.utcnow()):
        raise APIError(401, 'unauthenticated')
    pair = opened_reply(settings, sealed)
    if not hmac.compare_digest(digest(pair['refresh_token']), session['refresh_hash']):
        raise APIError(409, 'stale_exchange', 'A newer refresh already exists. Reload your shared credential file; do not overwrite it with an older response.')
    return pair


def verify_access(conn, settings, token):
    try:
        header = jwt.get_unverified_header(token)
        if header.get('alg') != 'HS256' or header.get('kid') not in settings.jwt_keys:
            raise ValueError()
        claims = jwt.decode(token, settings.jwt_keys[header['kid']], algorithms=['HS256'],
                            audience=settings.domain, issuer='agent-room', leeway=30,
                            options={'require': ['exp', 'iat', 'sub', 'sid', 'jti', 'role', 'scope', 'typ']})
        if claims['typ'] != 'access' or not all(isinstance(claims[k], str) for k in ['sub', 'sid', 'jti', 'role', 'scope']):
            raise ValueError()
        session = db.session(conn, claims['sid'])
        member = db.member(conn, claims['sub'])
        if not session or not member or session['member_id'] != member['member_id'] or session['revoked_at'] or db.parse(session['expires_at']) <= db.utcnow() or member['status'] != 'active' or member['role'] != claims['role']:
            raise ValueError()
        if sandbox.guest(member) and (settings.profile != 'sandbox' or sandbox.expires(member) <= db.utcnow()):
            raise ValueError()
        scopes = set(claims['scope'].split()) & set((db.role_scopes(conn, member['role']) or '').split())
        return {**member, 'claims': claims, 'can_negotiate': 'negotiate' in scopes}
    except (jwt.PyJWTError, ValueError, KeyError, TypeError):
        raise APIError(401, 'unauthenticated') from None


def authorize(conn, member, action, message_type):
    scopes = set(member['claims']['scope'].split())
    current = set((db.role_scopes(conn, member['role']) or '').split())
    required = {'room:read' if action == 'read' else 'room:write'}
    if message_type in NEGOTIATION_TYPES:
        required.add('negotiate')
    if not required <= (scopes & current) or not db.permission(conn, member['role'], 'read' if action == 'read' else 'send', message_type):
        raise APIError(403, 'forbidden')


def require_read_scope(conn, member):
    if 'room:read' not in member['claims']['scope'].split() or 'room:read' not in (db.role_scopes(conn, member['role']) or '').split():
        raise APIError(403, 'forbidden')


async def member_auth(request: Request, conn=Depends(db.dependency)):
    member = verify_access(conn, request.app.state.settings, bearer(request))
    request.state.member_id = member['member_id']
    now = db.stamp()
    conn.execute('''INSERT INTO member_activity(member_id,last_contact_at) VALUES(?,?)
        ON CONFLICT(member_id) DO UPDATE SET last_contact_at=excluded.last_contact_at
        WHERE member_activity.last_contact_at<?''',
        (member['member_id'], now, db.stamp(db.utcnow() - timedelta(minutes=1))))
    if request.method == 'GET':
        request.app.state.limiter.hit('member_get', member['member_id'], 10, 30)
        require_read_scope(conn, member)
    return member


async def owner_auth(request: Request):
    request.app.state.limiter.hit('owner', request.state.ip, 30 / 60, 30)
    if request.headers.get('authorization'):
        if not hmac.compare_digest(bearer(request).encode(), request.app.state.settings.owner_key.encode()):
            raise APIError(401, 'unauthenticated')
    else:
        from .console import session_auth
        session_auth(request)
    request.state.owner_authorized = True

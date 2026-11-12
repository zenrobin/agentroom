import hmac
import json
import secrets
from datetime import timedelta
from typing import Literal
from uuid import uuid4

import jwt
from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import Field, model_validator

from .. import auth, console, db, negotiation, operations
from ..errors import APIError
from ..models import Join, StrictModel
from ..notify import send_owner_login
from ..signing import canonical_json, digest

router = APIRouter()


class Login(StrictModel):
    token: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')


class Device(Join):
    mode: Literal['new', 'reconnect'] = 'new'
    member_id: str | None = Field(default=None, pattern=r'^m_[a-f0-9]{32}$')

    @model_validator(mode='after')
    def reconnect_target(self):
        if (self.mode == 'reconnect') != bool(self.member_id):
            raise ValueError('Reconnection requires the existing member ID')
        if self.room_code:
            raise ValueError('Pairing uses explicit owner approval, not an invitation code')
        return self


@router.get('/owner')
async def page(request: Request):
    return console.page('owner.html', request.app.state.settings)


@router.post('/v1/console/login', status_code=202)
async def login_request(request: Request, background: BackgroundTasks, conn=Depends(db.dependency)):
    console.same_origin(request)
    settings, limiter = request.app.state.settings, request.app.state.limiter
    limiter.hit('owner_login_ip', request.state.ip, 3 / 3600, 3)
    limiter.hit('owner_login_email', 'room', 1 / 60, 1)
    if not settings.email:
        raise APIError(503, 'owner_email_not_configured', 'Owner email sign-in is not configured.')
    token = secrets.token_urlsafe(32)
    with db.transaction(conn, ('owner_login', 'room')):
        conn.execute('DELETE FROM owner_challenges WHERE expires_at<?', (db.stamp(),))
        conn.execute('INSERT INTO owner_challenges VALUES(?,?,NULL)',
                     (digest(token), db.stamp(db.utcnow() + timedelta(minutes=15))))
        db.audit(conn, 'anonymous', 'owner.login_requested', ip=request.state.ip)
    background.add_task(send_owner_login, settings, token)
    return {'status': 'queued', 'message': 'A sign-in link will be sent to the configured room owner.'}


@router.post('/v1/console/session')
async def login(data: Login, request: Request, conn=Depends(db.dependency)):
    console.same_origin(request)
    request.app.state.limiter.hit('owner_login_exchange', request.state.ip, 5 / 60, 5)
    session = secrets.token_urlsafe(32)
    now = db.stamp()
    expires = db.stamp(db.utcnow() + timedelta(hours=8))
    with db.transaction(conn, ('owner_login', 'room')):
        challenge = db.one(conn, 'SELECT * FROM owner_challenges WHERE challenge_hash=?', (digest(data.token),))
        if not challenge or challenge['used_at'] or challenge['expires_at'] <= now:
            raise APIError(401, 'login_link_invalid', 'This sign-in link expired or was already used. Request a new one.')
        conn.execute('UPDATE owner_challenges SET used_at=? WHERE challenge_hash=?', (now, digest(data.token)))
        conn.execute('INSERT INTO owner_sessions VALUES(?,?,?)', (digest(session), expires, now))
        db.audit(conn, 'owner', 'owner.signed_in', ip=request.state.ip)
    response = JSONResponse({'csrf': console.csrf(request.app.state.settings, session), 'expires_at': expires})
    response.set_cookie(console.cookie_name(request.app.state.settings), session, max_age=8 * 3600,
                        secure=not request.app.state.settings.dev, httponly=True, samesite='strict', path='/')
    return response


@router.get('/v1/console/session')
async def current(request: Request):
    row = console.session_auth(request)
    return {'csrf': console.csrf(request.app.state.settings, request.state.owner_session), 'expires_at': row['expires_at']}


@router.post('/v1/console/logout')
async def logout(request: Request, conn=Depends(db.dependency)):
    console.session_auth(request)
    conn.execute('DELETE FROM owner_sessions WHERE session_hash=?', (digest(request.state.owner_session),))
    db.audit(conn, 'owner', 'owner.signed_out', ip=request.state.ip)
    response = JSONResponse({'status': 'signed_out'})
    response.delete_cookie(console.cookie_name(request.app.state.settings), path='/', secure=not request.app.state.settings.dev,
                           httponly=True, samesite='strict')
    return response


@router.get('/v1/owner/overview', dependencies=[Depends(auth.owner_auth)])
async def overview(request: Request, conn=Depends(db.dependency)):
    settings = request.app.state.settings
    members = db.rows(conn, """SELECT m.*,a.last_contact_at,a.last_inbox_at,c.acked_seq,
        (SELECT max(expires_at) FROM sessions s WHERE s.member_id=m.member_id AND s.revoked_at IS NULL AND s.expires_at>?) AS session_expires_at
        FROM members m LEFT JOIN member_activity a ON a.member_id=m.member_id
        LEFT JOIN member_cursors c ON c.member_id=m.member_id ORDER BY m.created_at""", (db.stamp(),))
    with db.transaction(conn):
        db.expire_joins(conn)
    operations.connection_checks(conn,members)
    return {'name': settings.room_name, 'members': members, 'pending': db.list_joins(conn, 'pending'),
            'pairings': db.rows(conn, "SELECT user_code,mode,member_id,applicant,expires_at FROM device_connections WHERE status='pending' AND expires_at>? ORDER BY created_at", (db.stamp(),)),
            'invitation': {'guide': settings.public_url + '/agent-guide', 'room_code': settings.room_code},
            'delivery': {'worker': 'vercel-queues' if settings.serverless else 'local-worker',
                         'last_check': db.one(conn, 'SELECT * FROM delivery_worker_checks ORDER BY created_at DESC LIMIT 1'),
                         'counts': db.rows(conn, 'SELECT status,count(*) AS count FROM webhook_deliveries GROUP BY status')},
            'room_policy': {'profile': settings.profile, 'encryption': 'Server-readable; no end-to-end encryption',
                            'auditing': 'Owner audit log; configurable rules and auditor roles are planned'}}


@router.post('/v1/owner/delivery/check', status_code=202, dependencies=[Depends(auth.owner_auth)])
async def worker_check(request: Request, conn=Depends(db.dependency)):
    request.app.state.limiter.hit('worker_check', 'room', 1 / 60, 1)
    check_id = str(uuid4())
    conn.execute("INSERT INTO delivery_worker_checks VALUES(?,'queued',?,NULL)", (check_id, db.stamp()))
    if request.app.state.settings.serverless:
        from vercel.queue import QueueClient
        try:
            await QueueClient(region='iad1', timeout=5).send('agent-room-webhooks', {'check_id': check_id},
                idempotency_key='worker-check-' + check_id)
        except Exception:
            conn.execute("UPDATE delivery_worker_checks SET status='publish_failed',completed_at=? WHERE check_id=?", (db.stamp(), check_id))
            raise APIError(503, 'queue_unavailable', 'The delivery worker check could not be queued.') from None
    else:
        conn.execute("UPDATE delivery_worker_checks SET status='completed',completed_at=? WHERE check_id=?", (db.stamp(), check_id))
    db.audit(conn, 'owner', 'delivery.worker_check', check_id, request.state.ip)
    return {'check_id': check_id, 'status': 'queued' if request.app.state.settings.serverless else 'completed'}


@router.post('/v1/connect', status_code=202)
async def connect(data: Device, request: Request, conn=Depends(db.dependency)):
    limiter = request.app.state.limiter
    body = data.model_dump(mode='json')
    hashed = digest(canonical_json(body))
    rid = str(data.request_id)
    previous = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (rid,))
    if not previous or not hmac.compare_digest(previous['body_hash'], hashed):
        # A safe resume proves possession of the original secret/payload and does
        # not consume admission quota again. Invalid guesses still consume it.
        limiter.hit('device_connect', request.state.ip, 3 / 3600, 3)
    with db.transaction(conn, ('device_connect', 'room')):
        prior = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (rid,))
        if prior:
            if not hmac.compare_digest(prior['body_hash'], hashed):
                raise APIError(409, 'id_conflict')
            row = prior
        else:
            pending = conn.execute("SELECT count(*) FROM device_connections WHERE status='pending' AND expires_at>?", (db.stamp(),)).fetchone()[0]
            if pending >= 50:
                raise APIError(503, 'queue_full')
            alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
            code = ''.join(secrets.choice(alphabet) for _ in range(5)) + '-' + ''.join(secrets.choice(alphabet) for _ in range(5))
            applicant = {k: v for k, v in body.items() if k not in {'poll_secret', 'room_code'}}
            conn.execute('INSERT INTO device_connections(request_id,poll_hash,body_hash,user_code,mode,member_id,applicant,status,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                         (rid, digest(data.poll_secret), hashed, code, data.mode, data.member_id, canonical_json(applicant), 'pending', db.stamp(db.utcnow() + timedelta(minutes=15)), db.stamp()))
            db.audit(conn, 'anonymous', 'connection.requested', rid, request.state.ip)
            row = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (rid,))
    return {'request_id': rid, 'user_code': row['user_code'], 'expires_at': row['expires_at'], 'poll_after_s': 5,
            'verification_uri': request.app.state.settings.public_url + '/owner#pair=' + row['user_code']}


@router.get('/v1/connect/{request_id}')
async def connect_poll(request_id: str, request: Request, conn=Depends(db.dependency)):
    request.app.state.limiter.hit('device_poll', request.state.ip, 1, 15)
    row = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (request_id,))
    supplied = digest(request.headers.get('x-connection-secret', ''))
    if not row or not hmac.compare_digest(supplied, row['poll_hash']):
        raise APIError(404, 'not_found')
    if row['expires_at'] <= db.stamp():
        return {'status': 'expired'}
    if row['status'] != 'approved':
        return {'status': row['status']}
    # Only successful token pickup needs the ordered room mutation lock. Junk
    # polls and pending status checks cannot queue ahead of real messages.
    with db.transaction(conn):
        row = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (request_id,))
        supplied = digest(request.headers.get('x-connection-secret', ''))
        if not row or not hmac.compare_digest(supplied, row['poll_hash']):
            raise APIError(404, 'not_found')
        if row['expires_at'] <= db.stamp():
            return {'status': 'expired'}
        if row['status'] != 'approved':
            return {'status': row['status']}
        member = db.member(conn, row['member_id'])
        if not member or member['status'] != 'active':
            raise APIError(401, 'unauthenticated')
        if row['reply']:
            pair = auth.retry_pair(conn, request.app.state.settings, db.session(conn, row['session_id']), member,
                                   row['reply'], row['reply_expires_at'])
        else:
            pair = auth.new_tokens(conn, request.app.state.settings, member)
            sid = jwt.decode(pair['access_token'], options={'verify_signature': False})['sid']
            conn.execute('UPDATE device_connections SET reply=?,session_id=?,reply_expires_at=? WHERE request_id=?',
                         (auth.sealed_reply(request.app.state.settings, pair), sid, db.stamp(db.utcnow() + timedelta(minutes=5)), request_id))
            db.audit(conn, member['member_id'], 'connection.collected', request_id, request.state.ip)
    return {'status': 'approved', **pair}


@router.post('/v1/owner/connections/{code}/{decision}', dependencies=[Depends(auth.owner_auth)])
async def decide(code: str, decision: Literal['approve', 'deny'], request: Request, conn=Depends(db.dependency)):
    with db.transaction(conn):
        row = db.one(conn, 'SELECT * FROM device_connections WHERE user_code=?', (code,))
        if not row:
            raise APIError(404, 'not_found')
        if row['status'] != 'pending' or row['expires_at'] <= db.stamp():
            raise APIError(409, 'connection_closed')
        mid = row['member_id']
        if decision == 'approve':
            applicant = json.loads(row['applicant'])
            if row['mode'] == 'reconnect':
                member = db.member(conn, mid)
                if not member or member['status'] != 'active':
                    raise APIError(409, 'fresh_join_required')
                if member['role']=='sandbox_peer':
                    raise APIError(409,'internal_identity','Simulated peers cannot be reconnected.')
                db.revoke_member(conn, mid)
                # Invalidate every other recovery grant for the old session/identity.
                conn.execute('DELETE FROM reconnect_tickets WHERE member_id=?', (mid,))
                conn.execute("UPDATE device_connections SET status='denied' WHERE member_id=? AND request_id<>?", (mid, row['request_id']))
            else:
                if db.name_taken(conn, applicant['agent_name']):
                    raise APIError(409, 'name_taken', 'This name already exists. Use reconnect for an existing agent.')
                if db.join_counts(conn)[0] >= request.app.state.settings.max_members:
                    raise APIError(403, 'room_full')
                mid = 'm_' + uuid4().hex
                db.add_identity(conn, applicant, mid, 'member')
                negotiation.server_message(conn, request.app.state.settings, 'system', {'event': 'member.joined', 'member_id': mid})
            conn.execute("UPDATE device_connections SET status='approved',member_id=?,expires_at=? WHERE request_id=?",
                         (mid, db.stamp(db.utcnow() + timedelta(minutes=15)), row['request_id']))
        else:
            conn.execute("UPDATE device_connections SET status='denied' WHERE request_id=?", (row['request_id'],))
        db.audit(conn, 'owner', 'connection.' + decision, row['request_id'], request.state.ip)
    request.app.state.hub.publish()
    return {'status': 'approved' if decision == 'approve' else 'denied', 'member_id': mid}

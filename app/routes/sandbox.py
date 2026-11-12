import hmac
from datetime import timedelta
from uuid import UUID, uuid4

import jwt
from fastapi import APIRouter, Depends, Request
from pydantic import Field, UUID4

from .. import auth, db, negotiation, room_rules, sandbox
from ..errors import APIError
from ..models import StrictModel
from ..signing import canonical_json, digest

router = APIRouter(prefix='/v1/sandbox')


def enabled(request: Request):
    if request.app.state.settings.profile != 'sandbox':
        raise APIError(404, 'not_found')


class Admission(StrictModel):
    request_id: UUID4
    poll_secret: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')


@router.post('/join', dependencies=[Depends(enabled)])
async def join(data: Admission, request: Request, conn=Depends(db.dependency)):
    state, rid = request.app.state, str(data.request_id)
    hashed = digest(canonical_json(data.model_dump(mode='json')))
    prior = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (rid,))
    if not prior or not hmac.compare_digest(prior['body_hash'], hashed):
        state.limiter.hourly('sandbox_admission_ip', request.state.ip, 3)
        state.limiter.hourly('sandbox_admission_global', 'room', 30)
    with db.transaction(conn):
        prior = db.one(conn, 'SELECT * FROM device_connections WHERE request_id=?', (rid,))
        if prior:
            if not hmac.compare_digest(prior['body_hash'], hashed):
                raise APIError(409, 'id_conflict')
            member = db.member(conn, prior['member_id'])
            pair = auth.retry_pair(conn, state.settings, db.session(conn, prior['session_id']), member,
                                   prior['reply'], prior['reply_expires_at'])
        else:
            sandbox.cleanup(conn)
            if conn.execute("SELECT count(*) FROM members WHERE role=? AND status='active'", (sandbox.ROLE,)).fetchone()[0] >= 10:
                raise APIError(503, 'sandbox_full', 'Ten test sessions are active. Try again after a session expires.')
            mid = 'm_' + uuid4().hex
            db.add_identity(conn, {'agent_name': 'guest-' + mid[2:], 'display_name': 'Temporary test agent',
                            'owner_name': 'Demo visitor', 'platform': 'Sandbox'}, mid, sandbox.ROLE)
            member = db.member(conn, mid)
            pair = auth.new_tokens(conn, state.settings, member)
            sid = jwt.decode(pair['access_token'], options={'verify_signature': False})['sid']
            conn.execute('INSERT INTO device_connections(request_id,poll_hash,body_hash,user_code,mode,member_id,applicant,status,expires_at,session_id,reply,reply_expires_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                         (rid, digest(data.poll_secret), hashed, rid, 'sandbox', mid, '{}', 'approved',
                          db.stamp(sandbox.expires(member)), sid, auth.sealed_reply(state.settings, pair),
                          db.stamp(db.utcnow() + timedelta(minutes=5)), db.stamp()))
    return {**pair, 'status': 'approved', 'peer_id': sandbox.PEER, 'expires_at': db.stamp(sandbox.expires(member)),
            'notice': 'Temporary private test lane. Do not enter personal data. No external actions. Demo data is purged after 24 hours on cleanup.'}


class Acceptance(StrictModel):
    version: int = Field(ge=1)
    terms_hash: str = Field(pattern=r'^[a-f0-9]{64}$')


@router.post('/peer/accept/{nid}', dependencies=[Depends(enabled)])
async def peer_accept(nid: UUID, data: Acceptance, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    if not sandbox.guest(actor):
        raise APIError(403, 'forbidden')
    auth.authorize(conn, actor, 'send', 'accept')
    request.app.state.limiter.hourly('sandbox_peer', actor['member_id'], 30)
    expired = False
    with db.transaction(conn):
        current_actor = auth.verify_access(conn, request.app.state.settings, auth.bearer(request))
        auth.authorize(conn, current_actor, 'send', 'accept')
        parties = db.parties(conn, str(nid))
        if set(parties) != {actor['member_id'], sandbox.PEER}:
            raise APIError(404, 'not_found')
        # The guest triggers this acceptance. Server system messages stay exempt.
        room_rules.enforce_operation(conn, current_actor, 'accept', {'negotiation_id': str(nid), **data.model_dump()})
        current = db.negotiation(conn, str(nid))
        if current['state'] == 'committed':
            commit = db.commit_message(conn, str(nid))
            if commit['body']['version'] != data.version or commit['body']['terms_hash'] != data.terms_hash:
                raise APIError(409, 'stale_version')
            return {'commit': db.visible_message(conn, current_actor, commit['seq']), 'status': 'committed'}
        message = {'id': str(uuid4()), 'from': sandbox.PEER, 'to': [actor['member_id']], 'type': 'accept',
                   'ts': db.stamp(), 'received_at': db.stamp(), 'thread_id': str(nid),
                   'body': {'negotiation_id': str(nid), **data.model_dump()}}
        peer = db.member(conn, sandbox.PEER)
        if current['state'] == 'open' and db.parse(db.version(conn, str(nid), current['current_version'])['expires_at']) <= db.utcnow():
            negotiation.expire_due(conn, request.app.state.settings)
            expired = True
        else:
            context = negotiation.validate(conn, peer, message)
            message = db.insert_message(conn, message)
            emitted = negotiation.apply(conn, request.app.state.settings, peer, message, context)
            result = {'simulated_acceptance': db.visible_message(conn, current_actor, message['seq']),
                      'commit': db.visible_message(conn, current_actor, emitted[0]['seq']) if emitted else None}
    request.app.state.hub.publish()
    # Commit expiry even though the attempted action is rejected.
    if expired:
        raise APIError(409, 'negotiation_closed')
    return result

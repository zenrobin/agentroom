import asyncio
import json
import time
from uuid import UUID
from datetime import timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import ValidationError

from .. import auth, db, negotiation, room_rules, sandbox
from ..errors import APIError
from ..models import Ack, AgentRoomMessageSubmit
from ..room_rules import AckVersion
from ..signing import canonical_json, digest, public_key

router = APIRouter(prefix='/v1')
I64 = 2 ** 63 - 1


def expire(request, conn):
    with db.transaction(conn):
        emitted = negotiation.expire_due(conn, request.app.state.settings)
    if emitted:
        request.app.state.hub.publish()


@router.get('/rules')
async def rules(actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    return room_rules.view(conn, actor['member_id'])


@router.post('/rules/ack')
async def acknowledge_rules(data: AckVersion, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    with db.transaction(conn):
        version = room_rules.acknowledge(conn, actor['member_id'], data.version)
    return {'version': version, 'acknowledged': True}


@router.get('/me')
async def me(actor=Depends(auth.member_auth)):
    return {key: actor[key] for key in ['member_id', 'agent_name', 'role']} | {'scopes': actor['claims']['scope'].split()}


@router.get('/members')
async def members(actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    if sandbox.guest(actor):
        return [m for m in db.members(conn) if m['member_id'] in {actor['member_id'], sandbox.PEER}]
    return db.members(conn)


@router.post('/token/revoke', status_code=204)
async def logout(request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    with db.transaction(conn):
        db.revoke_session(conn, actor['claims']['sid'])
        db.audit(conn, actor['member_id'], 'token.revoked', actor['claims']['sid'], request.state.ip)
    request.app.state.hub.publish()
    return Response(status_code=204)


@router.post('/messages')
async def send(request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    state, mid = request.app.state, actor['member_id']
    state.limiter.hit('send', mid, 2, 10)
    state.limiter.hourly('send_hour', mid, 500)
    raw = bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw) > 32768:
            raise APIError(413, 'payload_too_large')
    if request.headers.get('content-type', '').split(';')[0].strip().lower() != 'application/json':
        raise APIError(415, 'unsupported_media_type')
    try:
        submitted = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if isinstance(submitted, dict) and submitted.get('type') in {'commit', 'expire', 'system'}:
            raise APIError(400, 'reserved_type')
        model = AgentRoomMessageSubmit.model_validate(submitted).root
    except (ValueError, ValidationError, RecursionError):
        raise APIError(422, 'validation_error', 'Invalid message envelope or body') from None
    if model.sender is not None and model.sender != mid:
        raise APIError(403, 'sender_mismatch')
    sandbox.check_send(state.settings, actor, model, len(raw), state.limiter)
    hashed = digest(canonical_json(submitted))
    # Expiry is committed independently so an ensuing negotiation_closed error cannot undo it.
    expire(request, conn)
    deferred_error = None
    transaction_expiries = []
    with db.transaction(conn):
        # Body upload is an await point: revocation or role changes may have happened meanwhile.
        actor = auth.verify_access(conn, state.settings, auth.bearer(request))
        prior = db.prior_message(conn, mid, str(model.id))
        if prior:
            if prior['submit_hash'] != hashed:
                raise APIError(409, 'id_conflict')
            return JSONResponse(db.envelope(prior), status_code=200)
        if abs((db.utcnow() - model.ts).total_seconds()) > 300:
            raise APIError(400, 'timestamp_out_of_window')
        if db.nonce_exists(conn, mid, model.nonce):
            raise APIError(409, 'nonce_reused')
        auth.authorize(conn, actor, 'send', model.type)
        room_rules.enforce(conn, actor, model)
        recipients = model.to or []
        followup = model.type in {'counter', 'accept', 'decline', 'withdraw'}
        if mid in recipients or any(not (member := db.member(conn, recipient)) or (not followup and member['status'] != 'active') for recipient in recipients):
            raise APIError(400, 'bad_recipients')
        # Require a visible thread/reply, avoiding blind writes into another member's private thread.
        if model.type != 'propose' and str(model.thread_id) != str(model.id) and not db.thread_exists(conn, actor, model.thread_id):
            raise APIError(404, 'not_found')
        if model.reply_to and not db.visible_id(conn, actor, str(model.reply_to)):
            raise APIError(404, 'not_found')
        message = {**model.model_dump(mode='json', by_alias=True, exclude_unset=True), 'from': mid, 'received_at': db.stamp()}
        # The write lock may have been held by another instance while terms expired.
        # Reconcile under the SAME lock as the transition; commit expiry even when rejecting the accept.
        if model.type in auth.NEGOTIATION_TYPES:
            transaction_expiries = negotiation.expire_due(conn, state.settings)
        try:
            context = negotiation.validate(conn, actor, message) if model.type in auth.NEGOTIATION_TYPES else None
        except APIError as error:
            if error.code == 'negotiation_closed' and any(m['body']['negotiation_id'] == message['body'].get('negotiation_id') for m in transaction_expiries):
                deferred_error = error
                context = None
            else:
                raise
        emitted = []
        if deferred_error is None:
            db.add_nonce(conn, mid, model.nonce)
            message = db.insert_message(conn, message, hashed)
            if context:
                emitted = negotiation.apply(conn, state.settings, actor, message, context)
            if sandbox.guest(actor):
                sandbox.reply(conn, state.settings, message)
    for stored in [*([] if deferred_error else [message]), *emitted, *transaction_expiries]:
        state.message_counts[stored['type']] += 1
    state.hub.publish()
    if deferred_error:
        raise deferred_error
    return JSONResponse(message, status_code=201)


async def page(request, conn, actor, after, limit, wait, thread_id=None, inbox=False):
    now = db.stamp()
    conn.execute('''INSERT INTO member_activity(member_id,last_contact_at,last_inbox_at) VALUES(?,?,?)
        ON CONFLICT(member_id) DO UPDATE SET last_inbox_at=excluded.last_inbox_at
        WHERE member_activity.last_inbox_at IS NULL OR member_activity.last_inbox_at<?''',
        (actor['member_id'], now, now, db.stamp(db.utcnow() - timedelta(minutes=1))))
    expire(request, conn)
    deadline = time.monotonic() + wait
    expiry_checked_at = time.monotonic()
    with request.app.state.hub.subscribe(actor['member_id'], 'poll') as queue:
        while True:
            if not request.app.state.hub.renew(queue, conn):
                raise APIError(503, 'unavailable', 'Connection lease expired; reconnect')
            # Recheck session, current role and scopes while the request is held open.
            actor = auth.verify_access(conn, request.app.state.settings, auth.bearer(request))
            auth.require_read_scope(conn, actor)
            if request.app.state.settings.database_url and time.monotonic() - expiry_checked_at >= 1:
                expire(request, conn)
                expiry_checked_at = time.monotonic()
            if inbox:
                after = db.cursor(conn, actor['member_id'])
            result = db.message_page(conn, actor, after, limit, thread_id, inbox)
            if inbox:
                result['acked_seq'] = after
            remaining = deadline - time.monotonic()
            if result['items'] or remaining <= 0:
                return result
            if await request.is_disconnected():
                return result
            try:
                await asyncio.wait_for(queue.get(), min(remaining, 1.5 if request.app.state.settings.database_url else 1))
            except asyncio.TimeoutError:
                pass


@router.get('/messages')
async def messages(request: Request, after: int = Query(0, ge=0, le=I64), limit: int = Query(50, ge=1, le=200),
                   wait: int = Query(0, ge=0, le=120), thread_id: UUID | None = None,
                   actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    return await page(request, conn, actor, after, limit, wait, thread_id)


@router.get('/inbox')
async def inbox(request: Request, limit: int = Query(50, ge=1, le=200), wait: int = Query(0, ge=0, le=120),
                actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    return await page(request, conn, actor, 0, limit, wait, inbox=True)


@router.post('/inbox/ack')
async def ack(data: Ack, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    auth.require_read_scope(conn, actor)
    with db.transaction(conn):
        seq = db.ack(conn, actor, min(data.seq, I64))
    return {'acked_seq': seq}


@router.get('/messages/{seq}')
async def get_message(request: Request, seq: int, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    expire(request, conn)
    message = db.visible_message(conn, actor, seq) if 0 < seq <= I64 else None
    if not message:
        raise APIError(404, 'not_found')
    auth.authorize(conn, actor, 'read', message['type'])
    return message


@router.get('/stream')
async def stream(request: Request, after: int = Query(0, ge=0, le=I64), actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    try:
        after = int(request.headers.get('last-event-id', str(after)))
        if not 0 <= after <= I64:
            raise ValueError()
    except ValueError:
        raise APIError(422, 'validation_error', 'Last-Event-ID must be a nonnegative sequence') from None
    expire(request, conn)
    subscription = request.app.state.hub.subscribe(actor['member_id'], 'sse')
    queue = subscription.__enter__()

    async def events():
        nonlocal after, actor
        started, ping_at = time.monotonic(), time.monotonic()
        expiry_checked_at = time.monotonic()
        try:
            yield ': connected\n\n'
            while time.monotonic() - started < (240 if request.app.state.settings.database_url else 1800):
                if not request.app.state.hub.renew(queue, conn):
                    return
                try:
                    actor = auth.verify_access(conn, request.app.state.settings, auth.bearer(request))
                    auth.require_read_scope(conn, actor)
                except APIError:
                    return
                if request.app.state.settings.database_url and time.monotonic() - expiry_checked_at >= 1:
                    expire(request, conn)
                    expiry_checked_at = time.monotonic()
                result = db.message_page(conn, actor, after, 200)
                for message in result['items']:
                    after = message['seq']
                    yield f'id: {after}\nevent: message\ndata: {canonical_json(message)}\n\n'
                if result['has_more']:
                    continue
                if time.monotonic() - ping_at >= 15:
                    yield ': ping\n\n'
                    ping_at = time.monotonic()
                try:
                    await asyncio.wait_for(queue.get(), 1.5 if request.app.state.settings.database_url else 4)
                except asyncio.TimeoutError:
                    pass
        finally:
            subscription.__exit__(None, None, None)
    return StreamingResponse(events(), media_type='text/event-stream', headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


def readable_negotiation(conn, actor, nid):
    data = db.negotiation(conn, nid)
    if not data or actor['member_id'] not in db.parties(conn, nid):
        raise APIError(404, 'not_found')
    version = db.version(conn, nid, data['current_version'])
    if not db.visible_message(conn, actor, version['message_seq']):
        raise APIError(404, 'not_found')
    return data


@router.get('/negotiations')
async def negotiations(request: Request, state: str = Query('open', pattern='^(open|committed|declined|withdrawn|expired)$'),
                       actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    expire(request, conn)
    return db.list_negotiations(conn, actor, state)


@router.get('/negotiations/{negotiation_id}')
async def get_negotiation(negotiation_id: UUID, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    expire(request, conn)
    nid = str(negotiation_id)
    readable_negotiation(conn, actor, nid)
    data = db.negotiation_detail(conn, nid)
    # Each version's message permission applies, including custom per-type overrides.
    data['versions'] = [v for v in data['versions'] if db.visible_message(conn, actor, db.version(conn, nid, v['version'])['message_seq'])]
    if data['commit'] and not db.visible_message(conn, actor, data['commit']['seq']):
        data['commit'] = None
    return data


@router.get('/commits/{negotiation_id}')
async def commit(negotiation_id: UUID, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    nid = str(negotiation_id)
    readable_negotiation(conn, actor, nid)
    message = db.commit_message(conn, nid)
    if not message or not db.visible_message(conn, actor, message['seq']):
        raise APIError(404, 'not_found')
    auth.authorize(conn, actor, 'read', 'commit')
    return {'commit_message': message, 'public_key': public_key(request.app.state.settings.signing_key)}

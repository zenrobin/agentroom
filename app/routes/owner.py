import json
import secrets
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, Query, Request
from fastapi.responses import PlainTextResponse

from .. import auth, db, negotiation, room_rules
from ..errors import APIError
from ..models import Approve, Deny
from ..notify import notify_owner
from ..signing import digest

router = APIRouter(prefix='/v1/owner', dependencies=[Depends(auth.owner_auth)])


@router.get('/requests')
async def requests(status: str = Query('pending', pattern='^(pending|approved|denied|expired)$'), conn=Depends(db.dependency)):
    with db.transaction(conn):
        db.expire_joins(conn)
    return db.list_joins(conn, status)


@router.post('/requests/{request_id}/approve')
async def approve(request_id: str, request: Request, data: Approve | None = None, conn=Depends(db.dependency)):
    role = (data or Approve()).role
    with db.transaction(conn):
        db.expire_joins(conn)
        row = db.join_request(conn, request_id)
        if not row:
            raise APIError(404, 'not_found')
        if row['status'] in {'approved', 'delivered'}:
            return {'request_id': request_id, 'status': 'approved', 'member_id': row['member_id']}
        if row['status'] != 'pending':
            raise APIError(409, 'invalid_request', 'Request is already closed')
        if db.role_scopes(conn, role) is None:
            raise APIError(422, 'validation_error', 'Unknown role')
        if db.join_counts(conn)[0] >= request.app.state.settings.max_members:
            raise APIError(403, 'room_full')
        mid = 'm_' + uuid4().hex
        db.add_member(conn, row, mid, role)
        negotiation.server_message(conn, request.app.state.settings, 'system', {'event': 'member.joined', 'member_id': mid})
        db.audit(conn, 'owner', 'join.approved', request_id, request.state.ip)
    request.app.state.hub.publish()
    return {'request_id': request_id, 'status': 'approved', 'member_id': mid}


@router.post('/requests/{request_id}/deny')
async def deny(request_id: str, request: Request, data: Deny | None = None, conn=Depends(db.dependency)):
    with db.transaction(conn):
        db.expire_joins(conn)
        row = db.join_request(conn, request_id)
        if not row:
            raise APIError(404, 'not_found')
        if row['status'] == 'denied':
            return {'request_id': request_id, 'status': 'denied'}
        if row['status'] != 'pending':
            raise APIError(409, 'invalid_request', 'Request is already closed')
        db.decide_join(conn, request_id, 'denied')
        # The free-text reason is deliberately not logged or returned to the applicant.
        db.audit(conn, 'owner', 'join.denied', request_id, request.state.ip)
    return {'request_id': request_id, 'status': 'denied'}


@router.post('/requests/{request_id}/notify', status_code=202)
async def renotify(request_id: str, request: Request, background: BackgroundTasks, conn=Depends(db.dependency)):
    with db.transaction(conn):
        db.expire_joins(conn)
    row = db.join_request(conn, request_id)
    if not row:
        raise APIError(404, 'not_found')
    if row['status'] != 'pending':
        raise APIError(409, 'invalid_request', 'Only pending applications can be sent for review')
    request.app.state.limiter.hit('review_email', request_id, 1 / 60, 1)
    summary = {k: row[k] for k in ['request_id', 'agent_name', 'display_name', 'owner_name',
                                  'owner_contact', 'platform', 'purpose', 'ip']}
    summary['_delivery_id'] = str(uuid4())
    background.add_task(notify_owner, request.app.state.settings, summary)
    return {'request_id': request_id, 'status': 'queued'}


@router.get('/members')
async def members(conn=Depends(db.dependency)):
    return db.members(conn, owner=True)


@router.post('/members/{member_id}/test', status_code=201)
async def test_connection(member_id: str, request: Request, conn=Depends(db.dependency)):
    request.app.state.limiter.hit('member_test', member_id, 1 / 30, 1)
    with db.transaction(conn):
        member = db.member(conn, member_id)
        if not member or member['status'] != 'active':
            raise APIError(404, 'not_found')
        message = negotiation.server_message(conn, request.app.state.settings, 'system',
            {'event': 'connection.test', 'text': 'The room owner sent a connection test. Acknowledge only after processing.'}, [member_id])
        conn.execute('INSERT INTO connection_checks VALUES(?,?,?,?)',(message['id'],member_id,message['seq'],message['received_at']))
        db.audit(conn, 'owner', 'member.test_sent', member_id, request.state.ip)
    request.app.state.hub.publish()
    return {'message_id': message['id'], 'seq': message['seq'], 'status': 'queued',
            'message': 'Queued for the agent; this does not confirm the agent has fetched or processed it.'}


@router.post('/members/{member_id}/revoke')
async def revoke(member_id: str, request: Request, conn=Depends(db.dependency)):
    with db.transaction(conn):
        row = db.member(conn, member_id)
        if not row:
            raise APIError(404, 'not_found')
        if row['status'] != 'removed':
            db.revoke_member(conn, member_id, remove=True)
            negotiation.server_message(conn, request.app.state.settings, 'system', {'event': 'member.removed', 'member_id': member_id})
            db.audit(conn, 'owner', 'member.revoked', member_id, request.state.ip)
    request.app.state.hub.publish()
    return {'member_id': member_id, 'status': 'removed'}


@router.post('/members/{member_id}/reissue')
async def reissue(member_id: str, request: Request, conn=Depends(db.dependency)):
    raise APIError(410, 'fresh_join_required',
                   'Credential reissue is disabled. Revoke the old member, then submit a fresh join with a new agent name and poll secret.')


@router.post('/members/{member_id}/reconnect', status_code=201)
async def reconnect(member_id: str, request: Request, conn=Depends(db.dependency)):
    request.app.state.limiter.hit('owner_reconnect', member_id, 1 / 60, 1)
    ticket_id, secret = str(uuid4()), secrets.token_urlsafe(32)
    with db.transaction(conn):
        member = db.member(conn, member_id)
        if not member:
            raise APIError(404, 'not_found')
        if member['status'] != 'active':
            raise APIError(409, 'invalid_request', 'Removed members must submit a fresh join.')
        if member['role']=='sandbox_peer':
            raise APIError(409,'internal_identity','Simulated peers cannot be reconnected.')
        db.revoke_member(conn, member_id)
        db.create_reconnect(conn, ticket_id, member_id, digest(secret))
        db.audit(conn, 'owner', 'member.reconnect_issued', member_id, request.state.ip)
    request.app.state.hub.publish()
    return {'url': 'https://' + request.app.state.settings.domain, 'ticket_id': ticket_id, 'secret': secret,
            'member_id': member_id, 'agent_name': member['agent_name'], 'expires_in': 86400}


@router.get('/audit')
async def audit(after: int = Query(0, ge=0, le=2**63-1), limit: int = Query(50, ge=1, le=200), conn=Depends(db.dependency)):
    return db.audit_page(conn, after, limit)


@router.get('/metrics')
async def metrics(request: Request, conn=Depends(db.dependency)):
    state = request.app.state
    counts = db.metric_counts(conn)
    lines = []
    http_counts = {tuple(json.loads(key)): value for key, value in db.shared_metrics(conn, 'http').items()} if state.settings.database_url else state.http_counts
    message_counts = db.shared_metrics(conn, 'messages') if state.settings.database_url else state.message_counts
    failures = sum(n for (_, status), n in http_counts.items() if status in {401, 403}) if state.settings.database_url else state.auth_failures
    for (route, status), n in sorted(http_counts.items()):
        lines.append(f'ar_http_requests_total{{route={json.dumps(route)},status="{status}"}} {n}')
    for kind, n in sorted(message_counts.items()):
        lines.append(f'ar_messages_total{{type={json.dumps(kind)}}} {n}')
    lines.append(f'ar_auth_failures_total {failures}')
    for scope, n in sorted(state.limiter.counts.items()):
        lines.append(f'ar_rate_limited_total{{scope={json.dumps(scope)}}} {n}')
    lines.append(f'ar_sse_streams {state.hub.streams}')
    for row in counts['joins']:
        lines.append(f'ar_join_requests_total{{status={json.dumps(row["status"])}}} {row["n"]}')
    lines.append(f'ar_commits_total {counts["commits"]}')
    return PlainTextResponse('\n'.join(lines) + '\n', media_type='text/plain; version=0.0.4')


@router.get('/audit/recent')
async def recent_audit(conn=Depends(db.dependency)):
    return db.rows(conn, 'SELECT * FROM audit_log ORDER BY id DESC LIMIT 50')


@router.get('/rules')
async def list_rules(conn=Depends(db.dependency)):
    versions = []
    for row in db.rows(conn, 'SELECT version, document, created_at FROM room_rules ORDER BY version'):
        versions.append({'version': row['version'], 'created_at': row['created_at'], 'document': json.loads(row['document'])})
    current = room_rules.active(conn)
    return {'active_version': current['version'] if current else None, 'versions': versions}


@router.post('/rules', status_code=201)
async def publish_rules(request: Request, conn=Depends(db.dependency)):
    try:
        payload = await request.json()
    except Exception:
        raise APIError(422, 'validation_error', 'Invalid room rules document') from None
    with db.transaction(conn):
        version = room_rules.publish(conn, payload)
        db.audit(conn, 'owner', 'rules.published', str(version), request.state.ip)
    return {'version': version, 'active_version': version}

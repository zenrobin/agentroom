import hmac
import jwt
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import PlainTextResponse

from .. import auth, db, providers, room_rules
from ..errors import APIError
from ..models import Join, Refresh, Reconnect
from ..notify import notify_owner
from ..signing import canonical_json, digest, public_key

router = APIRouter()


@router.get('/developers/{document}.md', response_class=PlainTextResponse)
async def developer_document(document: str):
    sources = {'strategy': 'developer-strategy.md', 'home-team': 'roadmap-home-team.md',
               'our-plan': 'roadmap-our-plan.md'}
    if document not in sources:
        raise APIError(404, 'not_found')
    # Publish only reviewed, product-neutral documents; never arbitrary repository files.
    return Path(__file__).parent.parent.joinpath('static', sources[document]).read_text()


@router.get('/agent-guide', response_class=PlainTextResponse)
async def agent_guide(request: Request):
    settings = request.app.state.settings
    name = 'sandbox-guide.md' if settings.profile == 'sandbox' else 'agent-guide.md'
    text = Path(__file__).parent.parent.joinpath('static', name).read_text()
    return text.replace('{{ROOM_URL}}', settings.public_url).replace('{{PRODUCT_NAME}}', settings.room_name)


@router.get('/client/room_client.py', response_class=PlainTextResponse)
async def reference_client():
    # Fixed, reviewed public source only. Never expose arbitrary repository paths or env files.
    return Path(__file__).resolve().parents[2].joinpath('clients/room_client.py').read_text()


@router.get('/client/room_connector.py', response_class=PlainTextResponse)
async def outbound_client():
    return Path(__file__).resolve().parents[2].joinpath('clients/room_connector.py').read_text()


@router.get('/client/webhook_receiver.py', response_class=PlainTextResponse)
async def reference_receiver():
    return Path(__file__).resolve().parents[2].joinpath('examples/webhook_receiver.py').read_text()


@router.get('/runtime-guide', response_class=PlainTextResponse)
async def runtime_guide(request: Request):
    text = Path(__file__).parent.parent.joinpath('static/runtime-guide.md').read_text()
    return text.replace('{{ROOM_URL}}', request.app.state.settings.public_url)


@router.get('/v1/setup/providers')
async def setup_providers(request: Request):
    return {'providers': providers.catalog(), 'outbound_integrations': request.app.state.settings.profile != 'sandbox'}


@router.get('/setup/{provider}', response_class=PlainTextResponse)
async def provider_guide(provider: str, request: Request):
    if provider not in {'grok', 'muse'}:
        raise APIError(404, 'not_found')
    settings = request.app.state.settings
    if settings.profile == 'sandbox':
        return 'This development sandbox does not connect external agent services. Use ' + settings.public_url + '/agent-guide for an isolated trial.\n'
    name = 'setup-grok-scheduled.md' if provider == 'grok' and request.query_params.get('delivery') == 'scheduled' else 'setup-' + provider + '.md'
    text = Path(__file__).parent.parent.joinpath('static', name).read_text()
    return text.replace('{{ROOM_URL}}', settings.public_url).replace('{{PRODUCT_NAME}}', settings.room_name)


@router.get('/healthz')
async def health():
    return {'ok': True}


@router.get('/readyz')
async def ready(conn=Depends(db.dependency)):
    try:
        if db.ready(conn):
            return {'ok': True}
    except Exception:
        pass
    raise APIError(503, 'unavailable')


@router.get('/v1/room')
async def room(request: Request, conn=Depends(db.dependency)):
    settings = request.app.state.settings
    return {'name': settings.room_name, 'requires_room_code': bool(settings.room_code),
            'commit_public_key': public_key(settings.signing_key), 'api_version': '1',
            'technology': 'AgentRoom', 'profile': settings.profile, 'url': settings.public_url,
            'rules_version': (room_rules.active(conn) or {}).get('version'),
            'sandbox': {'public_sessions': True, 'session_minutes': 15, 'retention_hours': 24,
                        'outbound_integrations': False, 'max_active_sessions': 10} if settings.profile == 'sandbox' else None}


@router.post('/v1/join', status_code=202)
async def join(data: Join, request: Request, background: BackgroundTasks, conn=Depends(db.dependency)):
    settings, state = request.app.state.settings, request.app.state
    state.limiter.hit('join', request.state.ip, 5 / 3600, 3)
    hashed = digest(canonical_json(data.model_dump(mode='json')))
    rid = str(data.request_id)
    created = False
    if settings.room_code and not hmac.compare_digest((data.room_code or '').encode(), settings.room_code.encode()):
        prior = db.join_request(conn, rid)
        # Let an existing applicant retry unchanged after a room code is enabled or rotated.
        if not prior or not hmac.compare_digest(prior['body_hash'], hashed):
            raise APIError(403, 'bad_room_code')
    with db.transaction(conn):
        db.expire_joins(conn)
        prior = db.join_request(conn, rid)
        if prior:
            if not hmac.compare_digest(prior['body_hash'], hashed):
                raise APIError(409, 'id_conflict')
        else:
            if settings.room_code and not hmac.compare_digest((data.room_code or '').encode(), settings.room_code.encode()):
                raise APIError(403, 'bad_room_code')
            if db.name_taken(conn, data.agent_name):
                raise APIError(409, 'name_taken')
            active, pending = db.join_counts(conn)
            if pending >= 50:
                raise APIError(503, 'queue_full')
            if active >= settings.max_members:
                raise APIError(403, 'room_full')
            db.create_join(conn, data, digest(data.poll_secret), hashed, request.state.ip)
            created = True
    if created:
        summary = {**data.model_dump(mode='json', exclude={'poll_secret', 'room_code'}), 'ip': request.state.ip}
        if settings.database_url:
            # ASGI awaits response background work; do not detach tasks that can be frozen on Vercel.
            background.add_task(notify_owner, settings, summary)
        else:
            state.spawn(notify_owner(settings, summary))
    return {'request_id': rid, 'status': 'pending', 'poll_after_s': 10}


@router.get('/v1/join/{request_id}')
async def poll(request_id: str, request: Request, conn=Depends(db.dependency)):
    state = request.app.state
    # Bound unknown-id buckets to the IP; known requests have their own poll limit.
    row = db.join_request(conn, request_id)
    state.limiter.hit('poll', request_id if row else request.state.ip, 30 / 60, 30)
    supplied = digest(request.headers.get('x-poll-secret', ''))
    with db.transaction(conn):
        db.expire_joins(conn)
        row = db.join_request(conn, request_id)
        if not hmac.compare_digest(supplied, row['poll_secret_hash'] if row else '0' * 64) or not row:
            raise APIError(404, 'not_found')
        if row['status'] == 'delivered':
            return {'status': 'approved', 'delivered': True}
        if row['status'] != 'approved':
            return {'status': row['status']}
        member = db.member(conn, row['member_id'])
        if not member or member['status'] != 'active':
            return {'status': 'denied'}
        pair = auth.new_tokens(conn, state.settings, member)
        db.delivered(conn, request_id)
        db.audit(conn, member['member_id'], 'token.delivered', request_id, request.state.ip)
    return {'status': 'approved', **pair}


@router.post('/v1/token/refresh')
async def refresh(data: Refresh, request: Request, conn=Depends(db.dependency)):
    import secrets
    state = request.app.state
    pair = None
    lookup, _ = db.refresh_lookup(conn, digest(data.refresh_token))
    state.limiter.hit('refresh', lookup['member_id'] if lookup else request.state.ip, 10 / 60, 10)
    with db.transaction(conn):
        session, reused = db.refresh_lookup(conn, digest(data.refresh_token))
        member = db.member(conn, session['member_id']) if session else None
        if session and not session['revoked_at'] and reused:
            exchange = db.refresh_exchange(conn, digest(data.refresh_token))
            if exchange and data.request_id and exchange['request_id'] == str(data.request_id) and db.parse(exchange['expires_at']) > db.utcnow():
                return auth.retry_pair(conn, state.settings, session, member, exchange['reply'], exchange['expires_at'])
            db.revoke_session(conn, session['session_id'])
            db.audit(conn, session['member_id'], 'token.refresh_reuse', session['session_id'], request.state.ip)
        elif session and not session['revoked_at'] and db.parse(session['expires_at']) > db.utcnow():
            if member and member['status'] == 'active':
                token = 'art_' + secrets.token_urlsafe(32)
                db.rotate_refresh(conn, session, digest(token))
                pair = auth.token_pair(conn, state.settings, member, session['session_id'], token)
                if data.request_id:
                    db.store_refresh_exchange(conn, digest(data.refresh_token), session['session_id'],
                                              str(data.request_id), auth.sealed_reply(state.settings, pair))
                db.audit(conn, member['member_id'], 'token.refreshed', session['session_id'], request.state.ip)
    # Raise outside transaction: theft-detection revocation MUST survive the 401.
    if pair is None:
        raise APIError(401, 'unauthenticated')
    return pair


@router.post('/v1/token/reconnect')
async def reconnect(data: Reconnect, request: Request, conn=Depends(db.dependency)):
    state = request.app.state
    state.limiter.hit('reconnect', request.state.ip, 5 / 60, 5)
    with db.transaction(conn):
        ticket = db.reconnect_ticket(conn, str(data.ticket_id))
        if not ticket or not hmac.compare_digest(digest(data.secret), ticket['secret_hash']) or db.parse(ticket['expires_at']) <= db.utcnow():
            raise APIError(401, 'unauthenticated')
        member = db.member(conn, ticket['member_id'])
        if not member or member['status'] != 'active':
            raise APIError(401, 'unauthenticated')
        if ticket['reply']:
            return auth.retry_pair(conn, state.settings, db.session(conn, ticket['session_id']), member,
                                   ticket['reply'], ticket['reply_expires_at'])
        pair = auth.new_tokens(conn, state.settings, member)
        claims = jwt.decode(pair['access_token'], options={'verify_signature': False})
        db.consume_reconnect(conn, str(data.ticket_id), claims['sid'], auth.sealed_reply(state.settings, pair))
        db.audit(conn, member['member_id'], 'token.reconnected', str(data.ticket_id), request.state.ip)
    return pair

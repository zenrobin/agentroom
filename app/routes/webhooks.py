import json
import secrets
from datetime import timedelta
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import Field, SecretStr

from .. import auth, db, providers, webhooks, outbound
from ..errors import APIError
from ..models import StrictModel

MessageType = Literal['chat', 'request', 'response', 'event', 'propose', 'counter', 'accept', 'decline', 'withdraw', 'commit', 'expire', 'system']


class Subscription(StrictModel):
    member_id: str | None = Field(default=None, pattern=r'^m_[a-f0-9]{32}$')
    url: str = Field(min_length=1, max_length=2048)
    types: list[MessageType] = Field(default_factory=lambda: ['chat', 'request', 'response', 'event', 'propose', 'counter', 'accept', 'decline', 'withdraw', 'commit', 'expire', 'system'], min_length=1, max_length=12)
    thread_id: UUID | None = None
    provider: Literal['agentroom', 'grok'] = 'agentroom'
    authorization: SecretStr | None = Field(default=None, max_length=2100)


class RoutineCredentials(StrictModel):
    url: str = Field(min_length=1, max_length=2048)
    authorization: SecretStr = Field(min_length=1, max_length=2100)


def routes(admin=False):
    router = APIRouter(prefix='/v1/owner/webhooks' if admin else '/v1/webhooks')
    dependency = auth.owner_auth if admin else auth.member_auth

    def owned(conn, sid, actor):
        row = db.one(conn, 'SELECT * FROM webhook_subscriptions WHERE subscription_id=? AND deleted_at IS NULL', (sid,))
        if not row or (not admin and row['member_id'] != actor['member_id']):
            raise APIError(404, 'not_found')
        if not admin and not webhooks.actor(conn, row['member_id']):
            raise APIError(403, 'forbidden')
        return row

    @router.get('')
    async def listing(actor=Depends(dependency), conn=Depends(db.dependency)):
        return webhooks.subscriptions(conn, None if admin else actor['member_id'])

    @router.post('', status_code=201)
    async def create(data: Subscription, request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
        mid = data.member_id if admin else actor['member_id']
        if not mid or (not admin and data.member_id and data.member_id != mid):
            raise APIError(403, 'forbidden')
        request.app.state.limiter.hit('webhook_manage', mid, 10 / 60, 10)
        with db.transaction(conn):
            result = webhooks.create(conn, request.app.state.settings, mid, data.url, data.types,
                                     str(data.thread_id) if data.thread_id else None,
                                     None if admin else actor['claims']['scope'], None if admin else actor['claims']['sid'],
                                     provider=data.provider, authorization=data.authorization.get_secret_value() if data.authorization is not None else None)
            db.audit(conn, 'owner' if admin else mid, 'webhook.created', result['subscription_id'], request.state.ip)
        return result

    @router.post('/{sid}/configure')
    async def configure(sid: str, data: RoutineCredentials, request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
        request.app.state.limiter.hit('webhook_credentials', sid, 1 / 60, 3)
        url = webhooks.endpoint(data.url)
        authorization = providers.grok_credentials(url, data.authorization.get_secret_value())
        with db.transaction(conn):
            sub = owned(conn, sid, actor)
            credential = db.one(conn, 'SELECT provider FROM webhook_provider_credentials WHERE subscription_id=?', (sid,))
            if not credential or credential['provider'] != 'grok':
                raise APIError(409, 'provider_mismatch')
            if not webhooks.subscription_actor(conn, sub):
                raise APIError(403, 'forbidden', 'This connection expired or was revoked. Create a new connection.')
            encrypted = providers.cipher(request.app.state.settings).encrypt(authorization.encode()).decode()
            conn.execute('UPDATE webhook_provider_credentials SET credential=? WHERE subscription_id=?', (encrypted, sid))
            conn.execute('UPDATE webhook_subscriptions SET url=?,generation=generation+1 WHERE subscription_id=?', (url, sid))
            conn.execute('UPDATE connector_watchdogs SET server_confirmed_at=NULL WHERE member_id=?',(sub['member_id'],))
            conn.execute("UPDATE webhook_deliveries SET status='cancelled',completed_at=?,last_error='configuration_changed' WHERE subscription_id=? AND status='pending' AND lease_id IS NULL", (db.stamp(), sid))
            db.audit(conn, 'owner' if admin else actor['member_id'], 'webhook.provider_updated', sid, request.state.ip)
        return {'subscription_id': sid, 'provider': 'grok', 'status': 'configured'}

    @router.get('/{sid}/deliveries')
    async def history(sid: str, actor=Depends(dependency), conn=Depends(db.dependency)):
        owned(conn, sid, actor)
        deliveries = db.rows(conn, '''SELECT delivery_id,message_seq,status,attempts,cycle,next_at,last_status,last_error,created_at,completed_at
            FROM webhook_deliveries WHERE subscription_id=? ORDER BY created_at DESC LIMIT 50''', (sid,))
        for row in deliveries:
            row['history'] = db.rows(conn, 'SELECT attempt,at,status_code,outcome FROM webhook_attempts WHERE delivery_id=? ORDER BY at DESC LIMIT 20', (row['delivery_id'],))
        return deliveries

    if admin:
        @router.post('/{sid}/renew')
        async def renew(sid: str, request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
            with db.transaction(conn):
                sub = owned(conn, sid, actor)
                # Recovery/revocation pauses old grants. Renewal cannot revive them or widen scopes.
                if not sub['enabled'] or sub['session_id'] or not webhooks.actor(conn, sub['member_id'], sub['scopes']):
                    raise APIError(409, 'fresh_connection_required', 'Paused, revoked, or agent-managed grants require a fresh connection.')
                expires = db.stamp(db.utcnow() + timedelta(days=30))
                conn.execute('UPDATE webhook_subscriptions SET expires_at=? WHERE subscription_id=?', (expires, sid))
                db.audit(conn, 'owner', 'webhook.renewed', sid, request.state.ip)
            return {'subscription_id': sid, 'expires_at': expires}

    @router.post('/{sid}/test', status_code=202)
    async def test(sid: str, request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
        request.app.state.limiter.hit('webhook_test', sid, 1 / 30, 1)
        with db.transaction(conn):
            sub = owned(conn, sid, actor)
            if not sub['enabled']:
                raise APIError(409, 'webhook_paused')
            if not webhooks.subscription_actor(conn, sub):
                raise APIError(403, 'forbidden')
            did = webhooks.enqueue(conn, sub, {'type': 'webhook.test', 'member_id': sub['member_id'], 'occurred_at': db.stamp()})
        return {'delivery_id': did, 'status': 'pending'}

    @router.post('/{sid}/rotate')
    async def rotate(sid: str, request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
        request.app.state.limiter.hit('webhook_rotate', sid, 1 / 300, 1)
        secret = 'arwh_' + secrets.token_urlsafe(32)
        with db.transaction(conn):
            sub = owned(conn, sid, actor)
            if outbound.is_outbound(conn,sid):
                raise APIError(409,'no_webhook_secret','Outbound HTTPS uses existing room authentication.')
            if db.one(conn, 'SELECT provider FROM webhook_provider_credentials WHERE subscription_id=?', (sid,)):
                raise APIError(409, 'provider_managed_key', 'Grok manages this key. Update the routine key through Connect an agent.')
            encrypted = webhooks.cipher(request.app.state.settings).encrypt(secret.encode()).decode()
            conn.execute('UPDATE webhook_subscriptions SET previous_secret=secret,previous_until=?,secret=? WHERE subscription_id=?',
                         (db.stamp(db.utcnow() + timedelta(minutes=5)), encrypted, sid))
            db.audit(conn, 'owner' if admin else actor['member_id'], 'webhook.secret_rotated', sid, request.state.ip)
        return {'subscription_id': sid, 'secret': secret, 'previous_signature_expires_in': 300}

    @router.post('/{sid}/replay/{did}', status_code=202)
    async def replay(sid: str, did: str, request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
        request.app.state.limiter.hit('webhook_replay', sid, 1 / 10, 1)
        with db.transaction(conn):
            sub = owned(conn, sid, actor)
            row = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=? AND subscription_id=?', (did, sid))
            if not row:
                raise APIError(404, 'not_found')
            if row['status'] not in {'failed', 'cancelled'}:
                raise APIError(409, 'replay_not_available')
            row['generation'] = sub['generation']
            if not webhooks.valid(conn, row):
                raise APIError(403, 'forbidden')
            conn.execute("""UPDATE webhook_deliveries SET status='pending',attempts=0,cycle=cycle+1,generation=?,
                next_at=?,lease_id=NULL,lease_until=NULL,completed_at=NULL,enqueued_attempt=-1,enqueued_at=NULL WHERE delivery_id=?""",
                (sub['generation'], db.stamp(), did))
            db.audit(conn, 'owner' if admin else actor['member_id'], 'webhook.replayed', did, request.state.ip)
        return {'delivery_id': did, 'status': 'pending'}

    @router.post('/{sid}/{action}')
    async def manage(sid: str, action: Literal['pause', 'resume', 'delete'], request: Request, actor=Depends(dependency), conn=Depends(db.dependency)):
        with db.transaction(conn):
            sub = owned(conn, sid, actor)
            if action == 'resume' and not webhooks.subscription_actor(conn, sub):
                raise APIError(403, 'forbidden', 'This grant expired or was revoked. Create a new endpoint grant.')
            conn.execute('UPDATE webhook_subscriptions SET enabled=?,generation=generation+1,deleted_at=? WHERE subscription_id=?',
                         (int(action == 'resume'), db.stamp() if action == 'delete' else None, sid))
            if db.one(conn,'SELECT 1 FROM webhook_provider_credentials WHERE subscription_id=?',(sid,)):
                conn.execute('UPDATE connector_watchdogs SET server_confirmed_at=NULL WHERE member_id=?',(sub['member_id'],))
            outbound.signal(conn,sub['member_id'])
            if action == 'delete':
                conn.execute('DELETE FROM outbound_connectors WHERE subscription_id=?', (sid,))
                conn.execute('DELETE FROM webhook_provider_credentials WHERE subscription_id=?', (sid,))
            conn.execute("UPDATE webhook_deliveries SET status='cancelled',completed_at=?,last_error='subscription_changed' WHERE subscription_id=? AND status='pending' AND lease_id IS NULL", (db.stamp(), sid))
            db.audit(conn, 'owner' if admin else actor['member_id'], 'webhook.' + action, sid, request.state.ip)
        return {'status': action}

    return router


router, owner_router = routes(), routes(admin=True)

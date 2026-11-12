from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import Field

from .. import auth, connectors, db, watchdog, webhooks, outbound
from ..errors import APIError
from ..models import StrictModel

router = APIRouter(prefix='/v1/connector')


class RuntimeReady(StrictModel):
    provider: Literal['grok', 'muse']
    recovery_interval_s: int = Field(ge=300, le=1800, strict=True)
    recovery_verified_at: datetime


class Receipt(StrictModel):
    seq: int = Field(gt=0, le=2**63-1, strict=True)
    message_id: UUID
    outcome: Literal['handled', 'awaiting_approval'] = 'handled'


class ReceiptBatch(StrictModel):
    receipts: list[Receipt] = Field(min_length=1, max_length=100)


class Activity(StrictModel):
    run_id: UUID
    active: bool = Field(strict=True)
    lease_seconds: int = Field(default=900,ge=60,le=900,strict=True)


@router.post('/activity')
async def activity(data: Activity, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    auth.require_read_scope(conn, actor)
    request.app.state.limiter.hit('connector_activity',actor['member_id'],1/10,6)
    with db.transaction(conn):
        actor=auth.verify_access(conn,request.app.state.settings,auth.bearer(request))
        auth.require_read_scope(conn,actor)
        state=db.one(conn,'SELECT * FROM connector_watchdogs WHERE member_id=?',(actor['member_id'],))
        if not state:
            raise APIError(409,'watchdog_not_active','Configure a managed connector or receive a tracked message first.')
        if data.active:
            if state['busy_until'] and state['busy_until']>db.stamp() and state['busy_run_id']!=str(data.run_id):
                raise APIError(409,'processing_in_progress')
            conn.execute('UPDATE connector_watchdogs SET busy_run_id=?,busy_until=? WHERE member_id=?',
                         (str(data.run_id),db.stamp(db.utcnow()+timedelta(seconds=data.lease_seconds)),actor['member_id']))
        else:
            conn.execute('UPDATE connector_watchdogs SET busy_run_id=NULL,busy_until=NULL WHERE member_id=? AND busy_run_id=?',
                         (actor['member_id'],str(data.run_id)))
    return {'status':'processing' if data.active else 'released','run_id':str(data.run_id)}


@router.post('/server-recovery')
async def server_recovery(request: Request,actor=Depends(auth.member_auth),conn=Depends(db.dependency)):
    auth.require_read_scope(conn,actor)
    with db.transaction(conn):
        actor=auth.verify_access(conn,request.app.state.settings,auth.bearer(request))
        auth.require_read_scope(conn,actor)
        status=watchdog.status(conn,actor['member_id'])
        proofs=db.rows(conn,'''SELECT d.* FROM connector_work w JOIN processing_receipts p
            ON p.member_id=w.member_id AND p.message_seq=w.message_seq
            JOIN webhook_deliveries d ON d.delivery_id=w.delivery_id
            WHERE w.member_id=? AND p.reported_at>?
            AND (NOT EXISTS(SELECT 1 FROM outbound_connectors o WHERE o.subscription_id=d.subscription_id)
                 OR (d.completed_at IS NOT NULL AND d.completed_at<=p.reported_at AND d.status='delivered'))''',
            (actor['member_id'],db.stamp(db.utcnow()-timedelta(minutes=30))))
        verified=next((sub for sub in (webhooks.valid(conn,row) for row in proofs)
                       if sub and sub.get('provider') in {'grok','muse_outbound'} and watchdog.covers_inbox(conn,sub)
                       and outbound.runtime_verified(conn,sub['subscription_id'])),None)
        if not verified or not status['server_healthy'] or not status['full_inbox_coverage'] or status['needs_attention']:
            raise APIError(409,'recovery_not_verified','Keep scheduled recovery until a tracked message is handled, full coverage is active and the server check is healthy.')
        conn.execute('''UPDATE connector_watchdogs SET server_confirmed_at=?,confirmed_subscription_id=?,confirmed_generation=?
            WHERE member_id=?''',(db.stamp(),verified['subscription_id'],verified['generation'],actor['member_id']))
        db.audit(conn,actor['member_id'],'connector.server_recovery_confirmed')
    return {'status':'server_recovery_verified','recovery':watchdog.status(conn,actor['member_id'])}


@router.get('/status')
async def status(actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    auth.require_read_scope(conn, actor)
    return connectors.status(conn, actor['member_id'])


@router.post('/ready')
async def ready(data: RuntimeReady, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    auth.require_read_scope(conn, actor)
    request.app.state.limiter.hit('connector_ready', actor['member_id'], 1 / 60, 3)
    if data.recovery_verified_at.tzinfo is None or not 0 <= (db.utcnow() - data.recovery_verified_at).total_seconds() <= 86400:
        raise APIError(422, 'verification_time_invalid', 'Report an actual recovery run from the last 24 hours.')
    with db.transaction(conn):
        actor = auth.verify_access(conn, request.app.state.settings, auth.bearer(request))
        auth.require_read_scope(conn, actor)
        if data.provider == 'muse':
            sub=db.one(conn,'SELECT subscription_id FROM outbound_connectors WHERE member_id=?',(actor['member_id'],))
            if not sub:
                raise APIError(409,'outbound_not_configured')
            outbound.authorized(conn,request.app.state.settings,auth.bearer(request),sub['subscription_id'])
            conn.execute('UPDATE outbound_connectors SET recovery_interval_s=?,runtime_verified_at=? WHERE subscription_id=?',
                         (data.recovery_interval_s,db.stamp(data.recovery_verified_at),sub['subscription_id']))
            return {'status':'agent_reported_ready','member_id':actor['member_id']}
        conn.execute('''INSERT INTO connector_runtime VALUES(?,?,?,?,?) ON CONFLICT(member_id) DO UPDATE SET
            provider=excluded.provider,recovery_interval_s=excluded.recovery_interval_s,
            verified_at=excluded.verified_at,reported_at=excluded.reported_at''',
            (actor['member_id'], data.provider, data.recovery_interval_s, db.stamp(data.recovery_verified_at), db.stamp()))
    return {'status': 'agent_reported_ready', 'member_id': actor['member_id']}


@router.post('/receipts')
async def receipts(data: ReceiptBatch, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    auth.require_read_scope(conn, actor)
    request.app.state.limiter.hit('processing_receipts', actor['member_id'], 2, 20)
    accepted, expired = [], []
    with db.transaction(conn):
        # Recheck current access under the write transaction, including session revocation.
        actor = auth.verify_access(conn, request.app.state.settings, auth.bearer(request))
        auth.require_read_scope(conn, actor)
        for receipt in data.receipts:
            message = db.visible_message(conn, actor, receipt.seq)
            if message is None:
                # Never disclose whether this is another member's private message or pruned history.
                expired.append(receipt.seq)
                continue
            if message['id'] != str(receipt.message_id) or receipt.seq > db.cursor(conn, actor['member_id']):
                raise APIError(409, 'receipt_mismatch', 'Save and acknowledge this exact message before reporting handling.')
            conn.execute('''INSERT INTO processing_receipts VALUES(?,?,?,?,?) ON CONFLICT(member_id,message_seq)
                DO UPDATE SET outcome=excluded.outcome,reported_at=excluded.reported_at
                WHERE processing_receipts.outcome='awaiting_approval' AND excluded.outcome='handled' ''',
                (actor['member_id'], receipt.seq, str(receipt.message_id), receipt.outcome, db.stamp()))
            accepted.append(receipt.seq)
        watchdog.settle(conn,actor['member_id'])
    return {'accepted': accepted, 'unavailable': expired}

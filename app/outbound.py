"""Durable wake hints over authenticated outbound HTTPS; never message contents.

Postgres notifications only interrupt the wait. Durable rows and current grants
decide what may be sent. NOTIFY runs in the message/recovery transaction, so a
rollback cannot publish a wake. SQLite is a local-test fallback, not production.
"""
import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

from . import auth, db, watchdog, webhooks
from .errors import APIError

HEARTBEAT = 25
STREAM_SECONDS = 240


def channel(mid):
    return 'ar_wake_' + hashlib.sha256(mid.encode()).hexdigest()[:40]


def signal(conn, mid):
    if getattr(conn, 'postgres', False):
        conn.execute('SELECT pg_notify(?, ?)', (channel(mid), 'wake'))


def is_outbound(conn, sid):
    return db.one(conn, 'SELECT * FROM outbound_connectors WHERE subscription_id=?', (sid,))


def managed(conn, sid):
    return bool(is_outbound(conn, sid) or db.one(conn,
        "SELECT 1 FROM webhook_provider_credentials WHERE subscription_id=? AND provider='grok'", (sid,)))


def runtime_verified(conn, sid):
    state=is_outbound(conn,sid)
    return not state or bool(state['runtime_verified_at'] and state['last_contact_at'] and
        state['last_contact_at']>db.stamp(db.utcnow()-timedelta(seconds=90)))


def register(conn, actor, device_id):
    if db.one(conn,"SELECT 1 FROM operations_leases WHERE name='outbound_disabled'"):
        raise APIError(503,'outbound_disabled')
    mid = actor['member_id']
    existing = db.one(conn, '''SELECT s.*,o.device_id FROM webhook_subscriptions s JOIN outbound_connectors o
        ON o.subscription_id=s.subscription_id WHERE o.member_id=?''', (mid,))
    if existing:
        # A reconnect never revives a paused/revoked grant or changes its device.
        if (existing['device_id'] != device_id or not existing['enabled'] or existing['deleted_at']
                or existing['session_id'] != actor['claims']['sid'] or not webhooks.subscription_actor(conn, existing)):
            raise APIError(409, 'outbound_connection_exists', 'Preserve the existing device configuration. Delete a retired connection explicitly before replacing it.')
        return {'subscription_id': existing['subscription_id'], 'transport': 'outbound_https'}
    sid = str(uuid4())
    session = db.session(conn, actor['claims']['sid'])
    # Empty URL/secret is intentional: this transport never sends HTTP callbacks.
    conn.execute('''INSERT INTO webhook_subscriptions(subscription_id,member_id,url,secret,types,created_at,scopes,session_id,expires_at)
        VALUES(?,?,'','',?,?,?,?,?)''', (sid,mid,json.dumps(sorted(watchdog.TYPES)),db.stamp(),actor['claims']['scope'],
                                       actor['claims']['sid'],session['expires_at']))
    conn.execute('INSERT INTO outbound_connectors(subscription_id,member_id,device_id) VALUES(?,?,?)', (sid,mid,device_id))
    conn.execute('''INSERT INTO connector_watchdogs(member_id,created_at) VALUES(?,?)
        ON CONFLICT(member_id) DO UPDATE SET server_confirmed_at=NULL''', (mid,db.stamp()))
    db.audit(conn,mid,'connector.outbound_registered',sid)
    return {'subscription_id': sid, 'transport': 'outbound_https'}


def authorized(conn, settings, token, sid, stream_id=None):
    actor = auth.verify_access(conn, settings, token)
    auth.require_read_scope(conn, actor)
    if db.one(conn,"SELECT 1 FROM operations_leases WHERE name='outbound_disabled'"):
        raise APIError(503,'outbound_disabled')
    sub = db.one(conn, '''SELECT s.*,o.stream_id,o.stream_until FROM webhook_subscriptions s JOIN outbound_connectors o
        ON o.subscription_id=s.subscription_id WHERE s.subscription_id=? AND s.member_id=?''', (sid,actor['member_id']))
    if not sub or sub['deleted_at']:
        raise APIError(404, 'not_found')
    if not sub['enabled'] or not webhooks.subscription_actor(conn, sub):
        raise APIError(403, 'connector_inactive')
    if stream_id and (sub['stream_id'] != stream_id or not sub['stream_until'] or sub['stream_until']<=db.stamp()):
        raise APIError(409, 'stream_replaced')
    return actor, sub


def claim(conn, settings, token, sid):
    # A lease outlives the bounded stream and its final heartbeat. A terminated
    # function can temporarily occupy a slot but cannot create unlimited listeners.
    with db.transaction(conn,('outbound_streams','room')):
        actor,sub=authorized(conn,settings,token,sid)
        now=db.stamp()
        conn.execute('DELETE FROM outbound_streams WHERE expires_at<=?',(now,))
        count=conn.execute('SELECT count(*) FROM outbound_streams').fetchone()[0]
        limit=min(settings.max_members,10 if settings.profile=='sandbox' else 20)
        if db.one(conn,'SELECT 1 FROM outbound_streams WHERE member_id=?',(actor['member_id'],)) or count>=limit:
            raise APIError(429,'connector_capacity',headers={'Retry-After':'20'})
        stream_id=str(uuid4())
        until=db.stamp(db.utcnow()+timedelta(seconds=300))
        conn.execute('INSERT INTO outbound_streams VALUES(?,?,?)',(stream_id,actor['member_id'],until))
        conn.execute('UPDATE outbound_connectors SET stream_id=?,stream_until=? WHERE subscription_id=?',
                     (stream_id,until,sid))
        return actor,stream_id


def pending(conn, actor, sid):
    events = []
    rows = db.rows(conn, '''SELECT * FROM webhook_deliveries WHERE subscription_id=? AND status='pending'
        AND next_at<=? ORDER BY created_at,delivery_id LIMIT 100''', (sid,db.stamp()))
    for row in rows:
        if not webhooks.valid(conn, row):
            conn.execute("UPDATE webhook_deliveries SET status='cancelled',last_error='authorization_changed',completed_at=? WHERE delivery_id=? AND status='pending' AND cycle=?",
                         (db.stamp(),row['delivery_id'],row['cycle']))
            continue
        if row['message_seq'] is not None and not db.visible_message(conn, actor, row['message_seq']):
            continue
        gate = watchdog.before_delivery(conn, row)
        if gate == 'handling_resolved':
            conn.execute("UPDATE webhook_deliveries SET status='cancelled',last_error='handling_resolved',completed_at=? WHERE delivery_id=? AND status='pending' AND cycle=?",
                         (db.stamp(),row['delivery_id'],row['cycle']))
            continue
        if gate:
            continue
        payload = json.loads(row['payload'])
        events.append({'event_id':payload['event_id'], 'delivery_id':row['delivery_id'], 'cycle':row['cycle'],
                       'member_id':actor['member_id'], 'seq':row['message_seq'], 'type':payload['type']})
    return events


def received(conn, actor, sid, delivery_id, event_id, cycle):
    row = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=? AND subscription_id=?', (delivery_id,sid))
    if (not row or row['cycle'] != cycle or json.loads(row['payload'])['event_id'] != event_id
            or not webhooks.valid(conn,row)
            or (row['message_seq'] is not None and not db.visible_message(conn,actor,row['message_seq']))):
        raise APIError(409, 'wake_changed')
    if row['status'] not in {'pending','delivered'}:
        raise APIError(409, 'wake_changed')
    gate=watchdog.before_delivery(conn,row)
    if gate=='handling_resolved':
        raise APIError(409,'wake_changed')
    if gate:
        raise APIError(425,'processing_in_progress',headers={'Retry-After':'25'})
    conn.execute("UPDATE webhook_deliveries SET status='delivered',completed_at=?,last_error=NULL WHERE delivery_id=? AND status='pending'",
                 (db.stamp(),delivery_id))
    return {'status':'wake_saved', 'meaning':'Notification saved; message handling is still unconfirmed.'}


@asynccontextmanager
async def listener(settings, mid):
    if not settings.database_url:
        async def wait():
            await asyncio.sleep(min(HEARTBEAT, 1))
        yield wait
        return
    if not settings.database_url_unpooled:
        raise APIError(503,'outbound_unavailable','A direct database listener must be configured by the operator.')
    import certifi
    import psycopg
    from psycopg import sql
    connection = await psycopg.AsyncConnection.connect(settings.database_url_unpooled, autocommit=True,
        connect_timeout=10, sslmode='verify-full', sslrootcert=certifi.where())
    try:
        await connection.execute(sql.SQL('LISTEN {}').format(sql.Identifier(channel(mid))))
        async def wait():
            generator = connection.notifies(timeout=HEARTBEAT,stop_after=1)
            try:
                async for _ in generator:
                    return True
                return False
            finally:
                await generator.aclose()
        yield wait
    finally:
        await connection.close()

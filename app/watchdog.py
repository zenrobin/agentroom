"""Receipt-aware recovery. Only outstanding work can cause a bounded AI wake.

The queue carries identifiers; database state, current access and exact processing
receipts are checked again before a retry. Network I/O never holds a DB lock.
"""
import json
import math
from datetime import datetime, timedelta
from uuid import uuid4

from . import db, webhooks, outbound

TOPIC = 'agent-room-recovery'
GRACE = 300
RETRY_DELAYS = (300, 900, 1800)
MAX_RETRIES = len(RETRY_DELAYS)
TYPES = {'chat','request','response','event','propose','counter','accept','decline','withdraw','commit','expire','system'}


def track(conn, sub, message, delivery_id, *, probe=False):
    if not probe and not outbound.managed(conn,sub['subscription_id']):
        return
    now, due = db.stamp(), db.stamp(db.utcnow() + timedelta(seconds=GRACE))
    conn.execute('''INSERT INTO connector_work(member_id,message_seq,delivery_id,next_at,created_at)
        VALUES(?,?,?,?,?) ON CONFLICT(member_id,message_seq) DO NOTHING''', (sub['member_id'], message['seq'], delivery_id, due, now))
    conn.execute('''INSERT INTO connector_watchdogs(member_id,next_at,created_at) VALUES(?,?,?)
        ON CONFLICT(member_id) DO UPDATE SET next_at=excluded.next_at,revision=connector_watchdogs.revision+1
        WHERE connector_watchdogs.next_at IS NULL OR connector_watchdogs.next_at>excluded.next_at''', (sub['member_id'], due, now))


def outstanding(conn, mid, *, through=None):
    sql = '''SELECT w.*,d.subscription_id,d.generation,d.cycle,d.status AS delivery_status,d.completed_at,d.payload
        FROM connector_work w JOIN webhook_deliveries d ON d.delivery_id=w.delivery_id
        LEFT JOIN processing_receipts p ON p.member_id=w.member_id AND p.message_seq=w.message_seq
        WHERE w.member_id=? AND w.status IN ('waiting','attention') AND p.message_seq IS NULL'''
    return db.rows(conn, sql + (' AND w.message_seq<=?' if through is not None else '') + ' ORDER BY w.message_seq',
                   (mid,through) if through is not None else (mid,))


def settle(conn, mid):
    conn.execute('''UPDATE connector_work SET status='resolved' WHERE member_id=? AND status IN ('waiting','attention')
        AND EXISTS(SELECT 1 FROM processing_receipts p WHERE p.member_id=connector_work.member_id AND p.message_seq=connector_work.message_seq)''', (mid,))


def before_delivery(conn, row):
    """Recheck tracked handling before both transport retries and recovery wakes."""
    wake = db.one(conn, 'SELECT * FROM connector_retry_wakes WHERE delivery_id=? AND cycle=?', (row['delivery_id'],row['cycle']))
    if not wake:
        tracked = db.one(conn, '''SELECT w.member_id,w.message_seq FROM connector_work w
            JOIN webhook_subscriptions s ON s.member_id=w.member_id
            WHERE s.subscription_id=? AND w.message_seq=?
            AND (EXISTS(SELECT 1 FROM webhook_provider_credentials p WHERE p.subscription_id=s.subscription_id AND p.provider='grok')
                 OR EXISTS(SELECT 1 FROM outbound_connectors o WHERE o.subscription_id=s.subscription_id))''', (row['subscription_id'],row['message_seq']))
        if not tracked:
            return None
        if db.one(conn,'SELECT 1 FROM processing_receipts WHERE member_id=? AND message_seq=?',
                  (tracked['member_id'],tracked['message_seq'])):
            return 'handling_resolved'
        return None
    items = outstanding(conn, wake['member_id'], through=wake['through_seq'])
    if not any(webhooks.valid(conn, item) for item in items):
        return 'handling_resolved'
    state = db.one(conn, 'SELECT busy_until FROM connector_watchdogs WHERE member_id=?', (wake['member_id'],))
    return state['busy_until'] if state and state['busy_until'] and state['busy_until']>db.stamp() else None


def process(conn, mid):
    """One atomic member check. Duplicate/early callbacks cannot consume retry budget."""
    now = db.stamp()
    # Serialize with message insertion, handling receipts and revocation. This
    # short transaction performs no network I/O; rate/auth counters stay separate.
    with db.transaction(conn):
        state = db.one(conn, 'SELECT * FROM connector_watchdogs WHERE member_id=?', (mid,))
        if not state or not state['next_at'] or state['next_at'] > now:
            return
        settle(conn, mid)
        due, future = [], []
        for item in outstanding(conn, mid):
            sub = webhooks.valid(conn, item)
            if not sub:
                conn.execute("UPDATE connector_work SET status='cancelled' WHERE member_id=? AND message_seq=?", (mid,item['message_seq']))
                continue
            if item['status'] == 'attention':
                continue
            if item['delivery_status'] in {'failed','cancelled'}:
                conn.execute("UPDATE connector_work SET status='attention' WHERE member_id=? AND message_seq=?", (mid,item['message_seq']))
                db.audit(conn,'room','connector.needs_attention',mid,detail='transport_failed')
                continue
            check_at = item['next_at']
            if item['delivery_status'] == 'pending':
                check_at = max(check_at, db.stamp(db.utcnow()+timedelta(seconds=GRACE)))
            elif item['completed_at']:
                check_at = max(check_at, db.stamp(db.parse(item['completed_at'])+timedelta(seconds=GRACE)))
            if state['busy_until']:
                check_at = max(check_at, state['busy_until'])
            if state['last_retry_at']:
                check_at = max(check_at, db.stamp(db.parse(state['last_retry_at'])+timedelta(seconds=GRACE)))
            if check_at > now:
                future.append(check_at)
            elif item['retries'] >= MAX_RETRIES:
                conn.execute("UPDATE connector_work SET status='attention' WHERE member_id=? AND message_seq=?", (mid,item['message_seq']))
                db.audit(conn,'room','connector.needs_attention',mid,detail='handling_unconfirmed')
            else:
                due.append(item)
        if due:
            # One fresh wake per member for this batch. Retrying a notification must
            # use a fresh event ID so a durable receiver does not discard the hint.
            item = due[-1]
            payload = json.loads(item['payload']); payload['event_id'] = str(uuid4())
            cycle = item['cycle'] + 1
            conn.execute('''UPDATE webhook_deliveries SET status='pending',attempts=0,cycle=?,payload=?,
                next_at=?,completed_at=NULL,last_status=NULL,last_error=NULL,lease_id=NULL,lease_until=NULL,
                enqueued_attempt=-1,enqueued_at=NULL WHERE delivery_id=?''',
                (cycle,json.dumps(payload,separators=(',',':')),now,item['delivery_id']))
            conn.execute('INSERT INTO connector_retry_wakes VALUES(?,?,?,?)', (item['delivery_id'],cycle,mid,item['message_seq']))
            for work in due:
                next_at = db.stamp(db.utcnow()+timedelta(seconds=RETRY_DELAYS[work['retries']]))
                conn.execute('UPDATE connector_work SET retries=retries+1,next_at=? WHERE member_id=? AND message_seq=?', (next_at,mid,work['message_seq']))
                future.append(next_at)
            conn.execute('UPDATE connector_watchdogs SET last_retry_at=? WHERE member_id=?', (now,mid))
            db.audit(conn,'room','connector.recovery_wake',mid,detail=str(len(due)))
            outbound.signal(conn,mid)
        conn.execute('UPDATE connector_watchdogs SET next_at=?,revision=revision+1 WHERE member_id=?', (min(future) if future else None,mid))


async def dispatch(settings, state, *, rescue=False):
    from vercel.queue import QueueClient
    delay = max(0,math.ceil((db.parse(state['next_at'])-db.utcnow()).total_seconds()))
    # Rescue keys change each sweep, recovering an exhausted/lost queue callback.
    suffix = ':rescue:'+str(int(db.utcnow().timestamp())//GRACE) if rescue else ''
    await QueueClient(region='iad1',timeout=5).send(TOPIC, {'member_id':state['member_id']},
        idempotency_key=f"watch:{state['member_id']}:{state['revision']}"+suffix,
        delay=delay,retention=timedelta(days=7))
    conn=db.connect(db.target(settings))
    try:
        conn.execute('UPDATE connector_watchdogs SET enqueued_revision=?,enqueued_at=? WHERE member_id=? AND revision=?',
                     (state['revision'],db.stamp(),state['member_id'],state['revision']))
    finally:
        conn.close()


async def ensure_sweep(settings):
    """One cheap recurring server check; no AI session and no new hosting plan."""
    if not settings.serverless:
        return
    conn=db.connect(db.target(settings))
    try:
        saved=db.one(conn,"SELECT updated_at FROM operations_leases WHERE name='watchdog_sweep_enqueued'")
        if saved and saved['updated_at']>db.stamp():
            return
        slot=int(db.utcnow().timestamp())//GRACE+1
        due=datetime.fromtimestamp(slot*GRACE,tz=db.utcnow().tzinfo)
        from vercel.queue import QueueClient
        await QueueClient(region='iad1',timeout=5).send(TOPIC,{'sweep':True},
            idempotency_key='watchdog-sweep:'+str(slot),delay=max(0,math.ceil((due-db.utcnow()).total_seconds())),retention=timedelta(days=7))
        conn.execute("""INSERT INTO operations_leases VALUES('watchdog_sweep_enqueued',?)
            ON CONFLICT(name) DO UPDATE SET updated_at=excluded.updated_at
            WHERE operations_leases.updated_at<excluded.updated_at""",(db.stamp(due),))
    finally:
        conn.close()


async def kick(settings, *, strict=False, rescue=False):
    try:
        await ensure_sweep(settings)
        conn=db.connect(db.target(settings))
        try:
            states=db.rows(conn,'''SELECT * FROM connector_watchdogs WHERE next_at IS NOT NULL AND
                (enqueued_revision<revision''' + (' OR next_at<?' if rescue else '') + ') ORDER BY next_at LIMIT 30',
                (db.stamp(db.utcnow()-timedelta(seconds=60)),) if rescue else ())
        finally:
            conn.close()
        for state in states:
            if settings.serverless:
                await dispatch(settings,state,rescue=rescue)
            elif state['next_at']<=db.stamp():
                conn=db.connect(db.target(settings))
                try: process(conn,state['member_id'])
                finally: conn.close()
    except Exception:
        if strict: raise
        webhooks.log.warning('connector_watchdog_unavailable')


async def consume(settings, payload):
    # Schedule the next heartbeat even if a particular member's work fails.
    await ensure_sweep(settings)
    if payload.get('sweep'):
        await kick(settings,strict=True,rescue=True)
        if settings.profile!='sandbox':
            await webhooks.kick(settings,force=True,strict=True)
        conn=db.connect(db.target(settings))
        try:
            conn.execute("INSERT INTO operations_leases VALUES('watchdog_sweep_observed',?) ON CONFLICT(name) DO UPDATE SET updated_at=excluded.updated_at",(db.stamp(),))
        finally: conn.close()
    else:
        conn=db.connect(db.target(settings))
        try:
            process(conn,payload['member_id'])
            pending=db.rows(conn,'''SELECT d.* FROM webhook_deliveries d JOIN connector_retry_wakes w
                ON w.delivery_id=d.delivery_id AND w.cycle=d.cycle WHERE w.member_id=? AND d.status='pending' ''',(payload['member_id'],))
            state=db.one(conn,'SELECT * FROM connector_watchdogs WHERE member_id=?',(payload['member_id'],))
        finally: conn.close()
        for row in pending: await webhooks.dispatch(settings,row)
        if state and state['next_at']: await dispatch(settings,state)


def covers_inbox(conn, sub):
    member=db.member(conn,sub['member_id'])
    scopes=set((db.role_scopes(conn,member['role']) or '').split()) if member else set()
    return bool(not sub['thread_id'] and set(json.loads(sub['types']))==TYPES
                and scopes.issubset(set(sub['scopes'].split())) and webhooks.subscription_actor(conn,sub))


def status(conn, mid):
    state=db.one(conn,'SELECT * FROM connector_watchdogs WHERE member_id=?',(mid,))
    items=outstanding(conn,mid)
    observed=db.one(conn,"SELECT updated_at FROM operations_leases WHERE name='watchdog_sweep_observed'")
    healthy=bool(observed and observed['updated_at']>db.stamp(db.utcnow()-timedelta(minutes=10)))
    subs=db.rows(conn,"""SELECT s.* FROM webhook_subscriptions s
        WHERE s.member_id=? AND s.enabled=1 AND s.deleted_at IS NULL
        AND (EXISTS(SELECT 1 FROM webhook_provider_credentials p WHERE p.subscription_id=s.subscription_id AND p.provider='grok')
             OR EXISTS(SELECT 1 FROM outbound_connectors o WHERE o.subscription_id=s.subscription_id)) """,(mid,))
    full=any(covers_inbox(conn,s) for s in subs)
    confirmed=bool(state and state['server_confirmed_at'] and any(
        s['subscription_id']==state['confirmed_subscription_id'] and s['generation']==state['confirmed_generation']
        and covers_inbox(conn,s) and outbound.runtime_verified(conn,s['subscription_id']) for s in subs))
    attention=sum(w['status']=='attention' for w in items)
    return {'available':True,'scope':'new Grok and outbound connector message notifications','server_healthy':healthy,
        'last_server_check':observed['updated_at'] if observed else None,'full_inbox_coverage':full,
        'pending':len(items)-attention,'needs_attention':attention,'max_retries':MAX_RETRIES,
        'next_check_at':state['next_at'] if state else None,'last_retry_at':state['last_retry_at'] if state else None,
        'busy_until':state['busy_until'] if state else None,
        'can_disable_scheduled_recovery':bool(confirmed and healthy and full and not attention),
        'server_confirmed_at':state['server_confirmed_at'] if state else None}

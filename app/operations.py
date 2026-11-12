"""Metadata-only connection/delivery diagnostics and bounded outbox recovery."""
import math
from datetime import timedelta

from . import connectors, db, webhooks


def connection_checks(conn, members):
    latest = {}
    for row in db.rows(conn, 'SELECT * FROM connection_checks ORDER BY created_at DESC LIMIT 100'):
        latest.setdefault(row['member_id'], row)
    for member in members:
        member['connector'] = connectors.status(conn, member['member_id'])
        check = latest.get(member['member_id'])
        receipt = db.one(conn, 'SELECT outcome,reported_at FROM processing_receipts WHERE member_id=? AND message_seq=?',
                         (member['member_id'], check['message_seq'])) if check else None
        member['connection_check'] = None if not check else {
            **check, 'status': 'revoked' if member['status'] != 'active' else
            ('agent_reported_' + receipt['outcome'] if receipt else
             'acknowledged' if (member['acked_seq'] or 0) >= check['message_seq'] else 'waiting_for_acknowledgement')}


def health(conn):
    now = db.utcnow()
    counts = {row['status']: row['n'] for row in db.rows(conn, 'SELECT status,count(*) AS n FROM webhook_deliveries GROUP BY status')}
    oldest = db.one(conn, "SELECT min(created_at) AS at FROM webhook_deliveries WHERE status='pending'")['at']
    stale = conn.execute("""SELECT count(*) FROM webhook_deliveries WHERE status='pending' AND next_at<?
        AND (lease_until IS NULL OR lease_until<?)""", (db.stamp(now-timedelta(minutes=2)),db.stamp(now))).fetchone()[0]
    sample = db.rows(conn, "SELECT created_at,completed_at FROM webhook_deliveries WHERE status='delivered' ORDER BY completed_at DESC LIMIT 100")
    latencies = sorted(max(0, (db.parse(r['completed_at'])-db.parse(r['created_at'])).total_seconds()) for r in sample)
    def percentile(p):
        return round(latencies[max(0,math.ceil(len(latencies)*p)-1)],3) if latencies else None
    return {'counts':counts, 'oldest_pending_age_s':round(max(0,(now-db.parse(oldest)).total_seconds()),1) if oldest else None,
            'overdue_pending':stale, 'sample_size':len(sample), 'delivery_latency_s':{'p50':percentile(.5),'p95':percentile(.95)},
            'last_receiver_acceptance_at':sample[0]['completed_at'] if sample else None,
            'meaning':'Receiver acceptance and inbox acknowledgement do not prove agent processing.',
            'recovery':'Active authenticated reads check for overdue work at most once per minute; daily maintenance covers idle rooms.'}


async def recover(settings):
    if not settings.serverless or settings.profile == 'sandbox':
        return
    conn = db.connect(db.target(settings))
    try:
        with db.transaction(conn, ('operations','outbox_recovery')):
            now = db.utcnow()
            previous = db.one(conn, "SELECT updated_at FROM operations_leases WHERE name='outbox_recovery'")
            if previous and db.parse(previous['updated_at']) > now-timedelta(minutes=1):
                return
            conn.execute("INSERT INTO operations_leases VALUES('outbox_recovery',?) ON CONFLICT(name) DO UPDATE SET updated_at=excluded.updated_at",(db.stamp(now),))
            overdue = db.one(conn,"""SELECT 1 FROM webhook_deliveries WHERE status='pending' AND next_at<?
                AND (lease_until IS NULL OR lease_until<?) LIMIT 1""",(db.stamp(now-timedelta(minutes=2)),db.stamp(now)))
        if overdue:
            await webhooks.kick(settings,force=True)
    except Exception:
        webhooks.log.warning('webhook_recovery_unavailable')
    finally:
        conn.close()


def cleanup(conn,check_id=None):
    cutoff = db.stamp(db.utcnow()-timedelta(days=1))
    for row in db.rows(conn,'SELECT * FROM delivery_probes WHERE '+('check_id=?' if check_id else 'created_at<?'),(check_id or cutoff,)):
        conn.execute('DELETE FROM messages WHERE seq IN (SELECT message_seq FROM watchdog_probes WHERE check_id=?)',(row['check_id'],))
        conn.execute('DELETE FROM webhook_deliveries WHERE subscription_id IN(SELECT subscription_id FROM webhook_subscriptions WHERE member_id=?)',(row['member_id'],))
        conn.execute('DELETE FROM webhook_subscriptions WHERE member_id=?',(row['member_id'],))
        conn.execute('DELETE FROM reconnect_tickets WHERE member_id=?',(row['member_id'],))
        conn.execute('DELETE FROM sessions WHERE member_id=?',(row['member_id'],))
        for table in ['member_cursors','member_activity','nonces','connection_leases','device_connections','pickup_receipts']:
            conn.execute('DELETE FROM '+table+' WHERE member_id=?',(row['member_id'],))
        conn.execute('DELETE FROM members WHERE member_id=?',(row['member_id'],))
        conn.execute('DELETE FROM delivery_probes WHERE check_id=?',(row['check_id'],))
    conn.execute('DELETE FROM connection_checks WHERE created_at<?',(db.stamp(db.utcnow()-timedelta(days=30)),))

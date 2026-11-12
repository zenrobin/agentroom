"""Agent-reported processing is distinct from transport receipt and provider acceptance."""
from datetime import timedelta

from . import db, webhooks, watchdog


def status(conn, member_id):
    runtime = db.one(conn, 'SELECT provider,recovery_interval_s,verified_at,reported_at FROM connector_runtime WHERE member_id=?', (member_id,))
    subscriptions = db.rows(conn, """SELECT s.* FROM webhook_subscriptions s
        WHERE s.member_id=? AND s.deleted_at IS NULL
        AND (EXISTS(SELECT 1 FROM webhook_provider_credentials p WHERE p.subscription_id=s.subscription_id AND p.provider='grok')
             OR EXISTS(SELECT 1 FROM outbound_connectors o WHERE o.subscription_id=s.subscription_id)) ORDER BY s.created_at DESC""", (member_id,))
    current = next((s for s in subscriptions if s['enabled'] and webhooks.subscription_actor(conn, s)), None)
    sub = current or next(iter(subscriptions), None)
    outbound_state = db.one(conn,'SELECT last_contact_at,recovery_interval_s,runtime_verified_at FROM outbound_connectors WHERE subscription_id=?', (sub['subscription_id'],)) if sub else None
    if outbound_state and outbound_state['runtime_verified_at']:
        runtime={'provider':'muse','recovery_interval_s':outbound_state['recovery_interval_s'],'verified_at':outbound_state['runtime_verified_at']}
    delivery = db.one(conn, '''SELECT delivery_id,message_seq,status,last_status,last_error,created_at,completed_at
        FROM webhook_deliveries WHERE subscription_id=? ORDER BY created_at DESC LIMIT 1''', (sub['subscription_id'],)) if sub else None
    receipt = db.one(conn, '''SELECT message_seq,message_id,outcome,reported_at FROM processing_receipts
        WHERE member_id=? ORDER BY reported_at DESC,message_seq DESC LIMIT 1''', (member_id,))
    state = 'not_configured'
    if sub:
        if not sub['enabled']:
            state = 'paused'
        elif not current:
            state = 'authorization_expired'
        elif not runtime:
            state = 'runtime_setup_needed'
        elif delivery and (delivery['status'] in {'failed', 'cancelled'} or delivery['last_status'] in {401, 403}):
            state = 'wake_failed'
        elif delivery and delivery['status'] == 'pending':
            state = 'wake_pending'
        elif delivery and delivery['status'] == 'delivered':
            state = 'wake_accepted'
        else:
            state = 'ready_to_test'
    # A report proves only what the authenticated agent reported about that exact message.
    tested_receipt = db.one(conn, 'SELECT outcome,reported_at FROM processing_receipts WHERE member_id=? AND message_seq=?',
                            (member_id, delivery['message_seq'])) if delivery and delivery['message_seq'] else None
    if tested_receipt and (state == 'wake_accepted' or (state == 'wake_failed' and delivery['last_error']=='handling_resolved')):
        state = 'awaiting_approval' if tested_receipt['outcome'] == 'awaiting_approval' else 'processing_reported'
    elif state == 'wake_accepted' and delivery['message_seq'] and runtime:
        if db.parse(delivery['completed_at']) < db.utcnow() - timedelta(seconds=runtime['recovery_interval_s'] * 2):
            state = 'processing_unconfirmed'
    recovery = watchdog.status(conn, member_id)
    if current and recovery['needs_attention']:
        state = 'processing_needs_attention'
    if current and outbound_state and runtime and (not outbound_state['last_contact_at'] or outbound_state['last_contact_at']<db.stamp(db.utcnow()-timedelta(seconds=90))):
        state='outbound_disconnected'
    return {'state': state, 'provider': 'muse' if outbound_state else ('grok' if sub or runtime else None), 'outbound':outbound_state, 'recovery': recovery,
            'subscription_id': sub['subscription_id'] if sub else None,
            'expires_at': sub['expires_at'] if sub else None,
            'renewal_due': bool(sub and sub['expires_at'] <= db.stamp(db.utcnow() + timedelta(days=7))),
            'runtime': runtime, 'latest_wake': delivery, 'latest_processing_report': receipt,
            'meaning': 'Wake acceptance, inbox receipt, and agent-reported processing are separate checks.'}

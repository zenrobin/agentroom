"""Public, temporary test lanes. Never enabled against a private room database."""
from datetime import timedelta
from uuid import uuid4

from . import db
from .errors import APIError

ROLE = 'sandbox_guest'
PEER = 'm_00000000000000000000000000000001'
LIFETIME = timedelta(minutes=15)


def guest(member):
    return member['role'] == ROLE


def expires(member):
    return db.parse(member['created_at']) + LIFETIME


def initialize(conn, settings):
    # A config typo must not turn a populated family database into a public demo.
    with db.transaction(conn):
        conn.execute('CREATE TABLE IF NOT EXISTS deployment_profile (id INTEGER PRIMARY KEY CHECK(id=1), kind TEXT NOT NULL)')
        mode = 'sandbox' if settings.profile == 'sandbox' else 'private'
        marker = db.one(conn, 'SELECT kind FROM deployment_profile WHERE id=1')
        if marker and marker['kind'] != mode:
            raise RuntimeError('Public sandbox requires its own empty database; a sandbox database cannot serve a private product')
        if mode == 'private':
            if db.one(conn, 'SELECT 1 FROM roles WHERE name IN(?,?) LIMIT 1', (ROLE, 'sandbox_peer')):
                raise RuntimeError('A sandbox database cannot serve a private product')
            conn.execute("INSERT INTO deployment_profile VALUES(1,'private') ON CONFLICT(id) DO NOTHING")
            return
        if db.one(conn, 'SELECT 1 FROM members WHERE role NOT IN(?,?) LIMIT 1', (ROLE, 'sandbox_peer')):
            raise RuntimeError('Public sandbox requires its own empty database')
        if any(conn.execute('SELECT count(*) FROM ' + table).fetchone()[0] for table in
               ['join_requests', 'pickup_receipts', 'owner_sessions', 'owner_challenges']):
            raise RuntimeError('Public sandbox requires its own empty database')
        if db.one(conn,"""SELECT 1 FROM webhook_subscriptions s WHERE NOT EXISTS
            (SELECT 1 FROM delivery_probes p WHERE p.subscription_id=s.subscription_id AND p.member_id=s.member_id
             AND s.url=? || p.check_id)
            AND NOT (s.url='' AND s.secret='' AND (s.deleted_at IS NOT NULL OR EXISTS
              (SELECT 1 FROM outbound_connectors o JOIN members m ON m.member_id=o.member_id
               WHERE o.subscription_id=s.subscription_id AND o.member_id=s.member_id AND m.role='sandbox_guest'))) LIMIT 1""",(settings.public_url+'/_internal/delivery-probes/',)):
            raise RuntimeError('Public sandbox requires its own empty database')
        if db.one(conn, "SELECT 1 FROM device_connections WHERE mode<>'sandbox' LIMIT 1"):
            raise RuntimeError('Public sandbox requires its own empty database')
        conn.execute("INSERT INTO deployment_profile VALUES(1,'sandbox') ON CONFLICT(id) DO NOTHING")
        for role in [ROLE, 'sandbox_peer']:
            conn.execute('INSERT INTO roles(name,scopes) VALUES(?,?) ON CONFLICT(name) DO NOTHING',
                         (role, 'room:read room:write negotiate'))
            conn.execute("INSERT INTO role_permissions VALUES(?,'*',1,1) ON CONFLICT(role,message_type) DO NOTHING", (role,))
        if not db.member(conn, PEER):
            db.add_identity(conn, {'agent_name': 'test-peer', 'display_name': 'Simulated test peer',
                            'owner_name': 'AgentRoom Sandbox', 'platform': 'Deterministic simulator'}, PEER, 'sandbox_peer')
        cleanup(conn)


def cleanup(conn):
    """Sandbox only: expire at 15m; delete demo data after 24h on cron/admission."""
    from .operations import cleanup as cleanup_operations
    cleanup_operations(conn)
    now = db.utcnow()
    conn.execute("UPDATE members SET status='removed' WHERE role=? AND created_at<=?", (ROLE, db.stamp(now - LIFETIME)))
    cutoff = db.stamp(now - timedelta(days=1))
    conn.execute('DELETE FROM messages WHERE received_at<?', (cutoff,))
    conn.execute('DELETE FROM negotiations WHERE created_at<?', (cutoff,))
    conn.execute('DELETE FROM audit_log WHERE at<?', (cutoff,))
    old = db.rows(conn, 'SELECT member_id FROM members WHERE role=? AND created_at<?', (ROLE, cutoff))
    for row in old:
        mid = row['member_id']
        conn.execute('DELETE FROM reconnect_tickets WHERE member_id=?', (mid,))
        conn.execute('DELETE FROM webhook_deliveries WHERE subscription_id IN (SELECT subscription_id FROM webhook_subscriptions WHERE member_id=?)',(mid,))
        conn.execute('DELETE FROM webhook_subscriptions WHERE member_id=?',(mid,))
        conn.execute('DELETE FROM sessions WHERE member_id=?', (mid,))
        for table in ['device_connections', 'member_activity', 'member_cursors', 'nonces', 'connection_leases']:
            conn.execute(f'DELETE FROM {table} WHERE member_id=?', (mid,))
        conn.execute('DELETE FROM members WHERE member_id=?', (mid,))


def check_send(settings, actor, model, raw_size, limiter):
    if not guest(actor):
        return
    if settings.profile != 'sandbox':
        raise APIError(403, 'forbidden')
    if raw_size > 4096:
        raise APIError(413, 'payload_too_large', 'Sandbox messages are limited to 4 KB.')
    if model.to != [PEER]:
        raise APIError(403, 'sandbox_private_only', 'Address only test-peer. Public broadcasts and other visitors are unavailable.')
    limiter.hourly('sandbox_send', actor['member_id'], 60)
    limiter.hourly('sandbox_send_global', 'room', 300)


def reply(conn, settings, message):
    """A simulator, never an LLM, tool invocation, or real-world action."""
    from .signing import sign
    if message['type'] != 'chat':
        return
    now = db.stamp()
    response = {'id': str(uuid4()), 'from': PEER, 'to': [message['from']], 'type': 'chat',
                'ts': now, 'received_at': now, 'thread_id': message['thread_id'], 'reply_to': message['id'],
                'body': {'text': 'Test peer received: ' + message['body']['text'][:500] + '\nSimulation only; no external action was taken.'}}
    response['sig'] = sign(response, settings.signing_key)
    db.insert_message(conn, response)

"""All SQL for Agent Room. PostgreSQL on Vercel; SQLite for isolated local tests."""
import json
import os
import sqlite3
import hashlib
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import Request

SCHEMA_VERSION = 4
WRITE_LOCK = 713381901
_request_connections = ContextVar('agent_room_connections', default=None)


class RequestConnectionsMiddleware:
    """Reuse a pooled Neon connection through the full ASGI request, including background tasks."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        connections = {}
        token = _request_connections.set(connections)
        try:
            await self.app(scope, receive, send)
        finally:
            _request_connections.reset(token)
            for conn in connections.values():
                conn.raw.close()


def target(settings):
    return settings.database_url or settings.db_path


class Record(dict):
    """Support mapping access and sqlite3.Row-style numeric column access."""
    def __getitem__(self, key):
        return list(self.values())[key] if isinstance(key, int) else super().__getitem__(key)


def record_factory(cursor):
    names = [column.name for column in cursor.description or []]
    return lambda values: Record(zip(names, values))


class Postgres:
    postgres = True

    def __init__(self, url):
        import certifi
        import psycopg
        self.raw = psycopg.connect(url, autocommit=True, row_factory=record_factory, prepare_threshold=None,
                                  connect_timeout=10, sslmode='verify-full', sslrootcert=certifi.where())
        self.request_managed = False

    def execute(self, sql, args=()):
        return self.raw.execute(sql.replace('?', '%s'), args or None)

    def executemany(self, sql, args):
        with self.raw.cursor() as cursor:
            cursor.executemany(sql.replace('?', '%s'), args)

    def close(self):
        if not self.request_managed:
            self.raw.close()

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()


def utcnow():
    return datetime.now(timezone.utc)


def stamp(value=None):
    return (value or utcnow()).astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def parse(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def connect(path):
    if isinstance(path, str) and path.startswith(('postgres://', 'postgresql://')):
        connections = _request_connections.get()
        if connections is not None:
            if path not in connections:
                conn = Postgres(path)
                conn.request_managed = True
                connections[path] = conn
            return connections[path]
        return Postgres(path)
    conn = sqlite3.connect(str(path), timeout=5, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=5000')
    return conn


def initialize(path):
    if isinstance(path, str) and path.startswith(('postgres://', 'postgresql://')):
        conn = connect(path)
        try:
            with transaction(conn):
                exists = conn.execute("SELECT to_regclass('public.schema_version')").fetchone()[0]
                if not exists:
                    conn.raw.execute(Path(__file__).with_name('schema.postgres.sql').read_text(), prepare=False)
                upgrade_auth(conn)
                upgrade_console(conn)
                upgrade_operations(conn)
                upgrade_provider_setup(conn)
                upgrade_connector(conn)
                upgrade_watchdog(conn)
                upgrade_outbound(conn)
                upgrade_room_rules(conn)
                if not ready(conn):
                    raise RuntimeError('Unsupported PostgreSQL schema version; run the migration first')
        finally:
            conn.close()
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    os.fchmod(fd, 0o600)
    os.close(fd)
    conn = connect(path)
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version'").fetchone()
        if not exists:
            conn.executescript('BEGIN IMMEDIATE;\n' + Path(__file__).with_name('schema.sql').read_text() + '\nCOMMIT;')
        version = conn.execute('SELECT version FROM schema_version').fetchone()[0]
        if version == 1:
            with transaction(conn):
                conn.execute("CREATE TABLE IF NOT EXISTS pickup_receipts (request_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, poll_secret_hash TEXT NOT NULL, body_hash TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN('approved','delivered')))")
                conn.execute("INSERT OR IGNORE INTO pickup_receipts SELECT request_id,member_id,poll_secret_hash,body_hash,status FROM join_requests WHERE status IN('approved','delivered') AND member_id IS NOT NULL")
                conn.execute('UPDATE schema_version SET version=2')
            version = 2
        if version == 2:
            with transaction(conn):
                for statement in Path(__file__).with_name('shared.sql').read_text().split(';'):
                    if statement.strip():
                        conn.execute(statement)
                conn.execute('UPDATE schema_version SET version=3')
            version = 3
        if version == 3:
            with transaction(conn):
                upgrade_auth(conn)
            version = 4
        if version == 4:
            with transaction(conn):
                upgrade_console(conn)
                upgrade_operations(conn)
                upgrade_provider_setup(conn)
                upgrade_connector(conn)
                upgrade_watchdog(conn)
                upgrade_outbound(conn)
                upgrade_room_rules(conn)
        if version != SCHEMA_VERSION:
            raise RuntimeError('Unsupported database schema version')
    finally:
        conn.close()


def upgrade_auth(conn):
    if conn.execute('SELECT version FROM schema_version').fetchone()[0] == 3:
        for sql in Path(__file__).with_name('auth-state.sql').read_text().split(';'):
            if sql.strip():
                conn.execute(sql)
        conn.execute('UPDATE schema_version SET version=4')


def upgrade_console(conn):
    if conn.execute('SELECT version FROM schema_version').fetchone()[0] == 4:
        if console_ready(conn):
            return
        source = Path(__file__).with_name('console-delivery.sql').read_text()
        if getattr(conn, 'postgres', False):
            conn.raw.execute(source, prepare=False)
        else:
            for sql in source.split(';'):
                if sql.strip():
                    conn.execute(sql)


def console_ready(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.console_delivery_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='console_delivery_version'").fetchone()
    return bool(exists) and conn.execute('SELECT version FROM console_delivery_version').fetchone()[0] == 1


def operations_version(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.operations_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='operations_version'").fetchone()
    return conn.execute('SELECT version FROM operations_version').fetchone()[0] if exists else 0


def operations_ready(conn):
    return operations_version(conn)==2


def provider_setup_ready(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.provider_setup_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='provider_setup_version'").fetchone()
    return bool(exists) and conn.execute('SELECT version FROM provider_setup_version').fetchone()[0] == 1


def connector_ready(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.connector_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='connector_version'").fetchone()
    return bool(exists) and conn.execute('SELECT version FROM connector_version').fetchone()[0] == 1


def upgrade_connector(conn):
    if connector_ready(conn):
        return
    source = Path(__file__).with_name('connector.sql').read_text()
    if getattr(conn, 'postgres', False):
        conn.raw.execute(source, prepare=False)
    else:
        for sql in source.split(';'):
            if sql.strip():
                conn.execute(sql)


def watchdog_ready(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.watchdog_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='watchdog_version'").fetchone()
    return bool(exists) and conn.execute('SELECT version FROM watchdog_version').fetchone()[0] == 1


def upgrade_watchdog(conn):
    if watchdog_ready(conn):
        return
    for sql in Path(__file__).with_name('watchdog.sql').read_text().split(';'):
        if sql.strip():
            conn.execute(sql)


def outbound_ready(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.outbound_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='outbound_version'").fetchone()
    return bool(exists) and conn.execute('SELECT version FROM outbound_version').fetchone()[0] == 1


def upgrade_outbound(conn):
    if outbound_ready(conn):
        return
    for sql in Path(__file__).with_name('outbound.sql').read_text().split(';'):
        if sql.strip():
            conn.execute(sql)


def room_rules_ready(conn):
    if getattr(conn, 'postgres', False):
        exists = conn.execute("SELECT to_regclass('public.room_rules_version')").fetchone()[0]
    else:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='room_rules_version'").fetchone()
    return bool(exists) and conn.execute('SELECT version FROM room_rules_version').fetchone()[0] == 1


def upgrade_room_rules(conn):
    if room_rules_ready(conn):
        return
    for sql in Path(__file__).with_name('room-rules.sql').read_text().split(';'):
        if sql.strip():
            conn.execute(sql)


def upgrade_provider_setup(conn):
    if provider_setup_ready(conn):
        return
    source = Path(__file__).with_name('provider-setup.sql').read_text()
    if getattr(conn, 'postgres', False):
        conn.raw.execute(source, prepare=False)
    else:
        for sql in source.split(';'):
            if sql.strip():
                conn.execute(sql)


def upgrade_operations(conn):
    if operations_ready(conn):
        return
    if operations_version(conn)==0:
        source = Path(__file__).with_name('operations.sql').read_text()
        if getattr(conn, 'postgres', False):
            conn.raw.execute(source, prepare=False)
        else:
            for sql in source.split(';'):
                if sql.strip():
                    conn.execute(sql)
    if operations_version(conn)==1:
        conn.execute('''CREATE TABLE webhook_recoveries (delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
            recovery_id TEXT NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(delivery_id,recovery_id))''')
        conn.execute('UPDATE operations_version SET version=2')


def dependency(request: Request):
    conn = connect(target(request.app.state.settings))
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction(conn, lock_key=None):
    try:
        if getattr(conn, 'postgres', False):
            conn.execute('BEGIN')
            conn.execute("SET LOCAL statement_timeout='10s'")
            conn.execute("SET LOCAL lock_timeout='5s'")
            if lock_key is None:
                # Only room state mutations need total sequence/commit ordering.
                conn.execute('SELECT pg_advisory_xact_lock(?)', (WRITE_LOCK,))
            else:
                # Separate lock namespace: junk requests cannot hold the room's mutation lock.
                key = int.from_bytes(hashlib.sha256(json.dumps(lock_key).encode()).digest()[:4], 'big', signed=True)
                conn.execute('SELECT pg_advisory_xact_lock(?,?)', (713381902, key))
        else:
            conn.execute('BEGIN IMMEDIATE')
        yield
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def one(conn, sql, args=()):
    row = conn.execute(sql, args).fetchone()
    return dict(row) if row else None


def rows(conn, sql, args=()):
    return [dict(row) for row in conn.execute(sql, args)]


def ready(conn):
    return (conn.execute('SELECT 1').fetchone()[0] == 1 and conn.execute('SELECT version FROM schema_version').fetchone()[0] == SCHEMA_VERSION
            and console_ready(conn) and operations_ready(conn) and provider_setup_ready(conn) and connector_ready(conn) and watchdog_ready(conn) and outbound_ready(conn) and room_rules_ready(conn))


def audit(conn, actor, action, target=None, ip=None, detail=None):
    conn.execute('INSERT INTO audit_log(at,actor,action,target,ip,detail) VALUES(?,?,?,?,?,?)',
                 (stamp(), actor, action, target, ip, detail))


def join_request(conn, request_id):
    return (one(conn, 'SELECT * FROM join_requests WHERE request_id=?', (request_id,)) or
            one(conn, 'SELECT * FROM pickup_receipts WHERE request_id=?', (request_id,)))


def join_counts(conn):
    return (conn.execute("SELECT count(*) FROM members WHERE status='active'").fetchone()[0],
            conn.execute("SELECT count(*) FROM join_requests WHERE status='pending'").fetchone()[0])


def name_taken(conn, name):
    return bool(conn.execute("SELECT 1 FROM members WHERE agent_name=? UNION ALL SELECT 1 FROM join_requests WHERE agent_name=? AND status='pending'", (name, name)).fetchone())


def create_join(conn, data, secret_hash, body_hash, ip):
    conn.execute('INSERT INTO join_requests(request_id,poll_secret_hash,body_hash,agent_name,display_name,owner_name,owner_contact,platform,purpose,ip,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                 (str(data.request_id), secret_hash, body_hash, data.agent_name, data.display_name, data.owner_name,
                  data.owner_contact, data.platform, data.purpose, ip, 'pending', stamp()))
    audit(conn, 'anonymous', 'join.created', str(data.request_id), ip)


def expire_joins(conn):
    stale = rows(conn, "SELECT request_id FROM join_requests WHERE status='pending' AND created_at<?", (stamp(utcnow() - timedelta(days=7)),))
    for row in stale:
        conn.execute("UPDATE join_requests SET status='expired',decided_at=? WHERE request_id=?", (stamp(), row['request_id']))
        audit(conn, 'room', 'join.expired', row['request_id'])


def list_joins(conn, status):
    if status == 'approved':
        return rows(conn, "SELECT request_id,agent_name,display_name,owner_name,owner_contact,platform,purpose,ip,status,member_id,created_at,decided_at FROM join_requests WHERE status IN('approved','delivered') ORDER BY created_at")
    return rows(conn, 'SELECT request_id,agent_name,display_name,owner_name,owner_contact,platform,purpose,ip,status,member_id,created_at,decided_at FROM join_requests WHERE status=? ORDER BY created_at', (status,))


def max_seq(conn):
    if getattr(conn, 'postgres', False):
        return conn.execute('SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM messages_seq_seq').fetchone()[0]
    # sqlite_sequence preserves the high-water mark after retention removes the tail.
    return conn.execute("SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='messages'),0)").fetchone()[0]


def add_member(conn, req, member_id, role):
    add_identity(conn, req, member_id, role)
    conn.execute("UPDATE join_requests SET status='approved',member_id=?,decided_at=? WHERE request_id=?", (member_id, stamp(), req['request_id']))
    conn.execute("INSERT INTO pickup_receipts VALUES(?,?,?,?,'approved')", (req['request_id'], member_id, req['poll_secret_hash'], req['body_hash']))


def add_identity(conn, req, member_id, role):
    conn.execute('INSERT INTO members(member_id,agent_name,display_name,owner_name,platform,role,status,joined_seq,created_at) VALUES(?,?,?,?,?,?,?,?,?)',
                 (member_id, req['agent_name'], req['display_name'], req['owner_name'], req['platform'], role, 'active', max_seq(conn), stamp()))
    conn.execute('INSERT INTO member_cursors VALUES(?,?,?)', (member_id, 0, stamp()))


def decide_join(conn, request_id, status):
    conn.execute('UPDATE join_requests SET status=?,decided_at=? WHERE request_id=?', (status, stamp(), request_id))


def delivered(conn, request_id):
    conn.execute("UPDATE join_requests SET status='delivered' WHERE request_id=?", (request_id,))
    conn.execute("UPDATE pickup_receipts SET status='delivered' WHERE request_id=?", (request_id,))


def member(conn, member_id):
    return one(conn, 'SELECT * FROM members WHERE member_id=?', (member_id,))


def members(conn, owner=False):
    if owner:
        return rows(conn, 'SELECT * FROM members ORDER BY created_at')
    return rows(conn, "SELECT member_id,agent_name,display_name,owner_name,platform FROM members WHERE status='active' ORDER BY created_at")


def role_scopes(conn, role):
    row = one(conn, 'SELECT scopes FROM roles WHERE name=?', (role,))
    return row['scopes'] if row else None


def permission(conn, role, action, message_type):
    row = one(conn, 'SELECT can_send,can_read FROM role_permissions WHERE role=? AND message_type IN(?,\'*\') ORDER BY CASE WHEN message_type=? THEN 0 ELSE 1 END LIMIT 1', (role, message_type, message_type))
    return bool(row and row['can_' + action])


def create_session(conn, session_id, member_id, refresh_hash):
    conn.execute('INSERT INTO sessions(session_id,member_id,refresh_hash,expires_at,created_at) VALUES(?,?,?,?,?)',
                 (session_id, member_id, refresh_hash, stamp(utcnow() + timedelta(days=30)), stamp()))


def session(conn, session_id):
    return one(conn, 'SELECT * FROM sessions WHERE session_id=?', (session_id,))


def refresh_lookup(conn, hashed):
    row = one(conn, 'SELECT * FROM sessions WHERE refresh_hash=?', (hashed,))
    if row:
        return row, False
    row = one(conn, 'SELECT s.* FROM used_refresh_tokens u JOIN sessions s ON s.session_id=u.session_id WHERE u.refresh_hash=?', (hashed,))
    return row, bool(row)


def rotate_refresh(conn, row, new_hash):
    conn.execute('INSERT INTO used_refresh_tokens VALUES(?,?)', (row['refresh_hash'], row['session_id']))
    conn.execute('UPDATE sessions SET prev_refresh_hash=refresh_hash,refresh_hash=? WHERE session_id=?', (new_hash, row['session_id']))


def refresh_exchange(conn, hashed):
    return one(conn, 'SELECT * FROM refresh_exchanges WHERE refresh_hash=?', (hashed,))


def store_refresh_exchange(conn, hashed, session_id, request_id, reply):
    conn.execute('INSERT INTO refresh_exchanges VALUES(?,?,?,?,?)',
                 (hashed, session_id, request_id, stamp(utcnow() + timedelta(minutes=5)), reply))


def create_reconnect(conn, ticket_id, member_id, secret_hash):
    # Resetting access must close every old join/pickup path, including approved-but-unclaimed joins.
    conn.execute("UPDATE join_requests SET status='delivered' WHERE member_id=? AND status='approved'", (member_id,))
    conn.execute("UPDATE pickup_receipts SET status='delivered' WHERE member_id=? AND status='approved'", (member_id,))
    conn.execute('UPDATE reconnect_tickets SET expires_at=? WHERE member_id=?', (stamp(), member_id))
    conn.execute('INSERT INTO reconnect_tickets(ticket_id,member_id,secret_hash,created_at,expires_at) VALUES(?,?,?,?,?)',
                 (ticket_id, member_id, secret_hash, stamp(), stamp(utcnow() + timedelta(hours=24))))


def reconnect_ticket(conn, ticket_id):
    return one(conn, 'SELECT * FROM reconnect_tickets WHERE ticket_id=?', (ticket_id,))


def consume_reconnect(conn, ticket_id, session_id, reply):
    conn.execute('UPDATE reconnect_tickets SET session_id=?,reply=?,reply_expires_at=? WHERE ticket_id=?',
                 (session_id, reply, stamp(utcnow() + timedelta(minutes=5)), ticket_id))


def revoke_session(conn, session_id):
    conn.execute('UPDATE sessions SET revoked_at=coalesce(revoked_at,?) WHERE session_id=?', (stamp(), session_id))


def revoke_member(conn, member_id, remove=False):
    conn.execute('DELETE FROM connector_runtime WHERE member_id=?', (member_id,))
    conn.execute('DELETE FROM connector_watchdogs WHERE member_id=?', (member_id,))
    conn.execute("UPDATE connector_work SET status='cancelled' WHERE member_id=? AND status IN ('waiting','attention')", (member_id,))
    conn.execute('UPDATE sessions SET revoked_at=coalesce(revoked_at,?) WHERE member_id=?', (stamp(), member_id))
    conn.execute('UPDATE webhook_subscriptions SET enabled=0,generation=generation+1 WHERE member_id=?', (member_id,))
    conn.execute("UPDATE device_connections SET status='denied' WHERE member_id=?", (member_id,))
    conn.execute('DELETE FROM reconnect_tickets WHERE member_id=?', (member_id,))
    conn.execute("UPDATE pickup_receipts SET status='delivered' WHERE member_id=?", (member_id,))
    conn.execute("UPDATE join_requests SET status='delivered' WHERE member_id=? AND status='approved'", (member_id,))
    if remove:
        conn.execute("UPDATE members SET status='removed' WHERE member_id=?", (member_id,))


def origin_join(conn, member_id):
    return one(conn, 'SELECT * FROM pickup_receipts WHERE member_id=?', (member_id,))


def insert_message(conn, envelope, submit_hash=None):
    from .signing import canonical_json
    sql = 'INSERT INTO messages(id,from_id,type,thread_id,reply_to,ts,received_at,body,is_private,submit_hash,sig,envelope) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'
    cursor = conn.execute(sql + (' RETURNING seq' if getattr(conn, 'postgres', False) else ''),
                          (envelope['id'], envelope['from'], envelope['type'], envelope['thread_id'], envelope.get('reply_to'),
                           envelope['ts'], envelope['received_at'], canonical_json(envelope['body']), int('to' in envelope),
                           submit_hash, envelope.get('sig'), canonical_json(envelope)))
    seq = cursor.fetchone()[0] if getattr(conn, 'postgres', False) else cursor.lastrowid
    conn.executemany('INSERT INTO message_recipients VALUES(?,?)', [(seq, mid) for mid in envelope.get('to', [])])
    if getattr(conn, 'postgres', False):
        count_metric(conn, 'messages', envelope['type'])
    from .webhooks import enqueue_message
    enqueue_message(conn, {**envelope, 'seq': seq})
    return {**envelope, 'seq': seq}


def envelope(row):
    return {**json.loads(row['envelope']), 'seq': row['seq']}


def prior_message(conn, member_id, message_id):
    return one(conn, 'SELECT * FROM messages WHERE from_id=? AND id=?', (member_id, message_id))


def nonce_exists(conn, member_id, nonce):
    return bool(conn.execute('SELECT 1 FROM nonces WHERE member_id=? AND nonce=? AND expires_at>?', (member_id, nonce, stamp())).fetchone())


def add_nonce(conn, member_id, nonce):
    conn.execute('INSERT INTO nonces VALUES(?,?,?) ON CONFLICT(member_id,nonce) DO UPDATE SET expires_at=excluded.expires_at', (member_id, nonce, stamp(utcnow() + timedelta(minutes=10))))


VISIBLE = """m.seq>? AND (m.is_private=0 OR m.from_id=? OR EXISTS(SELECT 1 FROM message_recipients r WHERE r.seq=m.seq AND r.member_id=?))
 AND (?<>'sandbox_guest' OR m.is_private=1)
 AND coalesce((SELECT can_read FROM role_permissions p WHERE p.role=? AND p.message_type=m.type),
              (SELECT can_read FROM role_permissions p WHERE p.role=? AND p.message_type='*'),0)=1
 AND (m.type NOT IN('propose','counter','accept','decline','withdraw','commit','expire') OR ?)"""


def visibility_args(actor):
    return (actor['joined_seq'], actor['member_id'], actor['member_id'], actor['role'], actor['role'], actor['role'], actor.get('can_negotiate', False))


def visible_id(conn, actor, message_id):
    row = one(conn, 'SELECT m.* FROM messages m WHERE ' + VISIBLE + ' AND m.id=? LIMIT 1', (*visibility_args(actor), message_id))
    return envelope(row) if row else None


def message_page(conn, actor, after=0, limit=50, thread_id=None, inbox=False):
    clauses, args = [VISIBLE, 'm.seq>?'], [*visibility_args(actor), after]
    if inbox:
        clauses.append('m.from_id<>?')
        args.append(actor['member_id'])
    if thread_id:
        clauses.append('m.thread_id=?')
        args.append(str(thread_id))
    data = rows(conn, 'SELECT m.* FROM messages m WHERE ' + ' AND '.join(clauses) + ' ORDER BY m.seq LIMIT ?', (*args, limit + 1))
    items = [envelope(row) for row in data[:limit]]
    return {'items': items, 'next_after': items[-1]['seq'] if items else after, 'has_more': len(data) > limit}


def visible_message(conn, actor, seq):
    row = one(conn, 'SELECT m.* FROM messages m WHERE ' + VISIBLE + ' AND m.seq=?', (*visibility_args(actor), seq))
    return envelope(row) if row else None


def highest_visible(conn, actor):
    row = one(conn, 'SELECT max(m.seq) AS seq FROM messages m WHERE ' + VISIBLE, visibility_args(actor))
    return row['seq'] or 0


def cursor(conn, member_id):
    return conn.execute('SELECT acked_seq FROM member_cursors WHERE member_id=?', (member_id,)).fetchone()[0]


def ack(conn, actor, seq):
    capped = min(seq, highest_visible(conn, actor))
    greatest = 'greatest' if getattr(conn, 'postgres', False) else 'max'
    conn.execute(f'UPDATE member_cursors SET acked_seq={greatest}(acked_seq,?),updated_at=? WHERE member_id=?', (capped, stamp(), actor['member_id']))
    return cursor(conn, actor['member_id'])


def thread_exists(conn, actor, thread_id):
    return bool(one(conn, 'SELECT 1 FROM messages m WHERE ' + VISIBLE + ' AND m.thread_id=? LIMIT 1', (*visibility_args(actor), str(thread_id))))


def negotiation(conn, negotiation_id):
    return one(conn, 'SELECT * FROM negotiations WHERE negotiation_id=?', (negotiation_id,))


def parties(conn, negotiation_id):
    return [row['member_id'] for row in rows(conn, 'SELECT member_id FROM negotiation_parties WHERE negotiation_id=? ORDER BY member_id', (negotiation_id,))]


def version(conn, negotiation_id, number):
    return one(conn, 'SELECT * FROM negotiation_versions WHERE negotiation_id=? AND version=?', (negotiation_id, number))


def create_negotiation(conn, nid, subject, author, recipients):
    conn.execute("INSERT INTO negotiations VALUES(?,?,?,'open',1,?,NULL)", (nid, subject, author, stamp()))
    conn.executemany('INSERT INTO negotiation_parties VALUES(?,?)', [(nid, mid) for mid in [author, *recipients]])


def store_version(conn, nid, number, terms, terms_hash, author, seq, expires_at):
    from .signing import canonical_json
    conn.execute('INSERT INTO negotiation_versions(negotiation_id,version,terms,terms_hash,author_id,message_seq,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?)',
                 (nid, number, canonical_json(terms), terms_hash, author, seq, expires_at, stamp()))
    conn.execute('UPDATE negotiations SET current_version=? WHERE negotiation_id=?', (number, nid))
    conn.execute('DELETE FROM acceptances WHERE negotiation_id=?', (nid,))
    acceptance(conn, nid, number, author, seq)


def acceptance(conn, nid, number, member_id, seq):
    conn.execute('INSERT INTO acceptances VALUES(?,?,?,?,?) ON CONFLICT(negotiation_id,version,member_id) DO NOTHING', (nid, number, member_id, seq, stamp()))


def acceptances(conn, nid, number):
    return rows(conn, 'SELECT a.member_id,m.id AS message_id FROM acceptances a JOIN messages m ON m.seq=a.message_seq WHERE a.negotiation_id=? AND a.version=? ORDER BY a.member_id', (nid, number))


def close_negotiation(conn, nid, state):
    conn.execute('UPDATE negotiations SET state=?,closed_at=? WHERE negotiation_id=?', (state, stamp(), nid))


def save_commit(conn, nid, number, hashed, message):
    conn.execute('INSERT INTO commits VALUES(?,?,?,?,?,?)', (nid, number, hashed, message['seq'], message['sig'], message['body']['committed_at']))


def commit_message(conn, nid):
    row = one(conn, 'SELECT m.* FROM commits c JOIN messages m ON m.seq=c.message_seq WHERE c.negotiation_id=?', (nid,))
    return envelope(row) if row else None


def due_negotiations(conn):
    return rows(conn, "SELECT n.* FROM negotiations n JOIN negotiation_versions v ON n.negotiation_id=v.negotiation_id AND n.current_version=v.version WHERE n.state='open' AND v.expires_at<=?", (stamp(),))


def list_negotiations(conn, actor, state):
    return rows(conn, 'SELECT n.* FROM negotiations n JOIN negotiation_parties p ON n.negotiation_id=p.negotiation_id WHERE p.member_id=? AND n.state=? AND EXISTS(SELECT 1 FROM negotiation_versions v JOIN messages m ON v.message_seq=m.seq WHERE v.negotiation_id=n.negotiation_id AND ' + VISIBLE + ') ORDER BY n.created_at',
                (actor['member_id'], state, *visibility_args(actor)))


def negotiation_detail(conn, nid):
    data = negotiation(conn, nid)
    data['parties'] = parties(conn, nid)
    data['versions'] = [{'version': v['version'], 'terms': json.loads(v['terms']), 'terms_hash': v['terms_hash'],
                         'author': v['author_id'], 'expires_at': v['expires_at']} for v in rows(conn, 'SELECT * FROM negotiation_versions WHERE negotiation_id=? ORDER BY version', (nid,))]
    number = data['current_version']
    data['accepted_by'] = {str(number): [a['member_id'] for a in acceptances(conn, nid, number)]}
    data['commit'] = commit_message(conn, nid)
    return data


def audit_page(conn, after, limit):
    return rows(conn, 'SELECT * FROM audit_log WHERE id>? ORDER BY id LIMIT ?', (after, limit))


def metric_counts(conn):
    return {'messages': rows(conn, 'SELECT type,count(*) AS n FROM messages GROUP BY type'),
            'joins': rows(conn, 'SELECT status,count(*) AS n FROM join_requests GROUP BY status'),
            'commits': conn.execute('SELECT count(*) FROM commits').fetchone()[0]}


def cleanup(conn, retention_days):
    from .operations import cleanup as cleanup_operations
    cleanup_operations(conn)
    now = utcnow()
    conn.execute('DELETE FROM nonces WHERE expires_at<=?', (stamp(now),))
    conn.execute("DELETE FROM messages WHERE type IN('chat','request','response','event') AND received_at<?", (stamp(now - timedelta(days=retention_days)),))
    # Keep open negotiations. Terminal records and their messages share the same retention boundary.
    cutoff = stamp(now - timedelta(days=365))
    conn.execute("DELETE FROM messages WHERE thread_id IN(SELECT negotiation_id FROM negotiations WHERE state<>'open' AND closed_at<?)", (cutoff,))
    conn.execute("DELETE FROM negotiations WHERE state<>'open' AND closed_at<?", (cutoff,))
    conn.execute("DELETE FROM messages WHERE type='system' AND received_at<?", (cutoff,))
    conn.execute("DELETE FROM join_requests WHERE status IN('denied','expired','delivered') AND decided_at<?", (stamp(now - timedelta(days=30)),))
    # Keep one-time delivery state after application/contact metadata expires.
    conn.execute("DELETE FROM pickup_receipts WHERE member_id IN(SELECT member_id FROM members WHERE status='removed')")
    conn.execute('DELETE FROM audit_log WHERE at<?', (cutoff,))
    conn.execute('DELETE FROM sessions WHERE expires_at<?', (stamp(now),))
    conn.execute('DELETE FROM refresh_exchanges WHERE expires_at<?', (stamp(now),))
    conn.execute('DELETE FROM reconnect_tickets WHERE expires_at<?', (stamp(now),))
    now_epoch = now.timestamp()
    conn.execute('DELETE FROM rate_buckets WHERE updated_at<?', (now_epoch - 7200,))
    conn.execute('DELETE FROM rate_windows WHERE updated_at<?', (now_epoch - 7200,))
    conn.execute('DELETE FROM auth_blocks WHERE updated_at<? AND blocked_until<?', (now_epoch - 7200, now_epoch))
    conn.execute('DELETE FROM connection_leases WHERE expires_at<?', (now_epoch,))
    conn.execute('DELETE FROM owner_challenges WHERE expires_at<?', (stamp(now),))
    conn.execute('DELETE FROM owner_sessions WHERE expires_at<?', (stamp(now),))
    conn.execute('DELETE FROM delivery_worker_checks WHERE created_at<?', (stamp(now - timedelta(days=7)),))
    conn.execute('DELETE FROM device_connections WHERE expires_at<? AND (reply_expires_at IS NULL OR reply_expires_at<?)', (stamp(now), stamp(now)))
    conn.execute("DELETE FROM webhook_deliveries WHERE completed_at IS NOT NULL AND completed_at<?", (stamp(now - timedelta(days=7)),))


def prune_nonces(conn):
    conn.execute('DELETE FROM nonces WHERE expires_at<=?', (stamp(),))


def backup(path):
    if isinstance(path, str):
        raise RuntimeError('Use provider snapshots or pg_dump for PostgreSQL backups')
    directory = path.parent / 'backups'
    directory.mkdir(mode=0o700, exist_ok=True)
    os.chmod(directory, 0o700)
    target = directory / f'room-{utcnow():%Y%m%d}.db'
    if target.exists():
        return
    temporary = target.with_suffix('.tmp')
    fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_RDWR, 0o600)
    os.close(fd)
    source, destination = connect(path), sqlite3.connect(temporary)
    try:
        source.backup(destination)
    finally:
        destination.close()
        source.close()
    temporary.replace(target)
    for old in sorted(directory.glob('room-????????.db'))[:-7]:
        old.unlink()


def clock(conn):
    if getattr(conn, 'postgres', False):
        return float(conn.execute('SELECT extract(epoch FROM clock_timestamp())').fetchone()[0])
    return utcnow().timestamp()


def count_metric(conn, name, label='', amount=1):
    conn.execute('INSERT INTO shared_metrics(name,label,value) VALUES(?,?,?) ON CONFLICT(name,label) DO UPDATE SET value=shared_metrics.value+excluded.value', (name, label, amount))


def shared_metrics(conn, name):
    return {row['label']: row['value'] for row in rows(conn, 'SELECT label,value FROM shared_metrics WHERE name=?', (name,))}


def bucket(conn, scope, key):
    return one(conn, 'SELECT * FROM rate_buckets WHERE scope=? AND key=?', (scope, key))


def store_bucket(conn, scope, key, tokens, now):
    conn.execute('INSERT INTO rate_buckets VALUES(?,?,?,?) ON CONFLICT(scope,key) DO UPDATE SET tokens=excluded.tokens,updated_at=excluded.updated_at', (scope, key, tokens, now))


def window(conn, scope, key):
    row = one(conn, 'SELECT timestamps FROM rate_windows WHERE scope=? AND key=?', (scope, key))
    return json.loads(row['timestamps']) if row else []


def store_window(conn, scope, key, timestamps, now):
    conn.execute('INSERT INTO rate_windows VALUES(?,?,?,?) ON CONFLICT(scope,key) DO UPDATE SET timestamps=excluded.timestamps,updated_at=excluded.updated_at', (scope, key, json.dumps(timestamps), now))


def auth_block(conn, ip):
    return one(conn, 'SELECT * FROM auth_blocks WHERE ip=?', (ip,))


def store_auth_block(conn, ip, timestamps, blocked_until, now):
    conn.execute('INSERT INTO auth_blocks VALUES(?,?,?,?) ON CONFLICT(ip) DO UPDATE SET timestamps=excluded.timestamps,blocked_until=excluded.blocked_until,updated_at=excluded.updated_at', (ip, json.dumps(timestamps), blocked_until, now))


def acquire_lease(conn, lease_id, member_id, kind):
    now = clock(conn)
    conn.execute('DELETE FROM connection_leases WHERE expires_at<=?', (now,))
    count = conn.execute('SELECT count(*) FROM connection_leases WHERE member_id=? AND kind=?', (member_id, kind)).fetchone()[0]
    if count >= 2:
        return False
    conn.execute('INSERT INTO connection_leases VALUES(?,?,?,?)', (lease_id, member_id, kind, now + 15))
    return True


def renew_lease(conn, lease_id):
    now = clock(conn)
    cursor = conn.execute('UPDATE connection_leases SET expires_at=? WHERE lease_id=? AND expires_at>?', (now + 15, lease_id, now))
    return cursor.rowcount == 1


def release_lease(conn, lease_id):
    conn.execute('DELETE FROM connection_leases WHERE lease_id=?', (lease_id,))


def stream_count(conn):
    return conn.execute("SELECT count(*) FROM connection_leases WHERE kind='sse' AND expires_at>?", (clock(conn),)).fetchone()[0]


def test_database(admin_url, name, create=True):
    """Isolated integration databases only; never drop or truncate the application's database."""
    import re
    import psycopg
    from urllib.parse import urlsplit, urlunsplit
    if not re.fullmatch(r'ar_test_[a-f0-9]{12}', name):
        raise ValueError('Invalid test database name')
    conn = connect(admin_url)
    try:
        if create:
            conn.raw.execute(psycopg.sql.SQL('CREATE DATABASE {}').format(psycopg.sql.Identifier(name)))
        else:
            conn.raw.execute(psycopg.sql.SQL('DROP DATABASE {} WITH (FORCE)').format(psycopg.sql.Identifier(name)))
    finally:
        conn.close()
    url = urlsplit(admin_url)
    return urlunsplit(url._replace(path='/' + name))

"""Transactional metadata-only deliveries; no network I/O while holding database locks."""
import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import math
import secrets
import socket
from datetime import timedelta
from uuid import uuid4

import httpx
from cryptography.fernet import Fernet

from . import db, providers, outbound
from .errors import APIError
from .signing import canonical_json

TOPIC = 'agent-room-webhooks'
MAX_ATTEMPTS = 8
log = logging.getLogger('agent_room')


def cipher(settings):
    key = hmac.new(settings.signing_key, b'agent-room-webhook-secrets-v1', hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def endpoint(value):
    try:
        url = httpx.URL(value)
        if (len(value) > 2048 or url.scheme != 'https' or not url.host or url.port not in {None, 443}
                or url.userinfo or url.fragment or '\\' in value or any(ord(c) < 33 for c in value)):
            raise ValueError()
        host = url.host.rstrip('.').lower()
        if host == 'localhost' or host.endswith(('.localhost', '.local', '.internal')):
            raise ValueError()
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            if '.' not in host:
                raise ValueError()
        else:
            if not public_address(address):
                raise ValueError()
        return str(url)
    except (ValueError, httpx.InvalidURL):
        raise APIError(422, 'invalid_webhook_url', 'Use a public HTTPS endpoint on port 443, without credentials or fragments.') from None


def public_address(address):
    # Explicit flags also reject multicast and IPv4-mapped private IPv6 addresses.
    mapped = getattr(address, 'ipv4_mapped', None)
    return (address.is_global and not address.is_multicast and not address.is_reserved
            and not address.is_unspecified and (mapped is None or public_address(mapped)))


async def destination(value):
    url = httpx.URL(endpoint(value))
    records = await asyncio.wait_for(asyncio.get_running_loop().getaddrinfo(
        url.host, 443, type=socket.SOCK_STREAM), timeout=3)
    addresses = [ipaddress.ip_address(record[4][0]) for record in records]
    if not addresses or any(not public_address(address) for address in addresses):
        raise APIError(422, 'destination_blocked')
    # Pin the validated address for the actual connection, preserving Host and TLS SNI.
    # This avoids a second DNS lookup and DNS-rebinding access to private networks.
    return url.copy_with(host=str(addresses[0])), url.host


def actor(conn, member_id, granted_scopes=None):
    member = db.member(conn, member_id)
    scopes = (db.role_scopes(conn, member['role']) or '').split() if member else []
    if granted_scopes is not None:
        scopes = set(scopes) & set(granted_scopes.split())
    if not member or member['status'] != 'active' or 'room:read' not in scopes:
        return None
    return {**member, 'can_negotiate': 'negotiate' in scopes}


def subscriptions(conn, member_id=None):
    sql = """SELECT s.subscription_id,s.member_id,s.url,s.types,s.thread_id,s.enabled,s.created_at,s.expires_at,
        CASE WHEN o.subscription_id IS NOT NULL THEN 'muse_outbound' ELSE coalesce(p.provider,'agentroom') END AS provider FROM webhook_subscriptions s
        LEFT JOIN webhook_provider_credentials p ON p.subscription_id=s.subscription_id
        LEFT JOIN outbound_connectors o ON o.subscription_id=s.subscription_id"""
    rows = db.rows(conn, sql + ' WHERE s.deleted_at IS NULL' + (' AND s.member_id=?' if member_id else '') + ' ORDER BY s.created_at', (member_id,) if member_id else ())
    return [{**row, 'types': json.loads(row['types']), 'enabled': bool(row['enabled'])} for row in rows]


def create(conn, settings, member_id, url, types, thread_id=None, scopes=None, session_id=None, *, provider='agentroom', authorization=None):
    if not actor(conn, member_id):
        raise APIError(404, 'not_found')
    count = conn.execute('SELECT count(*) FROM webhook_subscriptions WHERE member_id=? AND deleted_at IS NULL', (member_id,)).fetchone()[0]
    if count >= 3:
        raise APIError(409, 'webhook_limit', 'Each member can have up to three endpoints. Delete an unused endpoint first.')
    url = endpoint(url)
    if provider == 'grok':
        authorization = providers.grok_credentials(url, authorization or '')
    elif provider != 'agentroom' or authorization is not None:
        raise APIError(422, 'invalid_provider_credentials')
    secret, sid = 'arwh_' + secrets.token_urlsafe(32), str(uuid4())
    scopes = scopes if scopes is not None else db.role_scopes(conn, db.member(conn, member_id)['role'])
    conn.execute('INSERT INTO webhook_subscriptions(subscription_id,member_id,url,secret,types,thread_id,created_at,scopes,session_id,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                 (sid, member_id, url, cipher(settings).encrypt(secret.encode()).decode(), canonical_json(types), thread_id,
                  db.stamp(), scopes, session_id, db.stamp(db.utcnow() + timedelta(days=30))))
    if provider == 'grok':
        conn.execute('INSERT INTO webhook_provider_credentials VALUES(?,?,?)',
                     (sid, provider, providers.cipher(settings).encrypt(authorization.encode()).decode()))
        conn.execute('UPDATE connector_watchdogs SET server_confirmed_at=NULL WHERE member_id=?',(member_id,))
        return {'subscription_id': sid, 'provider': provider, 'status': 'configured'}
    return {'subscription_id': sid, 'secret': secret}


def enqueue(conn, subscription, payload, seq=None):
    did = str(uuid4())
    payload = {**payload, 'event_id': did}
    conn.execute("""INSERT INTO webhook_deliveries(delivery_id,subscription_id,generation,message_seq,payload,status,next_at,created_at)
        VALUES(?,?,?,?,?,'pending',?,?) ON CONFLICT(subscription_id,message_seq) DO NOTHING""",
        (did, subscription['subscription_id'], subscription['generation'], seq, canonical_json(payload), db.stamp(), db.stamp()))
    if outbound.is_outbound(conn, subscription['subscription_id']):
        outbound.signal(conn, subscription['member_id'])
    return did


def enqueue_message(conn, message):
    for sub in db.rows(conn, 'SELECT * FROM webhook_subscriptions WHERE enabled=1'):
        if sub['member_id'] == message['from']:
            continue
        if message['type'] not in json.loads(sub['types']):
            continue
        if sub['thread_id'] and sub['thread_id'] != message['thread_id']:
            continue
        recipient = subscription_actor(conn, sub)
        if not recipient or not db.visible_message(conn, recipient, message['seq']):
            continue
        did = enqueue(conn, sub, {'type': 'message.available', 'occurred_at': message['received_at'],
            'member_id': sub['member_id'], 'message': {k: message[k] for k in ['seq', 'id', 'type', 'thread_id']}}, message['seq'])
        from . import watchdog
        watchdog.track(conn, sub, message, did)


def subscription_actor(conn, sub):
    if sub['expires_at'] <= db.stamp():
        return None
    if sub['session_id']:
        session = db.session(conn, sub['session_id'])
        if not session or session['revoked_at'] or session['expires_at'] <= db.stamp():
            return None
    return actor(conn, sub['member_id'], sub['scopes'])


def valid(conn, row):
    sub = db.one(conn, 'SELECT * FROM webhook_subscriptions WHERE subscription_id=?', (row['subscription_id'],))
    if not sub or not sub['enabled'] or sub['deleted_at'] or sub['generation'] != row['generation']:
        return None
    recipient = subscription_actor(conn, sub)
    if not recipient or (row['message_seq'] is not None and not db.visible_message(conn, recipient, row['message_seq'])):
        return None
    provider = db.one(conn, 'SELECT provider,credential AS provider_credential FROM webhook_provider_credentials WHERE subscription_id=?', (sub['subscription_id'],))
    return {**sub, **(provider or {}), **({'provider':'muse_outbound'} if outbound.is_outbound(conn,sub['subscription_id']) else {})}


async def post(settings, sub, payload):
    sub = dict(sub)
    if sub.get('provider') == 'grok':
        # Validate again at delivery so a stored credential can never be forwarded
        # to a changed host, redirect, query string or unrelated provider path.
        authorization = providers.grok_credentials(endpoint(sub['url']), providers.cipher(settings).decrypt(sub['provider_credential'].encode()).decode())
    else:
        authorization = None
    pinned, hostname = await destination(sub['url'])
    body, timestamp = payload.encode(), str(int(db.utcnow().timestamp()))
    signed = timestamp.encode() + b'.' + body
    headers = {'Content-Type': 'application/json', 'Host': hostname, 'X-AR-Event-Id': json.loads(payload)['event_id']}
    if authorization:
        headers['Authorization'] = authorization
    else:
        secret = cipher(settings).decrypt(sub['secret'].encode())
        headers.update({'X-AR-Timestamp': timestamp,
                        'X-AR-Signature': 'sha256=' + hmac.new(secret, signed, hashlib.sha256).hexdigest()})
    if not authorization and sub['previous_secret'] and sub['previous_until'] > db.stamp():
        previous = cipher(settings).decrypt(sub['previous_secret'].encode())
        headers['X-AR-Previous-Signature'] = 'sha256=' + hmac.new(previous, signed, hashlib.sha256).hexdigest()
    async with httpx.AsyncClient(timeout=4, trust_env=False, follow_redirects=False) as client:
        async with client.stream('POST', pinned, content=body, headers=headers,
                                 extensions={'sni_hostname': hostname}) as response:
            return response.status_code  # Never read or persist an untrusted response body.


async def deliver(settings, delivery_id):
    conn = db.connect(db.target(settings))
    lease = str(uuid4())
    try:
        with db.transaction(conn, ('webhook_delivery', delivery_id)):
            row = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=?', (delivery_id,))
            if not row or row['status'] != 'pending':
                return
            now = db.stamp()
            if row['next_at'] > now or (row['lease_until'] and row['lease_until'] > now):
                return {**row, '_waiting': True}
            sub = valid(conn, row)
            if not sub:
                conn.execute("UPDATE webhook_deliveries SET status='cancelled',last_error='authorization_changed',completed_at=? WHERE delivery_id=?", (now, delivery_id))
                return
            if sub.get('provider') == 'muse_outbound':
                outbound.signal(conn,sub['member_id'])
                return
            from . import watchdog
            gate = watchdog.before_delivery(conn, row)
            if gate == 'handling_resolved':
                conn.execute("UPDATE webhook_deliveries SET status='cancelled',last_error=?,completed_at=? WHERE delivery_id=?", (gate,now,delivery_id))
                return
            if gate:
                conn.execute('UPDATE webhook_deliveries SET next_at=? WHERE delivery_id=?', (gate,delivery_id))
                return {**row,'next_at':gate,'_waiting':True}
            if row['attempts'] >= MAX_ATTEMPTS:
                conn.execute("UPDATE webhook_deliveries SET status='failed',last_error='attempt_limit',completed_at=? WHERE delivery_id=?", (now, delivery_id))
                return
            conn.execute('UPDATE webhook_deliveries SET attempts=attempts+1,lease_id=?,lease_until=? WHERE delivery_id=?',
                         (lease, db.stamp(db.utcnow() + timedelta(seconds=45)), delivery_id))
            attempt = row['attempts'] + 1
        code, error = None, None
        try:
            # Recheck current grant after claiming; an in-flight HTTP request cannot be recalled.
            if not valid(conn, row):
                error = 'authorization_changed'
            else:
                # A receipt may arrive after the claim; suppress a now-empty recovery.
                gate = watchdog.before_delivery(conn, row)
                if gate:
                    error = 'handling_resolved' if gate == 'handling_resolved' else 'agent_busy'
                else:
                    code = await asyncio.wait_for(post(settings, sub, row['payload']), timeout=10)
                    if not 200 <= code < 300:
                        error = 'http_error'
        except APIError:
            error = 'destination_blocked'
        except Exception:
            error = 'network_error'
        if error == 'agent_busy':
            with db.transaction(conn, ('webhook_delivery', delivery_id)):
                conn.execute('''UPDATE webhook_deliveries SET attempts=attempts-1,lease_id=NULL,lease_until=NULL,
                    next_at=? WHERE delivery_id=? AND lease_id=?''',(gate,delivery_id,lease))
            pending = db.one(conn, "SELECT * FROM webhook_deliveries WHERE delivery_id=? AND status='pending'", (delivery_id,))
            return {**pending,'_waiting':True} if pending else None
        success = code is not None and 200 <= code < 300
        status = 'delivered' if success else ('cancelled' if error in {'authorization_changed','handling_resolved'} else ('failed' if attempt >= MAX_ATTEMPTS else 'pending'))
        with db.transaction(conn, ('webhook_delivery', delivery_id)):
            conn.execute('''UPDATE webhook_deliveries SET status=?,last_status=?,last_error=?,lease_id=NULL,lease_until=NULL,
                next_at=?,completed_at=? WHERE delivery_id=? AND lease_id=?''',
                (status, code, error, db.stamp(db.utcnow() + timedelta(seconds=min(300, 5 * 2 ** attempt))),
                 db.stamp() if status != 'pending' else None, delivery_id, lease))
            conn.execute('INSERT INTO webhook_attempts VALUES(?,?,?,?,?,?)',
                         (str(uuid4()), delivery_id, attempt, db.stamp(), code, 'delivered' if success else error))
            db.audit(conn, 'room', 'webhook.' + status, delivery_id, detail=str(code) if code else error)
        return db.one(conn, "SELECT * FROM webhook_deliveries WHERE delivery_id=? AND status='pending'", (delivery_id,))
    finally:
        conn.close()


async def dispatch(settings, row, recovery_id=None):
    conn = db.connect(db.target(settings))
    try:
        sub = outbound.is_outbound(conn,row['subscription_id'])
        if sub:
            outbound.signal(conn,sub['member_id'])
            return
    finally:
        conn.close()
    from vercel.queue import QueueClient
    due = max(db.parse(row['next_at']), db.parse(row['lease_until']) if row['lease_until'] else db.utcnow())
    delay = max(0, math.ceil((due - db.utcnow()).total_seconds()))
    await QueueClient(region='iad1', timeout=5).send(TOPIC, {'delivery_id': row['delivery_id']},
        idempotency_key=f"{row['delivery_id']}:{row['cycle']}:{row['generation']}:{row['attempts']}" + (':recovery:'+recovery_id if recovery_id else ''),
        retention=timedelta(days=7), delay=delay)
    conn = db.connect(db.target(settings))
    try:
        conn.execute('UPDATE webhook_deliveries SET enqueued_attempt=?,enqueued_at=? WHERE delivery_id=? AND cycle=? AND attempts=?',
                     (row['attempts'], db.stamp(), row['delivery_id'], row['cycle'], row['attempts']))
        if recovery_id:
            conn.execute('''INSERT INTO webhook_recoveries(delivery_id,recovery_id,created_at)
                SELECT delivery_id,?,? FROM webhook_deliveries WHERE delivery_id=?
                ON CONFLICT(delivery_id,recovery_id) DO NOTHING''',(recovery_id,db.stamp(),row['delivery_id']))
    finally:
        conn.close()


def pending_rows(settings, force=False, recovery_id=None):
    conn = db.connect(db.target(settings))
    try:
        if force and recovery_id:
            return db.rows(conn,"""SELECT d.* FROM webhook_deliveries d WHERE status='pending'
                AND NOT EXISTS(SELECT 1 FROM outbound_connectors o WHERE o.subscription_id=d.subscription_id)
                AND NOT EXISTS(SELECT 1 FROM webhook_recoveries r WHERE r.delivery_id=d.delivery_id AND r.recovery_id=?)
                ORDER BY next_at,delivery_id LIMIT 30""",(recovery_id,))
        return db.rows(conn, "SELECT * FROM webhook_deliveries d WHERE status='pending' AND NOT EXISTS(SELECT 1 FROM outbound_connectors o WHERE o.subscription_id=d.subscription_id)" +
            ('' if force else ' AND enqueued_attempt<attempts') + ' ORDER BY next_at,delivery_id LIMIT 30')
    finally:
        conn.close()


async def kick(settings, force=False, strict=False, recovery_id=None):
    if settings.profile == 'sandbox':
        return
    """Publish a small wakeup, not one API call per recipient in the user request."""
    try:
        recovery_id = (recovery_id or str(uuid4())) if force else None
        rows = pending_rows(settings, force, recovery_id)
    except Exception:
        if strict:
            raise
        log.warning('webhook_outbox_unavailable')
        return
    if not rows:
        return
    if settings.serverless:
        from vercel.queue import QueueClient
        fingerprint = hashlib.sha256(canonical_json([[r['delivery_id'], r['cycle'], r['generation'], r['attempts']] for r in rows]).encode()).hexdigest()
        try:
            await QueueClient(region='iad1', timeout=5).send(TOPIC, {'drain': True, 'force': force, 'recovery_id':recovery_id},
                idempotency_key='outbox-' + fingerprint + ('-'+recovery_id if force else ''), retention=timedelta(days=7))
        except Exception:
            if strict:
                raise
            log.warning('webhook_queue_publish_failed')
        return
    for row in rows:
        try:
            await deliver(settings, row['delivery_id'])
        except Exception:
            log.warning('webhook_queue_publish_failed')  # The durable row remains available for recovery.


async def pump(settings, force=False, recovery_id=None):
    """Queue fan-out in bounded batches; a durable continuation drains every batch."""
    recovery_id=(recovery_id or str(uuid4())) if force else None
    rows = pending_rows(settings, force, recovery_id)
    semaphore = asyncio.Semaphore(4)
    async def publish(row):
        async with semaphore:
            await dispatch(settings, row, recovery_id=recovery_id)
    await asyncio.gather(*(publish(row) for row in rows))
    # Any failure raises so the current queue message is retried. Continuations are
    # keyed to remaining work, not the first batch, avoiding a stranded fan-out tail.
    await kick(settings, force=force, strict=True, recovery_id=recovery_id)


async def consume(settings, delivery_id):
    pending = await deliver(settings, delivery_id)
    if pending:
        if pending.get('_waiting'):
            # Republishing the same attempt's idempotency key could be deduplicated
            # against this callback, then acknowledged too early. Retry the callback.
            raise RuntimeError('Delivery is not available yet; retry this queue callback')
        # A failed queue publish raises: the original queue message is then redelivered.
        await dispatch(settings, pending)

import json
from datetime import timedelta
from uuid import uuid4

from . import db
from .errors import APIError
from .signing import canonical_json, digest, sign


def server_message(conn, settings, kind, body, recipients=None, thread_id=None):
    mid, now = str(uuid4()), db.stamp()
    message = {'id': mid, 'from': 'room', 'ts': now, 'type': kind, 'thread_id': thread_id or mid,
               'body': body, 'received_at': now}
    if recipients:
        message['to'] = sorted(recipients)
    message['sig'] = sign(message, settings.signing_key)
    return db.insert_message(conn, message)


def expire_due(conn, settings):
    messages = []
    for negotiation in db.due_negotiations(conn):
        nid = negotiation['negotiation_id']
        db.close_negotiation(conn, nid, 'expired')
        messages.append(server_message(conn, settings, 'expire', {'negotiation_id': nid,
                        'version': negotiation['current_version'], 'expired_at': db.stamp()}, db.parties(conn, nid), nid))
        db.audit(conn, 'room', 'negotiation.expired', nid)
    return messages


def expiry(body):
    now = db.utcnow()
    when = db.parse(body['expires_at']) if body.get('expires_at') else now + timedelta(hours=72)
    if when <= now or when > now + timedelta(days=30):
        raise APIError(400, 'invalid_request', 'expires_at must be in the future and at most 30 days away')
    return db.stamp(when)


def validate(conn, actor, message):
    """Validate everything before persisting; called under BEGIN IMMEDIATE."""
    kind, body, mid = message['type'], message['body'], actor['member_id']
    if kind == 'propose':
        if message['thread_id'] != message['id'] or not 1 <= len(message.get('to', [])) <= 7:
            raise APIError(400, 'bad_recipients', 'Proposals need a new thread and 1–7 other parties')
        if db.negotiation(conn, message['id']):
            raise APIError(409, 'id_conflict')
        return {'nid': message['id'], 'version': 1, 'expiry': expiry(body)}
    nid = body['negotiation_id']
    negotiation = db.negotiation(conn, nid)
    if not negotiation or mid not in db.parties(conn, nid):
        raise APIError(404, 'not_found')
    if negotiation['state'] != 'open':
        raise APIError(409, 'negotiation_closed')
    others = set(db.parties(conn, nid)) - {mid}
    if message['thread_id'] != nid or set(message.get('to', [])) != others:
        raise APIError(400, 'bad_recipients')
    number = negotiation['current_version']
    version = db.version(conn, nid, number)
    if kind == 'counter' and body['base_version'] != number or kind in {'accept', 'decline'} and body['version'] != number:
        raise APIError(409, 'stale_version', f'Current version is {number}')
    if kind == 'accept' and body['terms_hash'] != version['terms_hash']:
        raise APIError(409, 'stale_version', 'Terms hash does not match the current version')
    return {'nid': nid, 'version': number + (kind == 'counter'), 'expiry': expiry(body) if kind == 'counter' else None}


def apply(conn, settings, actor, message, context):
    """Persist state and the exactly-once commit in the message transaction."""
    kind, body, mid = message['type'], message['body'], actor['member_id']
    nid, number = context['nid'], context['version']
    if kind == 'propose':
        db.create_negotiation(conn, nid, body['subject'], mid, message['to'])
    if kind in {'propose', 'counter'}:
        db.store_version(conn, nid, number, body['terms'], digest(canonical_json(body['terms'])), mid,
                         message['seq'], context['expiry'])
    elif kind == 'accept':
        db.acceptance(conn, nid, number, mid, message['seq'])
        parties = db.parties(conn, nid)
        accepts = db.acceptances(conn, nid, number)
        if {a['member_id'] for a in accepts} == set(parties):
            version = db.version(conn, nid, number)
            negotiation = db.negotiation(conn, nid)
            db.close_negotiation(conn, nid, 'committed')
            commit = server_message(conn, settings, 'commit', {
                'negotiation_id': nid, 'version': number, 'subject': negotiation['subject'],
                'terms': json.loads(version['terms']), 'terms_hash': version['terms_hash'], 'parties': parties,
                'acceptances': [a['message_id'] for a in accepts], 'committed_at': db.stamp()}, parties, nid)
            db.save_commit(conn, nid, number, version['terms_hash'], commit)
            db.audit(conn, 'room', 'negotiation.committed', nid)
            return [commit]
    elif kind in {'decline', 'withdraw'}:
        db.close_negotiation(conn, nid, 'declined' if kind == 'decline' else 'withdrawn')
    return []

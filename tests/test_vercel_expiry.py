from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import patch

from app import db
from .test_negotiation import action, propose


def test_expiry_under_accept_write_lock_is_committed(client, enroll, send, conn):
    a, b = enroll(), enroll()
    nid = propose(send, a, b)['id']
    detail = client.get('/v1/negotiations/' + nid, headers=b['headers']).json()
    original = db.transaction
    count = 0
    @contextmanager
    def delayed(connection):
        nonlocal count
        count += 1
        # First transaction does lazy expiry. Simulate another instance holding the next write lock.
        if count == 2:
            connection.execute('UPDATE negotiation_versions SET expires_at=? WHERE negotiation_id=?',
                               (db.stamp(db.utcnow() - timedelta(seconds=1)), nid))
        with original(connection):
            yield
    with patch('app.routes.members.db.transaction', delayed):
        result = action(send, b, a, nid, 'accept', {'version': 1, 'terms_hash': detail['versions'][0]['terms_hash']})
    assert result.status_code == 409, result.text
    assert db.negotiation(conn, nid)['state'] == 'expired'
    assert db.commit_message(conn, nid) is None
    assert len([m for m in client.get('/v1/messages', headers=b['headers']).json()['items'] if m['type'] == 'expire']) == 1

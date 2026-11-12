import sqlite3
from datetime import timedelta

from app import db
from app.main import create_app
from fastapi.testclient import TestClient


def test_backup_restore_and_restart(client, enroll, send, settings, tmp_path):
    a = enroll()
    sent = send(a, body={'text': 'survives restart'}).json()
    db.backup(settings.db_path)
    # Verify a live SQLite backup, including data committed after startup's first backup.
    backup = settings.db_path.parent / 'backups' / f'room-{db.utcnow():%Y%m%d}.db'
    backup.unlink()
    db.backup(settings.db_path)
    with sqlite3.connect(backup) as restored:
        assert restored.execute('SELECT id FROM messages WHERE seq=?', (sent['seq'],)).fetchone()[0] == sent['id']
        assert restored.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    assert settings.db_path.stat().st_mode & 0o777 == 0o600
    assert settings.db_path.parent.stat().st_mode & 0o777 == 0o700
    with TestClient(create_app(settings)) as restarted:
        assert restarted.get('/v1/messages/' + str(sent['seq']), headers=a['headers']).json()['body']['text'] == 'survives restart'


def test_retention_does_not_reuse_sequence(client, enroll, send, conn):
    a = enroll()
    sent = send(a).json()
    conn.execute('UPDATE messages SET received_at=? WHERE seq=?', (db.stamp(db.utcnow() - timedelta(days=31)), sent['seq']))
    with db.transaction(conn):
        db.cleanup(conn, 30)
    assert conn.execute('SELECT 1 FROM messages WHERE seq=?', (sent['seq'],)).fetchone() is None
    assert send(a).json()['seq'] > sent['seq']


def test_version_one_migration_preserves_pickup(settings):
    db.initialize(settings.db_path)
    conn = db.connect(settings.db_path)
    try:
        conn.execute('DROP TABLE pickup_receipts')
        conn.execute('UPDATE schema_version SET version=1')
    finally:
        conn.close()
    db.initialize(settings.db_path)
    conn = db.connect(settings.db_path)
    try:
        assert db.ready(conn)
        assert conn.execute('SELECT count(*) FROM pickup_receipts').fetchone()[0] == 0
    finally:
        conn.close()

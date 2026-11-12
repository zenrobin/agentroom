import json
from datetime import timedelta

import httpx

from app import connectors, db
from clients import room_client as cli
from tests.test_listener import credentials
from tests.test_providers import grok
from tests.conftest import message


def receipt(item, outcome='handled'):
    return {'seq': item['seq'], 'message_id': item['id'], 'outcome': outcome}


def test_receipt_requires_exact_visible_saved_message_and_never_acks(client, enroll, send, conn):
    a, b, outsider = enroll(), enroll(), enroll()
    item = send(a, to=[b['member_id']]).json()
    payload = {'receipts': [receipt(item)]}
    assert client.post('/v1/connector/receipts', headers=b['headers'], json=payload).status_code == 409
    assert db.cursor(conn, b['member_id']) == 0
    hidden = client.post('/v1/connector/receipts', headers=outsider['headers'], json=payload)
    assert hidden.json() == {'accepted': [], 'unavailable': [item['seq']]}
    assert not db.one(conn, 'SELECT * FROM processing_receipts')
    client.post('/v1/inbox/ack', headers=b['headers'], json={'seq': item['seq']})
    wrong = {'receipts': [{**receipt(item), 'message_id': message()['id']}]}
    assert client.post('/v1/connector/receipts', headers=b['headers'], json=wrong).status_code == 409
    assert client.post('/v1/connector/receipts', headers=b['headers'], json=payload).json()['accepted'] == [item['seq']]
    first = db.one(conn, 'SELECT * FROM processing_receipts')
    assert first['member_id'] == b['member_id']
    client.post('/v1/connector/receipts', headers=b['headers'], json=payload)
    assert db.one(conn, 'SELECT * FROM processing_receipts') == first


def test_processing_outcomes_upgrade_without_downgrading_and_cleanup_with_message(client, enroll, send, conn):
    a, b = enroll(), enroll()
    item = send(a, to=[b['member_id']]).json()
    client.post('/v1/inbox/ack', headers=b['headers'], json={'seq': item['seq']})
    for outcome in ['awaiting_approval', 'handled', 'awaiting_approval']:
        response = client.post('/v1/connector/receipts', headers=b['headers'], json={'receipts': [receipt(item, outcome)]})
        assert response.status_code == 200
    assert db.one(conn, 'SELECT outcome FROM processing_receipts')['outcome'] == 'handled'
    conn.execute('DELETE FROM messages WHERE seq=?', (item['seq'],))
    assert not db.one(conn, 'SELECT * FROM processing_receipts')


def test_connector_separates_wake_acceptance_from_processing_and_recovery(client, enroll, send, conn, owner):
    a, b = enroll(), enroll()
    sub = grok(client, b)
    assert connectors.status(conn, b['member_id'])['state'] == 'runtime_setup_needed'
    ready = {'provider': 'grok', 'recovery_interval_s': 300, 'recovery_verified_at': db.stamp()}
    assert client.post('/v1/connector/ready', headers=b['headers'], json=ready).status_code == 200
    item = send(a, to=[b['member_id']]).json()
    conn.execute("UPDATE webhook_deliveries SET status='delivered',last_status=200,completed_at=? WHERE subscription_id=?", (db.stamp(), sub['subscription_id']))
    assert connectors.status(conn, b['member_id'])['state'] == 'wake_accepted'
    client.post('/v1/inbox/ack', headers=b['headers'], json={'seq': item['seq']})
    assert connectors.status(conn, b['member_id'])['state'] == 'wake_accepted'
    client.post('/v1/connector/receipts', headers=b['headers'], json={'receipts': [receipt(item)]})
    assert connectors.status(conn, b['member_id'])['state'] == 'processing_reported'
    client.post('/v1/owner/members/' + b['member_id'] + '/revoke', headers=owner, json={})
    assert not connectors.status(conn, b['member_id'])['runtime']
    assert client.get('/v1/connector/status', headers=b['headers']).status_code == 401


def test_connection_test_only_reports_handling_for_that_exact_message(client, enroll, owner, conn):
    b = enroll()
    response = client.post('/v1/owner/members/' + b['member_id'] + '/test', headers=owner, json={})
    assert response.status_code == 201
    item = client.get('/v1/inbox', headers=b['headers']).json()['items'][-1]
    client.post('/v1/inbox/ack', headers=b['headers'], json={'seq': item['seq']})
    overview = client.get('/v1/owner/overview', headers=owner).json()
    assert overview['members'][0]['connection_check']['status'] == 'acknowledged'
    client.post('/v1/connector/receipts', headers=b['headers'], json={'receipts': [receipt(item, 'awaiting_approval')]})
    overview = client.get('/v1/owner/overview', headers=owner).json()
    assert overview['members'][0]['connection_check']['status'] == 'agent_reported_awaiting_approval'


def test_connector_rejects_unverified_time_and_false_types(client, enroll):
    b = enroll()
    for stamp in [db.stamp(db.utcnow() - timedelta(days=2)), db.stamp(db.utcnow() + timedelta(days=1)), '2026-01-01T01:00:00']:
        response = client.post('/v1/connector/ready', headers=b['headers'], json={
            'provider': 'grok', 'recovery_interval_s': 300, 'recovery_verified_at': stamp})
        assert response.status_code == 422


def test_local_processing_report_survives_offline_restart_and_concurrent_update(tmp_path):
    creds = credentials(tmp_path, {'member_id': 'a', 'access_token': 'access', 'refresh_token': 'refresh'}, 'https://room.test')
    path = creds.path.parent / 'inbox.sqlite3'
    store = cli.InboxStore(path, 'https://room.test', 'a')
    item = {**message(), 'seq': 4}
    store.save([item]); store.complete(seq=4, awaiting_approval=True)
    def offline(request):
        raise httpx.ConnectError('offline')
    agent = cli.RoomClient(creds, transport=httpx.MockTransport(offline))
    assert not agent.report_handling(store)
    assert store.pending() == [] and len(store.reports()) == 1
    agent.close(); store.close()
    store = cli.InboxStore(path, 'https://room.test', 'a')
    def online(request):
        assert request.url.path == '/v1/connector/receipts'
        sent = json.loads(request.content)['receipts']
        assert sent[0]['outcome'] == 'awaiting_approval'
        store.complete(seq=4)  # Human approval resolves while this report is in flight.
        return httpx.Response(200, json={'accepted': [4], 'unavailable': []})
    agent = cli.RoomClient(creds, transport=httpx.MockTransport(online))
    assert not agent.report_handling(store)
    assert store.reports()[0]['outcome'] == 'handled'
    agent.close()
    agent = cli.RoomClient(creds, transport=httpx.MockTransport(lambda _: httpx.Response(200, json={'accepted': [4], 'unavailable': []})))
    assert agent.report_handling(store)
    store.complete(seq=4, awaiting_approval=True)
    assert store.reports() == []
    agent.close(); store.close()


def test_connector_migration_preserves_existing_room(client, enroll, settings, conn):
    b = enroll()
    before = db.one(conn, 'SELECT count(*) AS n FROM messages')['n']
    for table in ['connector_runtime', 'processing_receipts', 'connector_version']:
        conn.execute('DROP TABLE ' + table)
    assert not db.ready(conn)
    db.initialize(settings.db_path); db.initialize(settings.db_path)
    assert db.ready(conn) and db.member(conn, b['member_id'])
    assert db.one(conn, 'SELECT count(*) AS n FROM messages')['n'] == before


def test_owner_renew_preserves_credentials_and_scopes_but_cannot_revive_recovery(client, enroll, owner, conn):
    from tests.test_providers import KEY, ROUTINE
    b = enroll()
    created = client.post('/v1/owner/webhooks', headers=owner, json={
        'member_id': b['member_id'], 'provider': 'grok', 'url': ROUTINE, 'authorization': KEY}).json()
    sid = created['subscription_id']
    conn.execute('UPDATE webhook_subscriptions SET expires_at=? WHERE subscription_id=?', (db.stamp(db.utcnow()-timedelta(days=1)), sid))
    before = db.one(conn, 'SELECT * FROM webhook_subscriptions WHERE subscription_id=?', (sid,))
    credential = db.one(conn, 'SELECT * FROM webhook_provider_credentials WHERE subscription_id=?', (sid,))
    path = '/v1/owner/webhooks/' + sid + '/renew'
    assert client.post(path, headers=b['headers'], json={}).status_code == 401
    response = client.post(path, headers=owner, json={})
    assert response.status_code == 200
    after = db.one(conn, 'SELECT * FROM webhook_subscriptions WHERE subscription_id=?', (sid,))
    assert after['expires_at'] > db.stamp()
    assert {k: v for k,v in after.items() if k!='expires_at'} == {k: v for k,v in before.items() if k!='expires_at'}
    assert db.one(conn, 'SELECT * FROM webhook_provider_credentials WHERE subscription_id=?', (sid,)) == credential
    with db.transaction(conn):
        db.revoke_member(conn, b['member_id'])
    assert client.post(path, headers=owner, json={}).status_code == 409

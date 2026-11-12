import time
from dataclasses import replace

from fastapi.testclient import TestClient

from app import db, sandbox
from app.main import create_app
from tests.conftest import message
from tests.test_sandbox import admit


INSTRUCTIONS = 'Read the product schema. Send identifiers only. Do not put amounts or documents in room messages.'


def document(**enforced):
    return {'schema': 'agentroom.rules/1', 'instructions': INSTRUCTIONS, 'enforced': enforced}


def test_absent_rules_preserve_current_send_behavior(client, enroll, send):
    actor = enroll()
    assert client.get('/v1/room').json()['rules_version'] is None
    assert client.get('/v1/rules', headers=actor['headers']).json()['version'] is None
    assert send(actor).status_code == 201


def test_owner_publishes_versioned_rules_and_members_must_acknowledge(client, enroll, owner, send):
    actor = enroll()
    assert client.post('/v1/owner/rules', headers=actor['headers'], json=document()).status_code == 401
    published = client.post('/v1/owner/rules', headers=owner, json=document(
        message_types=['chat', 'event'], require_acknowledgement=True, identifier_values=True, max_text_length=20))
    assert published.status_code == 201, published.text
    assert published.json()['version'] == 1
    assert client.get('/v1/room').json()['rules_version'] == 1
    blocked = send(actor)
    assert blocked.status_code == 403 and blocked.json()['error']['code'] == 'rules_unacknowledged'
    assert client.post('/v1/rules/ack', headers=actor['headers'], json={'version': 2}).json()['error']['code'] == 'rules_version'
    assert client.post('/v1/rules/ack', headers=actor['headers'], json={'version': 1}).status_code == 200
    assert client.post('/v1/rules/ack', headers=actor['headers'], json={'version': 1}).status_code == 200
    assert send(actor, body={'text': 'short'}).status_code == 201
    assert send(actor, body={'text': 'this chat is too long'}).json()['error']['code'] == 'rule_violation'
    prose = send(actor, 'event', {'name': 'packet.approved', 'data': {'note': 'Please send the full balance'}})
    assert prose.status_code == 403 and prose.json()['error']['code'] == 'rule_violation'
    assert send(actor, 'event', {'name': 'packet.approved', 'data': {'packetId': 'pkt_123', 'approvalId': 'apr_123'}}).status_code == 201
    assert send(actor, 'request', {'action': 'share', 'params': {}}).json()['error']['code'] == 'rule_violation'
    same = client.post('/v1/owner/rules', headers=owner, json=document(
        message_types=['chat', 'event'], require_acknowledgement=True, identifier_values=True, max_text_length=20))
    assert same.status_code == 409 and same.json()['error']['code'] == 'rules_unchanged'


def test_new_version_requires_a_fresh_acknowledgement(client, enroll, owner, send):
    actor = enroll()
    client.post('/v1/owner/rules', headers=owner, json=document(require_acknowledgement=True, message_types=['chat']))
    client.post('/v1/rules/ack', headers=actor['headers'], json={'version': 1})
    revised = client.post('/v1/owner/rules', headers=owner, json=document(
        require_acknowledgement=True, message_types=['chat'], max_text_length=100))
    assert revised.json()['version'] == 2
    assert send(actor).json()['error']['code'] == 'rules_unacknowledged'
    client.post('/v1/rules/ack', headers=actor['headers'], json={'version': 2})
    assert send(actor).status_code == 201
    history = client.get('/v1/owner/rules', headers=owner).json()
    assert [item['version'] for item in history['versions']] == [1, 2]
    assert history['active_version'] == 2


def test_rules_do_not_block_owner_connection_tests(client, enroll, owner):
    actor = enroll()
    client.post('/v1/owner/rules', headers=owner, json=document(require_acknowledgement=True, message_types=['event']))
    test = client.post('/v1/owner/members/' + actor['member_id'] + '/test', headers=owner)
    assert test.status_code == 201, test.text


def test_policy_rejections_do_not_lock_the_shared_ip(client, enroll, owner, send):
    now = [time.monotonic()]
    client.app.state.limiter.clock = lambda: now[0]
    actor = enroll()
    client.post('/v1/owner/rules', headers=owner, json=document(require_acknowledgement=True, message_types=['chat']))
    for index in range(20):
        if index == 10:
            now[0] += 10
        denied = send(actor)
        assert denied.status_code == 403 and denied.json()['error']['code'] == 'rules_unacknowledged'
    acknowledged = client.post('/v1/rules/ack', headers=actor['headers'], json={'version': 1})
    assert acknowledged.status_code == 200, acknowledged.text
    assert client.get('/v1/owner/members', headers=owner).status_code == 200
    now[0] += 10
    assert send(actor).status_code == 201


def test_sandbox_peer_accept_follows_the_active_rules(settings):
    config = replace(settings, profile='sandbox', room_name='AgentRoom Sandbox', webhook_url=None, webhook_secret=None)
    owner = {'Authorization': 'Bearer ' + config.owner_key}
    with TestClient(create_app(config)) as client:
        published = client.post('/v1/owner/rules', headers=owner, json=document(message_types=['propose']))
        assert published.status_code == 201, published.text
        guest = admit(client)
        proposal = message('propose', {'subject': 'Fictional job', 'terms': {'demo': True}}, to=[sandbox.PEER])
        assert client.post('/v1/messages', headers=guest['headers'], json=proposal).status_code == 201
        detail = client.get('/v1/negotiations/' + proposal['id'], headers=guest['headers']).json()
        body = {'version': 1, 'terms_hash': detail['versions'][0]['terms_hash']}
        denied = client.post('/v1/sandbox/peer/accept/' + proposal['id'], headers=guest['headers'], json=body)
        assert denied.status_code == 403 and denied.json()['error']['code'] == 'rule_violation'
        assert client.get('/v1/negotiations/' + proposal['id'], headers=guest['headers']).json()['state'] == 'open'
        allowed = client.post('/v1/owner/rules', headers=owner, json=document(message_types=['propose', 'accept']))
        assert allowed.status_code == 201, allowed.text
        accepted = client.post('/v1/sandbox/peer/accept/' + proposal['id'], headers=guest['headers'], json=body)
        assert accepted.status_code == 200, accepted.text
        assert accepted.json()['commit']['type'] == 'commit'


def test_invalid_rules_are_rejected_and_migration_is_idempotent(client, owner, settings, conn):
    assert client.post('/v1/owner/rules', headers=owner, json={'schema': 'agentroom.rules/1', 'instructions': 'x',
                                                              'enforced': {'message_types': ['system']}}).status_code == 422
    assert client.post('/v1/owner/rules', headers=owner, json={'schema': 'other', 'instructions': 'x'}).status_code == 422
    db.initialize(settings.db_path)
    db.initialize(settings.db_path)
    assert db.room_rules_ready(conn)

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from clients import room_client as cli
from .conftest import live_enroll


def test_cli_join_send_inbox_ack_propose_accept_commit(live, owner, tmp_path, monkeypatch, capsys):
    first = tmp_path / 'a' / 'credentials.json'
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS', str(first))
    real_sleep = time.sleep
    original_init = cli.RoomClient.__init__
    def fast_init(self, credentials, **kwargs):
        return original_init(self, credentials, sleep=lambda _: real_sleep(0.03), **kwargs)
    monkeypatch.setattr(cli.RoomClient, '__init__', fast_init)
    args = ['join', '--url', str(live.base_url).rstrip('/'), '--name', 'cli-a', '--display', 'CLI A', '--owner', 'Human', '--contact', 'test@example.com']
    with ThreadPoolExecutor(1) as pool:
        result = pool.submit(cli.main, args)
        pending = []
        for _ in range(20):
            pending = live.get('/v1/owner/requests', headers=owner).json()
            if pending:
                break
            real_sleep(0.02)
        assert pending
        assert live.post('/v1/owner/requests/' + pending[0]['request_id'] + '/approve', headers=owner).status_code == 200
        assert result.result(timeout=5) == 0
    a = json.loads(first.read_text())
    assert first.stat().st_mode & 0o777 == 0o600
    assert cli.main(['whoami']) == 0
    b = live_enroll(live, owner, 'cli-b')
    assert cli.main(['send', '--to', 'cli-b', '--text', 'from CLI']) == 0
    second = tmp_path / 'b' / 'credentials.json'
    creds = cli.Credentials(second)
    creds.data = {'url': a['url'], 'member_id': b['member_id'], 'access_token': b['access_token'],
                  'refresh_token': b['refresh_token'], 'expires_at': time.time() + 900}
    creds.save()
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS', str(second))
    assert cli.main(['inbox', '--ack']) == 0
    assert live.get('/v1/inbox', headers=b['headers']).json()['items'] == []
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS', str(first))
    assert cli.main(['propose', '--to', 'cli-b', '--subject', 'Pickup', '--terms', '{"day":"Thursday"}']) == 0
    proposals = live.get('/v1/negotiations', headers=b['headers']).json()
    nid = proposals[0]['negotiation_id']
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS', str(second))
    assert cli.main(['accept', nid, '--yes']) == 0
    assert live.get('/v1/commits/' + nid, headers=b['headers']).status_code == 200
    old_refresh = json.loads(second.read_text())['refresh_token']
    data = json.loads(second.read_text())
    data['expires_at'] = 0
    second.write_text(json.dumps(data))
    assert cli.main(['whoami']) == 0
    assert json.loads(second.read_text())['refresh_token'] != old_refresh
    assert 'Confirm these exact terms with your human first' in capsys.readouterr().err


def test_doctor_keeps_working_when_the_room_has_no_rules_route(tmp_path, monkeypatch, capsys):
    def handle(request):
        if request.url.path == '/readyz':
            return httpx.Response(200, json={'ok': True})
        if request.url.path == '/v1/me':
            return httpx.Response(200, json={'member_id': 'm_1', 'agent_name': 'jerry', 'role': 'member'})
        if request.url.path == '/v1/inbox':
            return httpx.Response(200, json={'items': [], 'acked_seq': 0, 'has_more': False})
        if request.url.path == '/v1/rules':
            return httpx.Response(404, json={'error': {'code': 'not_found', 'message': 'not found'}})
        return httpx.Response(500, json={'error': {'code': 'unavailable'}})
    path = tmp_path / 'credentials.json'
    creds = cli.Credentials(path)
    creds.data = {'url': 'https://room.test', 'member_id': 'm_1', 'access_token': 'token',
                  'refresh_token': 'refresh', 'expires_at': time.time() + 900}
    creds.save()
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS', str(path))
    original = cli.RoomClient.__init__
    monkeypatch.setattr(cli.RoomClient, '__init__', lambda self, credentials, **kwargs:
                        original(self, credentials, transport=httpx.MockTransport(handle), **kwargs))
    assert cli.main(['doctor']) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['ready'] is True and report['rules_version'] is None and report['client_version'] == '1.8.0'
    def deny(request):
        if request.url.path == '/v1/rules':
            return httpx.Response(403, json={'error': {'code': 'forbidden', 'message': 'forbidden'}})
        return handle(request)
    monkeypatch.setattr(cli.RoomClient, '__init__', lambda self, credentials, **kwargs:
                        original(self, credentials, transport=httpx.MockTransport(deny), **kwargs))
    assert cli.main(['doctor']) == 3


def test_credentials_permissions_and_uuid7(tmp_path):
    path = tmp_path / 'private' / 'credentials.json'
    credentials = cli.Credentials(path)
    credentials.data = {'url': 'https://room.test'}
    credentials.save()
    assert credentials.load()['url'] == 'https://room.test'
    path.chmod(0o644)
    with pytest.raises(cli.ClientError):
        credentials.load()
    from uuid import UUID
    assert UUID(cli.uuid7()).version == 7


def test_retries_keep_byte_identical_envelope_and_retry_after(tmp_path):
    requests, sleeps = [], []
    def handle(request):
        requests.append(request.content)
        if len(requests) == 1:
            return httpx.Response(429, headers={'Retry-After': '3'}, json={'error': {'code': 'rate_limited'}})
        if len(requests) == 2:
            return httpx.Response(503, json={'error': {'code': 'unavailable'}})
        return httpx.Response(201, json={'ok': True})
    credentials = cli.Credentials(tmp_path / 'credentials.json')
    credentials.data = {'url': 'https://room.test', 'access_token': 'test', 'expires_at': time.time() + 900}
    client = cli.RoomClient(credentials, transport=httpx.MockTransport(handle), sleep=sleeps.append)
    assert client.send('chat', {'text': 'hello'}) == {'ok': True}
    assert len(requests) == 3 and len(set(requests)) == 1
    assert sleeps == [3, 2]
    client.close()


def test_cli_pair_recover_and_diagnose_preserve_the_member(live, owner, tmp_path, monkeypatch):
    path = tmp_path / 'paired-agent' / 'credentials.json'
    monkeypatch.setenv('ROOM_CLIENT_CREDENTIALS', str(path))
    original = cli.RoomClient.__init__
    monkeypatch.setattr(cli.RoomClient, '__init__', lambda self, credentials, **kwargs:
                        original(self, credentials, sleep=lambda _: time.sleep(0.05), **kwargs))
    def approve_while_running(args):
        with ThreadPoolExecutor(1) as pool:
            result = pool.submit(cli.main, args)
            rows = []
            for _ in range(20):
                rows = live.get('/v1/owner/overview', headers=owner).json()['pairings']
                if rows:
                    break
                time.sleep(0.02)
            assert rows
            # The client persists its pickup identity before making the first request.
            assert json.loads(path.read_text())['pending_connection']['request_id']
            approved = live.post('/v1/owner/connections/' + rows[0]['user_code'] + '/approve', headers=owner, json={})
            assert approved.status_code == 200, approved.text
            assert result.result(timeout=5) == 0
    approve_while_running(['connect', '--url', str(live.base_url).rstrip('/'), '--name', 'paired-agent', '--owner', 'Human', '--contact', 'human@example.com'])
    first = json.loads(path.read_text())
    assert not first.get('pending_connection') and path.stat().st_mode & 0o777 == 0o600
    assert cli.main(['doctor']) == 0
    # An existing identity cannot accidentally be duplicated through the default command.
    assert cli.main(['connect']) == 3
    approve_while_running(['connect', '--recover'])
    second = json.loads(path.read_text())
    assert second['member_id'] == first['member_id'] and second['refresh_token'] != first['refresh_token']
    assert live.get('/v1/me', headers={'Authorization': 'Bearer ' + first['access_token']}).status_code == 401
    assert cli.main(['doctor']) == 0

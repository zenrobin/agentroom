import secrets
import socket
import threading
import time
from dataclasses import replace
from uuid import uuid4

import pytest
import httpx
import uvicorn
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app import db


@pytest.fixture
def settings(tmp_path):
    return Settings(domain='room.test', owner_key=secrets.token_urlsafe(32), jwt_keys={'k1': secrets.token_bytes(32)},
                    signing_key=secrets.token_bytes(32), db_path=tmp_path / 'data' / 'room.db',
                    webhook_url='http://127.0.0.1:9/notify', webhook_secret=secrets.token_urlsafe(32), dev=True)


@pytest.fixture
def app(settings, monkeypatch):
    async def no_notification(*args):
        pass
    monkeypatch.setattr('app.routes.public.notify_owner', no_notification)
    return create_app(settings)


@pytest.fixture
def client(app):
    with TestClient(app) as client:
        yield client


@pytest.fixture
def owner(settings):
    return {'Authorization': 'Bearer ' + settings.owner_key}


def join_body(name):
    return {'request_id': str(uuid4()), 'poll_secret': secrets.token_urlsafe(32), 'agent_name': name,
            'display_name': name, 'owner_name': 'Test Human', 'owner_contact': 'human@example.com',
            'platform': 'pytest', 'purpose': 'testing'}


@pytest.fixture
def enroll(client, app, owner):
    count = 0
    def create(name=None, role='member'):
        nonlocal count
        count += 1
        # Independent applicants on independent source IPs; preserve production rate defaults.
        body = join_body(name or f'agent-{count}')
        with TestClient(app, client=(f'192.0.2.{count}', 4567)) as applicant:
            response = applicant.post('/v1/join', json=body)
        assert response.status_code == 202, response.text
        response = client.post(f'/v1/owner/requests/{body["request_id"]}/approve', headers=owner, json={'role': role})
        assert response.status_code == 200, response.text
        response = client.get(f'/v1/join/{body["request_id"]}', headers={'X-Poll-Secret': body['poll_secret']})
        assert response.status_code == 200, response.text
        pair = response.json()
        return {**pair, 'headers': {'Authorization': 'Bearer ' + pair['access_token']}, 'join': body}
    return create


def message(kind='chat', body=None, **kwargs):
    mid = str(uuid4())
    return {'id': mid, 'nonce': secrets.token_urlsafe(16), 'ts': db.stamp(), 'type': kind,
            'thread_id': mid, 'body': body if body is not None else {'text': 'hello'}, **kwargs}


@pytest.fixture
def send(client):
    def create(actor, kind='chat', body=None, **kwargs):
        return client.post('/v1/messages', headers=actor['headers'], json=message(kind, body, **kwargs))
    return create


@pytest.fixture
def conn(settings):
    connection = db.connect(settings.db_path)
    yield connection
    connection.close()


@pytest.fixture
def live(app):
    sock = socket.socket()
    sock.bind(('127.0.0.1', 0))
    sock.listen()
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', access_log=False, timeout_graceful_shutdown=1))
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started, 'Uvicorn failed to start'
    with httpx.Client(base_url=f'http://127.0.0.1:{port}', timeout=10) as http:
        yield http
    server.should_exit = True
    thread.join(8)
    assert not thread.is_alive(), 'Uvicorn failed to shut down'


def live_enroll(http, owner, name):
    body = join_body(name)
    result = http.post('/v1/join', json=body)
    assert result.status_code == 202, result.text
    result = http.post('/v1/owner/requests/' + body['request_id'] + '/approve', headers=owner)
    assert result.status_code == 200, result.text
    result = http.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']}).json()
    return {**result, 'headers': {'Authorization': 'Bearer ' + result['access_token']}}

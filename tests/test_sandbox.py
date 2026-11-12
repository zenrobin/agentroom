import secrets
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app import db, negotiation, sandbox
from app.main import create_app
from tests.conftest import message, join_body


@pytest.fixture
def demo(settings):
    config = replace(settings, profile='sandbox', room_name='AgentRoom Sandbox', webhook_url=None, webhook_secret=None)
    with TestClient(create_app(config)) as client:
        yield client, config


def admit(client):
    body = {'request_id': str(uuid4()), 'poll_secret': secrets.token_urlsafe(32)}
    result = client.post('/v1/sandbox/join', json=body)
    assert result.status_code == 200, result.text
    pair = result.json()
    return {**pair, 'headers': {'Authorization': 'Bearer ' + pair['access_token']}, 'admission': body}


def test_guest_isolation_all_fetch_paths_and_writes(demo):
    client, config = demo
    a, b = admit(client), admit(client)
    assert {m['member_id'] for m in client.get('/v1/members', headers=a['headers']).json()} == {a['member_id'], sandbox.PEER}
    data = message(to=[sandbox.PEER], body={'text': 'fictional private A'})
    sent = client.post('/v1/messages', headers=a['headers'], json=data).json()
    a_inbox = client.get('/v1/inbox', headers=a['headers']).json()
    assert 'fictional private A' in a_inbox['items'][0]['body']['text']
    assert client.get('/v1/inbox', headers=b['headers']).json()['items'] == []
    assert client.get('/v1/messages', headers=b['headers']).json()['items'] == []
    assert client.get('/v1/messages/' + str(sent['seq']), headers=b['headers']).status_code == 404
    assert client.get('/v1/messages?thread_id=' + sent['thread_id'], headers=b['headers']).json()['items'] == []
    assert client.post('/v1/messages', headers=a['headers'], json=message()).status_code == 403
    assert client.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']])).status_code == 403
    assert client.post('/v1/messages', headers=b['headers'], json=message(to=[sandbox.PEER], thread_id=sent['thread_id'])).status_code == 404
    # Even an operator-created public event must not leak into a guest lane.
    conn = db.connect(config.db_path)
    with db.transaction(conn):
        public = negotiation.server_message(conn, config, 'system', {'event': 'developer.test'})
    conn.close()
    assert client.get('/v1/messages/' + str(public['seq']), headers=a['headers']).status_code == 404
    assert client.post('/v1/webhooks', headers=a['headers'], json={'url': 'https://example.com'}).status_code == 403
    assert client.get('/v1/owner/overview', headers=a['headers']).status_code == 401
    assert client.post('/v1/messages', headers=a['headers'], json=message(to=[sandbox.PEER], body={'text':'x'*5000})).status_code == 413


def test_guest_negotiation_uses_real_engine_and_is_private(demo):
    client, _ = demo
    a, b = admit(client), admit(client)
    proposal = message('propose', {'subject':'Fictional job', 'terms':{'demo':True}}, to=[sandbox.PEER])
    assert client.post('/v1/messages', headers=a['headers'], json=proposal).status_code == 201
    path = '/v1/negotiations/' + proposal['id']
    assert client.get(path, headers=b['headers']).status_code == 404
    detail = client.get(path, headers=a['headers']).json()
    body = {'version':1, 'terms_hash':detail['versions'][0]['terms_hash']}
    accept = '/v1/sandbox/peer/accept/' + proposal['id']
    assert client.post(accept, headers=b['headers'], json=body).status_code == 404
    assert client.post(accept, headers=a['headers'], json={**body, 'terms_hash':'0'*64}).status_code == 409
    result = client.post(accept, headers=a['headers'], json=body)
    assert result.status_code == 200, result.text
    commit = result.json()['commit']
    assert commit['type'] == 'commit' and commit['sig']
    assert client.post(accept, headers=a['headers'], json=body).json()['commit']['id'] == commit['id']
    assert client.get('/v1/commits/' + proposal['id'], headers=b['headers']).status_code == 404
    assert client.get('/v1/negotiations?state=committed', headers=b['headers']).json() == []


def test_admission_idempotency_absolute_expiry_and_cleanup(demo):
    client, config = demo
    a = admit(client)
    again = client.post('/v1/sandbox/join', json=a['admission']).json()
    assert again['refresh_token'] == a['refresh_token']
    assert client.post('/v1/sandbox/join', json={**a['admission'],'poll_secret':secrets.token_urlsafe(32)}).status_code == 409
    pair = client.post('/v1/token/refresh', json={'refresh_token':a['refresh_token'], 'request_id':str(uuid4())})
    assert pair.status_code == 200
    conn = db.connect(config.db_path)
    conn.execute('UPDATE members SET created_at=? WHERE member_id=?', (db.stamp(db.utcnow()-timedelta(minutes=16)),a['member_id']))
    assert client.get('/v1/me', headers=a['headers']).status_code == 401
    assert client.post('/v1/token/refresh', json={'refresh_token':pair.json()['refresh_token'], 'request_id':str(uuid4())}).status_code == 401
    conn.execute('UPDATE members SET created_at=? WHERE member_id=?', (db.stamp(db.utcnow()-timedelta(days=2)),a['member_id']))
    with db.transaction(conn):
        sandbox.cleanup(conn)
    assert db.member(conn, a['member_id']) is None
    conn.close()


def test_peer_endpoint_respects_commit_read_permission(demo):
    client, config = demo
    a = admit(client)
    p = message('propose', {'subject':'Demo', 'terms':{'demo':True}}, to=[sandbox.PEER])
    assert client.post('/v1/messages', headers=a['headers'], json=p).status_code == 201
    terms_hash = client.get('/v1/negotiations/'+p['id'],headers=a['headers']).json()['versions'][0]['terms_hash']
    conn = db.connect(config.db_path)
    conn.execute("INSERT INTO role_permissions VALUES(?,'commit',1,0)",(sandbox.ROLE,))
    endpoint = '/v1/sandbox/peer/accept/' + p['id']
    body = {'version':1,'terms_hash':terms_hash}
    assert client.post(endpoint,headers=a['headers'],json=body).json()['commit'] is None
    assert client.post(endpoint,headers=a['headers'],json=body).json()['commit'] is None
    assert client.get('/v1/commits/'+p['id'],headers=a['headers']).status_code == 404
    conn.close()


def test_peer_accept_commits_expiry_exactly_once(demo):
    client, config = demo
    a = admit(client)
    p = message('propose', {'subject':'Demo', 'terms':{'demo':True}}, to=[sandbox.PEER])
    assert client.post('/v1/messages',headers=a['headers'],json=p).status_code == 201
    version = client.get('/v1/negotiations/'+p['id'],headers=a['headers']).json()['versions'][0]
    conn = db.connect(config.db_path)
    conn.execute('UPDATE negotiation_versions SET expires_at=? WHERE negotiation_id=?', (db.stamp(db.utcnow()-timedelta(seconds=1)),p['id']))
    path = '/v1/sandbox/peer/accept/' + p['id']
    for _ in range(2):
        assert client.post(path,headers=a['headers'],json={'version':1,'terms_hash':version['terms_hash']}).status_code == 409
    assert db.negotiation(conn,p['id'])['state'] == 'expired'
    assert conn.execute("SELECT count(*) FROM messages WHERE thread_id=? AND type='expire'",(p['id'],)).fetchone()[0] == 1
    conn.close()


def test_private_room_cannot_enable_public_admission(client, settings):
    assert client.post('/v1/sandbox/join', json={'request_id':str(uuid4()),'poll_secret':secrets.token_urlsafe(32)}).status_code == 404
    assert client.get('/v1/room').json()['sandbox'] is None


def test_sandbox_never_accepts_family_database(settings, enroll):
    enroll('family-agent')
    with pytest.raises(RuntimeError, match='own empty database'):
        with TestClient(create_app(replace(settings, profile='sandbox'))):
            pass


def test_private_product_never_accepts_sandbox_database(demo):
    client, settings = demo
    with pytest.raises(RuntimeError, match='cannot serve a private product'):
        with TestClient(create_app(replace(settings, profile='home-team'))):
            pass


def test_unmarked_private_database_with_pending_applicant_is_not_a_sandbox(settings):
    db.initialize(settings.db_path)
    conn = db.connect(settings.db_path)
    with db.transaction(conn):
        body = join_body('pending-private')
        from app.models import Join
        db.create_join(conn, Join.model_validate(body), 'hash', 'hash', '192.0.2.1')
    with pytest.raises(RuntimeError, match='own empty database'):
        sandbox.initialize(conn, replace(settings, profile='sandbox'))
    assert conn.execute('SELECT count(*) FROM join_requests').fetchone()[0] == 1
    conn.close()


def test_unmarked_private_pairing_database_is_not_a_sandbox(settings):
    db.initialize(settings.db_path)
    conn = db.connect(settings.db_path)
    conn.execute('INSERT INTO device_connections(request_id,poll_hash,body_hash,user_code,mode,applicant,status,expires_at,created_at) VALUES(?,?,?,?,?,?,?,?,?)',
                 (str(uuid4()),'hash','hash','ABCDE-FG234','new','{"owner_contact":"private@example.test"}','pending',db.stamp(db.utcnow()+timedelta(minutes=15)),db.stamp()))
    with pytest.raises(RuntimeError, match='own empty database'):
        sandbox.initialize(conn, replace(settings, profile='sandbox'))
    assert conn.execute('SELECT count(*) FROM device_connections').fetchone()[0] == 1
    conn.close()


def test_sandbox_pages_and_protocol_discovery(demo):
    client, _ = demo
    assert client.get('/v1/room').json()['sandbox']['session_minutes'] == 15
    page = client.get('/', headers={'Accept':'text/html'})
    assert page.status_code == 200 and 'Start test session' in page.text
    assert 'sha256-' in page.headers['content-security-policy']
    guide = client.get('/agent-guide').text
    assert 'room_client.py sandbox --url https://room.test' in guide
    assert 'robin@' not in guide and 'agent-room-dusky' not in guide
    assert client.post('/v1/join', json=join_body('test')).status_code == 403


def test_home_team_branding_preserves_auth_audience(settings):
    config = replace(settings, profile='home-team', room_name='Home Team', public_domain='home.example.com')
    with TestClient(create_app(config)) as client:
        assert client.get('/v1/room').json()['url'] == 'https://home.example.com'
        assert '<title>Owner console · Home Team</title>' in client.get('/owner').text
        assert 'https://home.example.com' in client.get('/agent-guide').text
        assert config.domain == 'room.test'


def test_existing_member_tokens_survive_home_team_rebrand(settings, enroll):
    a = enroll('existing-family-agent')
    config = replace(settings, profile='home-team', room_name='Home Team', public_domain='home.example.test')
    with TestClient(create_app(config)) as rebranded:
        assert rebranded.get('/v1/me',headers=a['headers']).json()['member_id'] == a['member_id']
        assert rebranded.get('/v1/inbox',headers=a['headers']).status_code == 200


def test_standalone_sandbox_client_flow_and_credential_guard(demo, tmp_path, capsys):
    import httpx
    from clients import room_client as cli
    client, _ = demo
    def transport(request):
        r = client.request(request.method, request.url.raw_path.decode(), content=request.content, headers=request.headers)
        return httpx.Response(r.status_code, content=r.content, headers=r.headers)
    credentials = cli.Credentials(tmp_path/'sandbox'/'credentials.json')
    agent = cli.RoomClient(credentials, transport=httpx.MockTransport(transport))
    agent.sandbox(cli.parser().parse_args(['sandbox','--url','https://room.test']))
    assert credentials.path.stat().st_mode & 0o777 == 0o600
    sent = agent.send('chat',{'text':'Client demo'}, agent.resolve('test-peer'))
    assert sent['type'] == 'chat'
    assert agent.request('GET','/v1/inbox')['items']
    assert 'access_token' not in capsys.readouterr().out
    with pytest.raises(cli.ClientError, match='separate'):
        agent.sandbox(cli.parser().parse_args(['sandbox','--url','https://room.test']))
    agent.close()


def test_sandbox_sse_isolates_visitors_and_closes_at_deadline(settings):
    import json
    import socket
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    import httpx
    import uvicorn
    config = replace(settings,profile='sandbox',webhook_url=None,webhook_secret=None)
    app = create_app(config)
    sock = socket.socket();sock.bind(('127.0.0.1',0));sock.listen()
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app,log_level='critical',access_log=False,timeout_graceful_shutdown=1))
    thread = threading.Thread(target=lambda:server.run(sockets=[sock]),daemon=True);thread.start()
    deadline = time.monotonic()+10
    while not server.started and time.monotonic()<deadline:
        time.sleep(.01)
    assert server.started
    try:
        with httpx.Client(base_url=f'http://127.0.0.1:{port}',timeout=10) as http:
            a,b = admit(http),admit(http)
            private = http.post('/v1/messages',headers=a['headers'],json=message(to=[sandbox.PEER],body={'text':'A only'})).json()
            own = http.post('/v1/messages',headers=b['headers'],json=message(to=[sandbox.PEER],body={'text':'B only'})).json()
            with http.stream('GET','/v1/stream',headers=b['headers']) as stream:
                lines = stream.iter_lines();assert next(lines) == ': connected'
                seen=[]
                for line in lines:
                    if line.startswith('data: '):
                        m=json.loads(line[6:]);seen.append(m)
                        if m['from']==sandbox.PEER:break
                assert all(m['thread_id'] != private['thread_id'] for m in seen)
                assert any(m['thread_id']==own['thread_id'] for m in seen)
                with ThreadPoolExecutor(1) as pool:
                    remaining = pool.submit(lambda:list(lines))
                    conn = db.connect(config.db_path)
                    conn.execute('UPDATE members SET created_at=? WHERE member_id=?',(db.stamp(db.utcnow()-timedelta(minutes=16)),b['member_id']));conn.close()
                    http.post('/v1/messages',headers=a['headers'],json=message(to=[sandbox.PEER]))
                    assert all('data: ' not in line for line in remaining.result(timeout=5))
    finally:
        server.should_exit=True;thread.join(8)
        assert not thread.is_alive()

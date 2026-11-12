"""Opt-in integration tests against a disposable database on the linked Postgres server."""
import asyncio
import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import httpx
import pytest
from dotenv import dotenv_values
from fastapi.testclient import TestClient

from app import db
from app.config import Settings
from app.errors import APIError
from app.main import create_app
from app.ratelimit import DatabaseRateLimiter
from app.signing import verify
from app.stream import DatabaseHub
from .conftest import join_body, message

pytestmark = pytest.mark.skipif(os.getenv('AR_TEST_POSTGRES') != '1', reason='Set AR_TEST_POSTGRES=1 to run real Postgres tests')


def test_outbound_postgres_notifications_commit_rollback_and_member_isolation(postgres_url,settings,monkeypatch):
    from app import outbound
    monkeypatch.setattr(outbound,'HEARTBEAT',0.2)
    config=replace(settings,database_url=postgres_url,database_url_unpooled=postgres_url)
    mid='m_'+uuid4().hex
    async def verify():
        conn=db.connect(postgres_url)
        try:
            async with outbound.listener(config,mid) as wait:
                outbound.signal(conn,'m_'+uuid4().hex)
                assert not await wait(), 'Other members must not wake this listener'
                try:
                    with db.transaction(conn):
                        outbound.signal(conn,mid)
                        raise ValueError('rollback fixture')
                except ValueError:
                    pass
                assert not await wait(), 'Rolled-back work must not publish a notification'
                with db.transaction(conn): outbound.signal(conn,mid)
                assert await wait(), 'Committed work must signal another database connection'
                assert not await wait(), 'An idle connection must not manufacture events'
        finally:
            conn.close()
    asyncio.run(verify())


def test_postgres_sandbox_two_instances_and_cleanup(settings):
    from app import sandbox
    from .test_sandbox import admit
    from datetime import timedelta
    values = dotenv_values('.env.local')
    admin = values.get('DATABASE_URL_UNPOOLED') or values['DATABASE_URL']
    name = 'ar_test_' + uuid4().hex[:12]
    url = db.test_database(admin, name)
    config = replace(settings, database_url=url, profile='sandbox', webhook_url=None, webhook_secret=None)
    try:
        with TestClient(create_app(config)) as a_app, TestClient(create_app(config)) as b_app:
            a, b = admit(a_app), admit(b_app)
            sent = a_app.post('/v1/messages', headers=a['headers'], json=message(to=[sandbox.PEER],body={'text':'postgres-private-demo'}))
            assert sent.status_code == 201, sent.text
            assert b_app.get('/v1/messages', headers=b['headers']).json()['items'] == []
            assert a_app.get('/v1/inbox', headers=a['headers']).json()['items']
            conn = db.connect(url)
            with db.transaction(conn):
                conn.execute('UPDATE members SET created_at=? WHERE member_id=?', (db.stamp(db.utcnow()-timedelta(days=2)),a['member_id']))
                sandbox.cleanup(conn)
            assert db.member(conn,a['member_id']) is None
            conn.close()
            assert a_app.get('/v1/me', headers=a['headers']).status_code == 401
    finally:
        db.test_database(admin, name, create=False)


@pytest.fixture(scope='module')
def postgres_url():
    values = dotenv_values('.env.local')
    admin = values.get('DATABASE_URL_UNPOOLED') or values.get('DATABASE_URL')
    assert admin, 'Pull the linked development environment first'
    name = 'ar_test_' + uuid4().hex[:12]
    url = db.test_database(admin, name)
    try:
        db.initialize(url)
        yield url
    finally:
        db.test_database(admin, name, create=False)


def test_postgres_connector_receipt_and_readiness(postgres_url, settings):
    from app import auth
    config = replace(settings, database_url=postgres_url, webhook_url=None, webhook_secret=None)
    conn = db.connect(postgres_url)
    mid = 'm_' + uuid4().hex
    try:
        with db.transaction(conn):
            db.add_identity(conn, {'agent_name': 'connector-' + uuid4().hex, 'display_name': 'Connector fixture',
                                  'owner_name': 'Test', 'platform': 'Grok Bot'}, mid, 'member')
            pair = auth.new_tokens(conn, config, db.member(conn, mid))
        headers = {'Authorization': 'Bearer ' + pair['access_token']}
        owner = {'Authorization': 'Bearer ' + settings.owner_key}
        with TestClient(create_app(config)) as client:
            assert client.post('/v1/connector/ready', headers=headers, json={'provider':'grok',
                'recovery_interval_s':300,'recovery_verified_at':db.stamp()}).status_code == 200
            assert client.post('/v1/owner/members/'+mid+'/test', headers=owner, json={}).status_code == 201
            item = client.get('/v1/inbox', headers=headers).json()['items'][-1]
            assert client.post('/v1/inbox/ack', headers=headers, json={'seq':item['seq']}).status_code == 200
            body={'receipts':[{'seq':item['seq'],'message_id':item['id'],'outcome':'handled'}]}
            for _ in range(2):
                result=client.post('/v1/connector/receipts', headers=headers, json=body)
                assert result.status_code==200, result.text
                assert result.json()['accepted']==[item['seq']]
            status=client.get('/v1/connector/status',headers=headers).json()
            assert status['latest_processing_report']['message_seq']==item['seq']
            assert status['runtime']['provider']=='grok'
    finally:
        conn.close()


def test_postgres_watchdog_concurrency_and_receipt_race(postgres_url,settings,monkeypatch):
    from app import watchdog,webhooks,negotiation
    from datetime import timedelta
    from unittest.mock import AsyncMock
    from .test_providers import ROUTINE,KEY
    config=replace(settings,database_url=postgres_url,webhook_url=None,webhook_secret=None)
    mid='m_'+uuid4().hex
    conn=db.connect(postgres_url)
    try:
        with db.transaction(conn):
            db.add_identity(conn,{'agent_name':'watch-'+uuid4().hex,'display_name':'Watchdog fixture','owner_name':'Test','platform':'Grok'},mid,'member')
            grant=webhooks.create(conn,config,mid,ROUTINE,list(watchdog.TYPES),provider='grok',authorization=KEY)
            item=negotiation.server_message(conn,config,'system',{'text':'Fictional watchdog concurrency test'},[mid])
            conn.execute("UPDATE webhook_deliveries SET status='delivered',completed_at=? WHERE subscription_id=?",(db.stamp(),grant['subscription_id']))
        now=db.utcnow()+timedelta(seconds=301);monkeypatch.setattr(db,'utcnow',lambda:now)
        def check(_):
            c=db.connect(postgres_url)
            try: watchdog.process(c,mid)
            finally: c.close()
        with ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(check,range(3)))
        work=db.one(conn,'SELECT * FROM connector_work WHERE member_id=?',(mid,))
        assert work['retries']==1
        assert conn.execute('SELECT count(*) FROM connector_retry_wakes WHERE member_id=?',(mid,)).fetchone()[0]==1
        conn.execute("INSERT INTO processing_receipts VALUES(?,?,?,'handled',?)",(mid,item['seq'],item['id'],db.stamp()))
        post=AsyncMock();monkeypatch.setattr(webhooks,'post',post)
        asyncio.run(webhooks.deliver(config,work['delivery_id']))
        post.assert_not_called()
        # A newly enqueued message must not lose its schedule while an idle
        # check commits next_at=NULL from its earlier snapshot.
        from threading import Event
        snapshot,finish,attempting=Event(),Event(),Event()
        real_outstanding=watchdog.outstanding
        def held_snapshot(c,member,**kwargs):
            rows=real_outstanding(c,member,**kwargs)
            snapshot.set()
            assert finish.wait(5)
            return rows
        monkeypatch.setattr(watchdog,'outstanding',held_snapshot)
        conn.execute('UPDATE connector_watchdogs SET next_at=? WHERE member_id=?',(db.stamp(),mid))
        def new_message():
            c=db.connect(postgres_url)
            try:
                attempting.set()
                with db.transaction(c):
                    return negotiation.server_message(c,config,'system',{'text':'Concurrent fictional message'},[mid])
            finally:c.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            checking=pool.submit(check,0)
            assert snapshot.wait(5)
            sending=pool.submit(new_message)
            assert attempting.wait(5)
            try:
                from concurrent.futures import TimeoutError
                with pytest.raises(TimeoutError):sending.result(timeout=0.2)
            finally:finish.set()
            checking.result(timeout=5)
            added=sending.result(timeout=5)
        assert db.one(conn,'SELECT next_at FROM connector_watchdogs WHERE member_id=?',(mid,))['next_at']
        assert db.one(conn,'SELECT * FROM connector_work WHERE member_id=? AND message_seq=?',(mid,added['seq']))
        db.initialize(postgres_url)
        assert db.ready(conn) and db.member(conn,mid)['status']=='active'
    finally: conn.close()


def test_postgres_provider_credentials_and_delivery_lifecycle(postgres_url, settings, monkeypatch):
    from app import auth, providers, webhooks
    from unittest.mock import AsyncMock
    from .test_providers import ROUTINE, KEY
    config = replace(settings, database_url=postgres_url, webhook_url=None, webhook_secret=None)
    conn = db.connect(postgres_url)
    mid = 'm_' + uuid4().hex
    try:
        with db.transaction(conn):
            db.add_identity(conn, {'agent_name': 'provider-' + uuid4().hex, 'display_name': 'Provider fixture',
                                  'owner_name': 'Test', 'platform': 'Grok Bot'}, mid, 'member')
            pair = auth.new_tokens(conn, config, db.member(conn, mid))
        sender = AsyncMock(return_value=200)
        monkeypatch.setattr(webhooks, 'post', sender)
        headers = {'Authorization': 'Bearer ' + pair['access_token']}
        owner = {'Authorization': 'Bearer ' + settings.owner_key}
        with TestClient(create_app(config)) as client:
            result = client.post('/v1/webhooks', headers=headers,
                                 json={'provider': 'grok', 'url': ROUTINE, 'authorization': 'Bearer ' + KEY})
            assert result.status_code == 201, result.text
            sid = result.json()['subscription_id']
            saved = db.one(conn, 'SELECT * FROM webhook_provider_credentials WHERE subscription_id=?', (sid,))
            assert providers.cipher(config).decrypt(saved['credential'].encode()).decode() == 'Bearer ' + KEY
            test = client.post('/v1/webhooks/' + sid + '/test', headers=headers, json={})
            assert test.status_code == 202, test.text
            delivery = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=?', (test.json()['delivery_id'],))
            assert delivery['status'] == 'delivered' and delivery['last_status'] == 200
            assert KEY not in client.get('/v1/webhooks', headers=headers).text
            assert sender.await_count == 1
            assert client.post('/v1/owner/webhooks/' + sid + '/delete', headers=owner, json={}).status_code == 200
            assert db.one(conn, 'SELECT * FROM webhook_provider_credentials WHERE subscription_id=?', (sid,)) is None
        db.initialize(postgres_url)
        assert db.ready(conn) and db.member(conn, mid)
    finally:
        conn.close()


def test_postgres_shared_limits_and_leases(postgres_url):
    first, second = DatabaseRateLimiter(postgres_url), DatabaseRateLimiter(postgres_url)
    first.hit('isolation', 'actor', 0.001, 1)
    with pytest.raises(APIError) as error:
        second.hit('isolation', 'actor', 0.001, 1)
    assert error.value.status == 429
    for _ in range(3):
        first.hourly('hard_cap', 'actor', 3)
    with pytest.raises(APIError):
        second.hourly('hard_cap', 'actor', 3)
    for _ in range(20):
        first.fail('block-test')
    with pytest.raises(APIError):
        second.check_ip('block-test')
    assert second.counts['isolation'] == 1
    async def leases():
        a, b = DatabaseHub(postgres_url), DatabaseHub(postgres_url)
        with a.subscribe('member', 'sse'):
            with b.subscribe('member', 'sse'):
                with pytest.raises(APIError):
                    with a.subscribe('member', 'sse'):
                        pass
    asyncio.run(leases())


def test_postgres_outbox_recovery_does_not_wait_for_room_lock(postgres_url,settings,monkeypatch):
    from app import operations,webhooks
    from unittest.mock import AsyncMock
    from datetime import timedelta
    remote=replace(settings,database_url=postgres_url,serverless=True)
    conn=db.connect(postgres_url)
    try:
        # This test needs only a due outbox row. Its fixture stays in this
        # disposable database and no HTTP destination is contacted.
        with db.transaction(conn):
            mid='m_'+uuid4().hex
            db.add_identity(conn,{'agent_name':'recovery-'+uuid4().hex,'display_name':'Recovery fixture','owner_name':'Test','platform':'pytest'},mid,'member')
            grant=webhooks.create(conn,remote,mid,'https://receiver.example/hooks',['chat'])
            sub=db.one(conn,'SELECT * FROM webhook_subscriptions WHERE subscription_id=?',(grant['subscription_id'],))
            did=webhooks.enqueue(conn,sub,{'type':'webhook.test','member_id':mid})
            conn.execute('UPDATE webhook_deliveries SET next_at=? WHERE delivery_id=?',(db.stamp(db.utcnow()-timedelta(minutes=3)),did))
        kick=AsyncMock();monkeypatch.setattr(webhooks,'kick',kick)
        with db.transaction(conn):
            with ThreadPoolExecutor(1) as pool:
                pool.submit(lambda:asyncio.run(operations.recover(remote))).result(timeout=4)
        asyncio.run(operations.recover(remote))
        kick.assert_awaited_once_with(remote,force=True)
        monkeypatch.setattr('vercel.queue.QueueClient.send',AsyncMock())
        rid=str(uuid4())
        asyncio.run(webhooks.dispatch(remote,db.one(conn,'SELECT * FROM webhook_deliveries WHERE delivery_id=?',(did,)),recovery_id=rid))
        assert db.one(conn,'SELECT * FROM webhook_recoveries WHERE delivery_id=? AND recovery_id=?',(did,rid))
    finally:
        conn.close()


def test_postgres_limit_checks_do_not_wait_for_room_lock(postgres_url):
    conn = db.connect(postgres_url)
    limiter = DatabaseRateLimiter(postgres_url)
    try:
        with db.transaction(conn):
            with ThreadPoolExecutor(1) as pool:
                # A real held room lock must not block another instance's limiter/auth bookkeeping.
                pool.submit(limiter.hit, 'independent', 'sender', 1, 1).result(timeout=4)
                pool.submit(limiter.fail, 'independent-ip').result(timeout=4)
    finally:
        conn.close()


def test_postgres_same_bucket_is_atomic(postgres_url):
    def attempt(_):
        try:
            DatabaseRateLimiter(postgres_url).hit('concurrent', 'one-bucket', 0.0001, 1)
            return True
        except APIError as error:
            assert error.status == 429
            return False
    with ThreadPoolExecutor(4) as pool:
        assert sum(pool.map(attempt, range(4))) == 1


def test_postgres_http_reuses_and_releases_one_connection(postgres_url, settings, monkeypatch):
    app = create_app(replace(settings, database_url=postgres_url))
    with TestClient(app) as client:
        opened = []
        real = db.Postgres
        def track(url):
            conn = real(url)
            opened.append(conn)
            return conn
        monkeypatch.setattr(db, 'Postgres', track)
        assert client.get('/readyz').status_code == 200
        assert len(opened) == 1  # IP block lookup, readiness and metrics all reuse it.
        assert opened[0].raw.closed


def test_postgres_two_instances_commit_refresh_and_privacy(postgres_url, settings, monkeypatch):
    settings = replace(settings, database_url=postgres_url)
    async def no_notification(*args):
        pass
    monkeypatch.setattr('app.routes.public.notify_owner', no_notification)
    one, two = create_app(settings), create_app(settings)
    owner = {'Authorization': 'Bearer ' + settings.owner_key}
    with TestClient(one, client=('192.0.2.1', 1234)) as a_client, TestClient(two, client=('192.0.2.2', 1234)) as b_client:
        def enroll(client, name):
            body = join_body(name)
            assert client.post('/v1/join', json=body).status_code == 202
            result = client.post('/v1/owner/requests/' + body['request_id'] + '/approve', headers=owner)
            assert result.status_code == 200, result.text
            pair = client.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']}).json()
            return {**pair, 'headers': {'Authorization': 'Bearer ' + pair['access_token']}}
        a, b = enroll(a_client, 'pg-a'), enroll(b_client, 'pg-b')
        c = enroll(a_client, 'pg-c')
        sent = a_client.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']]))
        assert sent.status_code == 201, sent.text
        assert b_client.get('/v1/messages/' + str(sent.json()['seq']), headers=b['headers']).status_code == 200
        assert b_client.get('/v1/messages/' + str(sent.json()['seq']), headers=c['headers']).status_code == 404
        pending = b_client.get('/v1/inbox', headers=b['headers']).json()
        assert sent.json()['id'] in str(pending)
        b_client.post('/v1/inbox/ack', headers=b['headers'], json={'seq': pending['items'][-1]['seq']})
        with ThreadPoolExecutor(1) as pool:
            future = pool.submit(b_client.get, '/v1/inbox?wait=10', headers=b['headers'])
            time.sleep(0.1)
            next_message = a_client.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']]))
            result = future.result(timeout=5)
            assert next_message.json()['id'] in str(result.json())
        proposal = a_client.post('/v1/messages', headers=a['headers'], json=message('propose', {'subject': 'Pickup', 'terms': {'day': 'Thursday'}}, to=[b['member_id']]))
        assert proposal.status_code == 201, proposal.text
        nid = proposal.json()['id']
        detail = b_client.get('/v1/negotiations/' + nid, headers=b['headers']).json()
        assert detail['state'] == 'open'
        data = message('accept', {'negotiation_id': nid, 'version': 1, 'terms_hash': detail['versions'][0]['terms_hash']}, to=[a['member_id']], thread_id=nid)
        first = b_client.post('/v1/messages', headers=b['headers'], json=data)
        assert first.status_code == 201, first.text
        assert a_client.post('/v1/messages', headers=b['headers'], json=data).status_code == 200
        commit = a_client.get('/v1/commits/' + nid, headers=a['headers']).json()
        verify(commit['commit_message'], commit['public_key'])
        assert b_client.get('/v1/commits/' + nid, headers=c['headers']).status_code == 404
        proposal = a_client.post('/v1/messages', headers=a['headers'], json=message('propose', {'subject': 'Concurrent acceptance', 'terms': {}}, to=[b['member_id']]))
        nid = proposal.json()['id']
        detail = b_client.get('/v1/negotiations/' + nid, headers=b['headers']).json()
        def accept_on(client):
            return client.post('/v1/messages', headers=b['headers'], json=message('accept',
                {'negotiation_id': nid, 'version': 1, 'terms_hash': detail['versions'][0]['terms_hash']},
                to=[a['member_id']], thread_id=nid))
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(accept_on, [a_client, b_client]))
        assert sorted(result.status_code for result in results) == [201, 409]
        assert a_client.get('/v1/commits/' + nid, headers=a['headers']).status_code == 200
        rotated = b_client.post('/v1/token/refresh', json={'refresh_token': b['refresh_token']})
        assert rotated.status_code == 200, rotated.text
        assert a_client.post('/v1/token/refresh', json={'refresh_token': b['refresh_token']}).status_code == 401
        assert b_client.get('/v1/me', headers={'Authorization': 'Bearer ' + rotated.json()['access_token']}).status_code == 401
        assert a_client.get('/v1/owner/metrics', headers=owner).status_code == 200


def test_postgres_refresh_retry_and_owner_reconnect(postgres_url, settings, monkeypatch):
    async def quiet(*args):
        pass
    monkeypatch.setattr('app.routes.public.notify_owner', quiet)
    config = replace(settings, database_url=postgres_url)
    owner = {'Authorization': 'Bearer ' + config.owner_key}
    with TestClient(create_app(config), client=('192.0.2.41', 1234)) as first, TestClient(create_app(config), client=('192.0.2.42', 1234)) as second:
        body = join_body('pg-auth-retry')
        assert first.post('/v1/join', json=body).status_code == 202
        member = first.post('/v1/owner/requests/' + body['request_id'] + '/approve', headers=owner).json()['member_id']
        pair = first.get('/v1/join/' + body['request_id'], headers={'X-Poll-Secret': body['poll_secret']}).json()
        refresh = {'refresh_token': pair['refresh_token'], 'request_id': str(uuid4())}
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda client: client.post('/v1/token/refresh', json=refresh), [first, second]))
        assert [r.status_code for r in results] == [200, 200]
        assert results[0].json() == results[1].json()
        ticket = first.post('/v1/owner/members/' + member + '/reconnect', headers=owner, json={})
        assert ticket.status_code == 201
        grant = {k: ticket.json()[k] for k in ['ticket_id', 'secret']}
        renewed = second.post('/v1/token/reconnect', json=grant)
        assert renewed.status_code == 200 and renewed.json()['member_id'] == member
        assert first.post('/v1/token/reconnect', json=grant).json() == renewed.json()
        assert first.get('/v1/me', headers={'Authorization': 'Bearer ' + renewed.json()['access_token']}).status_code == 200


def test_postgres_console_pairing_and_atomic_webhook_outbox(postgres_url, settings, monkeypatch):
    from unittest.mock import AsyncMock
    from app import webhooks
    monkeypatch.setattr(webhooks, 'kick', AsyncMock())
    config = replace(settings, database_url=postgres_url)
    db.initialize(postgres_url)  # Extension migration is repeatable and leaves core version4 readable by the older deployment.
    owner = {'Authorization': 'Bearer ' + config.owner_key}
    with TestClient(create_app(config), client=('192.0.2.77', 1234)) as client:
        body = {**join_body('pg-paired'), 'mode': 'new'}
        started = client.post('/v1/connect', json=body)
        assert started.status_code == 202, started.text
        code = started.json()['user_code']
        locked = db.connect(postgres_url)
        try:
            with db.transaction(locked):
                with ThreadPoolExecutor(1) as pool:
                    pending = pool.submit(client.get, '/v1/connect/' + body['request_id'], headers={'X-Connection-Secret': body['poll_secret']}).result(timeout=4)
                    assert pending.json()['status'] == 'pending'
                    invalid = pool.submit(client.get, '/v1/connect/unknown').result(timeout=4)
                    assert invalid.status_code == 404
        finally:
            locked.close()
        assert client.post('/v1/owner/connections/' + code + '/approve', headers=owner, json={}).status_code == 200
        assert client.get('/v1/owner/overview', headers=owner).status_code == 200
        pair = client.get('/v1/connect/' + body['request_id'], headers={'X-Connection-Secret': body['poll_secret']}).json()
        headers = {'Authorization': 'Bearer ' + pair['access_token']}
        sub = client.post('/v1/webhooks', headers=headers, json={'url': 'https://receiver.example/notifications'})
        assert sub.status_code == 201, sub.text
        test = client.post('/v1/owner/members/' + pair['member_id'] + '/test', headers=owner)
        assert test.status_code == 201
        conn = db.connect(postgres_url)
        try:
            assert conn.execute('SELECT version FROM schema_version').fetchone()[0] == 4
            row = db.one(conn, 'SELECT * FROM webhook_deliveries WHERE message_seq=?', (test.json()['seq'],))
            assert row and row['status'] == 'pending'
            async def post(*args):
                await asyncio.sleep(0.05)
                return 204
            mocked = AsyncMock(side_effect=post)
            monkeypatch.setattr(webhooks, 'post', mocked)
            async def concurrent():
                await asyncio.gather(webhooks.deliver(config, row['delivery_id']), webhooks.deliver(config, row['delivery_id']))
            asyncio.run(concurrent())
            assert mocked.await_count == 1
            assert db.one(conn, 'SELECT * FROM webhook_deliveries WHERE delivery_id=?', (row['delivery_id'],))['status'] == 'delivered'
        finally:
            conn.close()

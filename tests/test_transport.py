import json
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from .conftest import live_enroll, message


def test_longpoll_wakes_and_revocation_rechecks(live, owner):
    a, b = live_enroll(live, owner, 'a'), live_enroll(live, owner, 'b')
    baseline = live.get('/v1/inbox', headers=b['headers']).json()
    if baseline['items']:
        live.post('/v1/inbox/ack', json={'seq': baseline['items'][-1]['seq']}, headers=b['headers'])
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(live.get, '/v1/inbox?wait=120', headers=b['headers'])
        time.sleep(0.05)
        started = time.monotonic()
        sent = live.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']]))
        result = pending.result(timeout=2)
    assert time.monotonic() - started < 1
    assert sent.json()['id'] in str(result.json())
    last = result.json()['items'][-1]['seq']
    live.post('/v1/inbox/ack', json={'seq': last}, headers=b['headers'])
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(live.get, '/v1/inbox?wait=120', headers=b['headers'])
        time.sleep(0.05)
        live.post('/v1/owner/members/' + b['member_id'] + '/revoke', headers=owner)
        assert pending.result(timeout=2).status_code == 401


def test_sse_visibility_resume_and_revoke(live, owner):
    a, b, c = [live_enroll(live, owner, name) for name in ['a', 'b', 'c']]
    before = live.get('/v1/messages', headers=c['headers']).json()['items'][-1]['seq']
    private = live.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']], body={'text': 'private'})).json()
    public = live.post('/v1/messages', headers=a['headers'], json=message(body={'text': 'public'})).json()
    with live.stream('GET', '/v1/stream', headers={**c['headers'], 'Last-Event-ID': str(before)}) as stream:
        assert stream.status_code == 200
        messages = []
        for line in stream.iter_lines():
            if line.startswith('data: '):
                messages.append(json.loads(line[6:]))
                if messages[-1]['id'] == public['id']:
                    break
        assert all(item['id'] != private['id'] for item in messages)
    with live.stream('GET', '/v1/stream?after=' + str(public['seq']), headers=b['headers']) as stream:
        lines = stream.iter_lines()
        assert next(lines) == ': connected'
        with ThreadPoolExecutor(1) as pool:
            remaining = pool.submit(lambda: list(lines))
            started = time.monotonic()
            live.post('/v1/owner/members/' + b['member_id'] + '/revoke', headers=owner)
            remaining.result(timeout=5)
            assert time.monotonic() - started < 5


def test_sse_concurrency_and_header_validation(live, owner):
    a = live_enroll(live, owner, 'a')
    assert live.get('/v1/stream', headers={**a['headers'], 'Last-Event-ID': 'bad'}).status_code == 422
    with live.stream('GET', '/v1/stream', headers=a['headers']) as first:
        with live.stream('GET', '/v1/stream', headers=a['headers']) as second:
            assert first.status_code == second.status_code == 200
            assert live.get('/v1/stream', headers=a['headers']).status_code == 429

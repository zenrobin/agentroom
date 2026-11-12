import json
import time

from .conftest import live_enroll, message


def test_500_messages_paginate_and_sse_resume_without_gaps(live, app, owner):
    a, b = live_enroll(live, owner, 'a'), live_enroll(live, owner, 'b')
    baseline = live.get('/v1/messages', headers=b['headers']).json()['items'][-1]['seq']
    clock = [time.monotonic()]
    app.state.limiter.clock = lambda: clock[0]
    expected = []
    for _ in range(500):
        clock[0] += 0.51
        result = live.post('/v1/messages', headers=a['headers'], json=message(to=[b['member_id']]))
        assert result.status_code == 201, result.text
        expected.append(result.json()['seq'])
    actual, cursor = [], baseline
    while True:
        page = live.get(f'/v1/messages?after={cursor}&limit=200', headers=b['headers']).json()
        actual.extend(item['seq'] for item in page['items'])
        cursor = page['next_after']
        if not page['has_more']:
            break
    assert actual == expected
    # Backfill crosses all 200-item boundaries; reconnect from a midstream event ID.
    received = []
    with live.stream('GET', '/v1/stream', headers={**b['headers'], 'Last-Event-ID': str(expected[249])}) as response:
        for line in response.iter_lines():
            if line.startswith('data: '):
                received.append(json.loads(line[6:])['seq'])
                if received[-1] == expected[-1]:
                    break
    assert received == expected[250:]

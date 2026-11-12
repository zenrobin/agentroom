import pytest

from app.errors import APIError
from app.ratelimit import RateLimiter
from .conftest import join_body, message


def test_fourth_join_and_integer_retry_after(client):
    for i in range(3):
        assert client.post('/v1/join', json=join_body(f'agent{i}')).status_code == 202
    response = client.post('/v1/join', json=join_body('over'))
    assert response.status_code == 429
    assert int(response.headers['Retry-After']) > 0


def test_eleventh_send(client, enroll):
    actor = enroll()
    for _ in range(10):
        assert client.post('/v1/messages', headers=actor['headers'], json=message()).status_code == 201
    assert client.post('/v1/messages', headers=actor['headers'], json=message()).status_code == 429


def test_failed_auth_block(client, owner):
    for _ in range(20):
        assert client.get('/v1/me').status_code == 401
    assert client.get('/v1/owner/members', headers=owner).status_code == 429


def test_refill_hourly_pruning_and_block_expiry():
    now = [0]
    limiter = RateLimiter(clock=lambda: now[0])
    limiter.hit('test', 'key', 1, 1)
    with pytest.raises(APIError):
        limiter.hit('test', 'key', 1, 1)
    now[0] = 1
    limiter.hit('test', 'key', 1, 1)
    for _ in range(500):
        limiter.hourly('hour', 'key', 500)
    now[0] = 100
    with pytest.raises(APIError):
        limiter.hourly('hour', 'key', 500)
    for _ in range(20):
        limiter.fail('ip')
    with pytest.raises(APIError):
        limiter.check_ip('ip')
    now[0] = 1001
    limiter.check_ip('ip')
    now[0] = 10000
    limiter.prune()
    assert not limiter.buckets and not limiter.failures and not limiter.blocked and not limiter.windows

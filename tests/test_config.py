import base64
import secrets

import pytest

from app.config import Settings, decode_key


def env(monkeypatch):
    import os
    for key in list(os.environ):
        if key.startswith('AR_'):
            monkeypatch.delenv(key)
    monkeypatch.setenv('AR_DOMAIN', 'agents.fixmyeveryday.com')
    monkeypatch.setenv('AR_OWNER_KEY', secrets.token_urlsafe(32))
    monkeypatch.setenv('AR_JWT_KEYS', 'k1:' + base64.b64encode(secrets.token_bytes(32)).decode())
    monkeypatch.setenv('AR_SIGNING_KEY', base64.b64encode(secrets.token_bytes(32)).decode())
    monkeypatch.setenv('AR_OWNER_WEBHOOK_URL', 'https://example.com/notify')
    monkeypatch.setenv('AR_WEBHOOK_SECRET', secrets.token_urlsafe(32))


def test_valid_env_and_rotated_keys(monkeypatch):
    env(monkeypatch)
    assert Settings.from_env().domain == 'agents.fixmyeveryday.com'
    import os
    monkeypatch.setenv('AR_JWT_KEYS', 'k2:' + base64.b64encode(secrets.token_bytes(32)).decode() + ',' + os.environ['AR_JWT_KEYS'])
    assert list(Settings.from_env().jwt_keys) == ['k2', 'k1']


@pytest.mark.parametrize('key,value', [('AR_DOMAIN', 'https://bad.test'), ('AR_DOMAIN', 'bad..test'),
    ('AR_OWNER_KEY', 'short'), ('AR_JWT_KEYS', 'broken'), ('AR_SIGNING_KEY', 'wrong'),
    ('AR_MAX_MEMBERS', '0'), ('AR_DEV', 'maybe'), ('AR_OWNER_WEBHOOK_URL', 'http://example.com'),
    ('AR_WEBHOOK_SECRET', ''), ('AR_OWNER_EMAIL', 'partial@example.com')])
def test_invalid_env(monkeypatch, key, value):
    env(monkeypatch)
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        Settings.from_env()


def test_no_channel_or_missing_required(monkeypatch):
    env(monkeypatch)
    monkeypatch.delenv('AR_DOMAIN')
    with pytest.raises(ValueError, match='AR_DOMAIN'):
        Settings.from_env()
    monkeypatch.setenv('AR_DOMAIN', 'room.test')
    monkeypatch.delenv('AR_OWNER_WEBHOOK_URL')
    monkeypatch.delenv('AR_WEBHOOK_SECRET')
    with pytest.raises(ValueError, match='notification'):
        Settings.from_env()

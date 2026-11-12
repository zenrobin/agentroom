from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def test_remote_startup_recovers_after_transient_failure(settings, monkeypatch):
    initialize = Mock(side_effect=[psycopg.OperationalError('temporary network failure'), None])
    connection = Mock()
    sleep = AsyncMock()
    monkeypatch.setattr('app.main.db.initialize', initialize)
    monkeypatch.setattr('app.main.db.connect', Mock(return_value=connection))
    monkeypatch.setattr('app.main.db.ready', Mock(return_value=True))
    monkeypatch.setattr('app.main.sandbox.initialize', Mock())
    monkeypatch.setattr('app.main.asyncio.sleep', sleep)
    app = create_app(replace(settings, database_url='postgresql://unused.test/room'))
    with TestClient(app):
        assert app.state.settings.database_url
    assert initialize.call_count == 2
    connection.close.assert_called_once()
    sleep.assert_awaited_once_with(1)


def test_remote_startup_failure_is_bounded_and_sanitized(settings, monkeypatch):
    initialize = Mock(side_effect=psycopg.OperationalError('sensitive provider diagnostic'))
    sleep = AsyncMock()
    monkeypatch.setattr('app.main.db.initialize', initialize)
    monkeypatch.setattr('app.main.asyncio.sleep', sleep)
    app = create_app(replace(settings, database_url='postgresql://unused.test/room'))
    with pytest.raises(RuntimeError, match='^Database unavailable during startup$'):
        with TestClient(app):
            pass
    assert initialize.call_count == 3
    assert sleep.await_count == 2

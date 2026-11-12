import asyncio
from unittest.mock import Mock

import pytest

from app import db


@pytest.mark.parametrize('fail', [False, True])
def test_request_reuses_connection_and_closes_after_background_work(monkeypatch, fail):
    connections = []
    class Connection:
        def __init__(self, url):
            self.raw = Mock()
            self.request_managed = False
            connections.append(self)
        def close(self):
            if not self.request_managed:
                self.raw.close()
    monkeypatch.setattr(db, 'Postgres', Connection)
    async def app(scope, receive, send):
        a = db.connect('postgresql://unused.test/room')
        a.close()
        assert db.connect('postgresql://unused.test/room') is a
        async def background():
            assert db.connect('postgresql://unused.test/room') is a
            a.raw.close.assert_not_called()
        await asyncio.create_task(background())
        if fail:
            raise RuntimeError('test')
    async def run():
        try:
            await db.RequestConnectionsMiddleware(app)({'type': 'http'}, None, None)
        except RuntimeError:
            assert fail
        assert db._request_connections.get() is None
    asyncio.run(run())
    assert len(connections) == 1
    connections[0].raw.close.assert_called_once()

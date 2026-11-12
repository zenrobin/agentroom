import asyncio
from collections import defaultdict
from contextlib import contextmanager

from .errors import APIError
from . import db
import time
from uuid import uuid4


class Hub:
    """Queue wakeups only; DB backfill is authoritative, including after overflow."""
    def __init__(self):
        self.listeners = set()
        self.active = defaultdict(int)

    def publish(self):
        for queue in tuple(self.listeners):
            if queue.empty():
                queue.put_nowait(True)

    @contextmanager
    def subscribe(self, member_id, kind):
        key = (member_id, kind)
        if self.active[key] >= 2:
            raise APIError(429, 'rate_limited', headers={'Retry-After': '5'})
        self.active[key] += 1
        queue = asyncio.Queue(maxsize=1)
        self.listeners.add(queue)
        try:
            yield queue
        finally:
            self.listeners.discard(queue)
            self.active[key] -= 1
            if not self.active[key]:
                del self.active[key]

    @property
    def streams(self):
        return sum(n for (_, kind), n in self.active.items() if kind == 'sse')

    def renew(self, queue, conn):
        return True


class DatabaseHub(Hub):
    """Shared connection leases; DB polling delivers across instances without a process-local bus."""
    def __init__(self, target):
        super().__init__()
        self.target = target

    @contextmanager
    def subscribe(self, member_id, kind):
        lease_id = str(uuid4())
        conn = db.connect(self.target)
        try:
            with db.transaction(conn, lock_key=('lease', member_id, kind)):
                allowed = db.acquire_lease(conn, lease_id, member_id, kind)
        finally:
            conn.close()
        if not allowed:
            raise APIError(429, 'rate_limited', headers={'Retry-After': '5'})
        queue = asyncio.Queue(maxsize=1)
        queue.lease_id, queue.renewed_at = lease_id, time.monotonic()
        self.listeners.add(queue)
        try:
            yield queue
        finally:
            self.listeners.discard(queue)
            try:
                conn = db.connect(self.target)
                try:
                    db.release_lease(conn, lease_id)
                finally:
                    conn.close()
            except Exception:
                pass  # An interrupted function's lease expires automatically in 15 seconds.

    def renew(self, queue, conn):
        if time.monotonic() - queue.renewed_at < 3:
            return True
        alive = db.renew_lease(conn, queue.lease_id)
        queue.renewed_at = time.monotonic()
        return alive

    @property
    def streams(self):
        conn = db.connect(self.target)
        try:
            return db.stream_count(conn)
        finally:
            conn.close()

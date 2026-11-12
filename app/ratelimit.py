import math
import json
import time
from collections import defaultdict, deque

from .errors import APIError
from . import db


class RateLimiter:
    """Single-process token buckets; methods run only on the event loop thread."""
    def __init__(self, clock=time.monotonic):
        self.clock, self.buckets = clock, {}
        self.failures, self.blocked = defaultdict(deque), {}
        self.counts = defaultdict(int)
        self.windows = defaultdict(deque)

    def hourly(self, scope, key, limit):
        now = self.clock()
        window = self.windows[(scope, key)]
        while window and window[0] <= now - 3600:
            window.popleft()
        if len(window) >= limit:
            self.counts[scope] += 1
            raise APIError(429, 'rate_limited', headers={'Retry-After': str(max(1, math.ceil(window[0] + 3600 - now)))})
        window.append(now)

    def hit(self, scope, key, rate, burst):
        now = self.clock()
        tokens, last = self.buckets.get((scope, key), (float(burst), now))
        tokens = min(burst, tokens + (now - last) * rate)
        if tokens < 1:
            self.counts[scope] += 1
            raise APIError(429, 'rate_limited', headers={'Retry-After': str(max(1, math.ceil((1 - tokens) / rate)))})
        self.buckets[(scope, key)] = (tokens - 1, now)

    def check_ip(self, ip):
        if self.blocked.get(ip, 0) > self.clock():
            self.counts['failed_auth'] += 1
            raise APIError(429, 'rate_limited', headers={'Retry-After': str(math.ceil(self.blocked[ip] - self.clock()))})

    def fail(self, ip):
        now = self.clock()
        failures = self.failures[ip]
        while failures and failures[0] < now - 600:
            failures.popleft()
        failures.append(now)
        if len(failures) >= 20:
            newly = self.blocked.get(ip, 0) <= now
            self.blocked[ip] = now + 900
            return newly
        return False

    def prune(self):
        now = self.clock()
        self.buckets = {k: v for k, v in self.buckets.items() if now - v[1] < 7200}
        self.blocked = {k: v for k, v in self.blocked.items() if v > now}
        for ip in list(self.failures):
            if not self.failures[ip] or self.failures[ip][-1] < now - 600:
                del self.failures[ip]
        for key in list(self.windows):
            if not self.windows[key] or self.windows[key][-1] <= now - 3600:
                del self.windows[key]


class DatabaseRateLimiter:
    """Atomic limits shared by all Vercel instances, using the database's clock."""
    def __init__(self, target):
        self.target = target

    @property
    def counts(self):
        conn = db.connect(self.target)
        try:
            return db.shared_metrics(conn, 'rate_limited')
        finally:
            conn.close()

    def hit(self, scope, key, rate, burst):
        conn, retry = db.connect(self.target), None
        try:
            with db.transaction(conn, lock_key=('bucket', scope, key)):
                now = db.clock(conn)
                prior = db.bucket(conn, scope, key)
                tokens = min(burst, prior['tokens'] + (now - prior['updated_at']) * rate) if prior else float(burst)
                if tokens < 1:
                    retry = max(1, math.ceil((1 - tokens) / rate))
                    db.count_metric(conn, 'rate_limited', scope)
                else:
                    db.store_bucket(conn, scope, key, tokens - 1, now)
        finally:
            conn.close()
        if retry:
            raise APIError(429, 'rate_limited', headers={'Retry-After': str(retry)})

    def hourly(self, scope, key, limit):
        conn, retry = db.connect(self.target), None
        try:
            with db.transaction(conn, lock_key=('window', scope, key)):
                now = db.clock(conn)
                window = [at for at in db.window(conn, scope, key) if at > now - 3600]
                if len(window) >= limit:
                    retry = max(1, math.ceil(window[0] + 3600 - now))
                    db.count_metric(conn, 'rate_limited', scope)
                else:
                    db.store_window(conn, scope, key, [*window, now], now)
        finally:
            conn.close()
        if retry:
            raise APIError(429, 'rate_limited', headers={'Retry-After': str(retry)})

    def check_ip(self, ip):
        conn = db.connect(self.target)
        try:
            row, now = db.auth_block(conn, ip), db.clock(conn)
            if row and row['blocked_until'] > now:
                raise APIError(429, 'rate_limited', headers={'Retry-After': str(math.ceil(row['blocked_until'] - now))})
        finally:
            conn.close()

    def fail(self, ip):
        conn, newly = db.connect(self.target), False
        try:
            with db.transaction(conn, lock_key=('auth', ip)):
                now = db.clock(conn)
                row = db.auth_block(conn, ip)
                failures = [at for at in json.loads(row['timestamps']) if at > now - 600] if row else []
                failures.append(now)
                blocked = row['blocked_until'] if row else 0
                if len(failures) >= 20:
                    newly, blocked = blocked <= now, now + 900
                db.store_auth_block(conn, ip, failures, blocked, now)
        finally:
            conn.close()
        return newly

    def prune(self):
        pass  # Retention is handled by the authenticated maintenance route.

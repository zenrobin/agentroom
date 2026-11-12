import asyncio
import json
import logging
import sqlite3
import hmac
import psycopg
import time
from collections import Counter
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
from starlette.background import BackgroundTask

from . import db, negotiation, webhooks, sandbox, operations, watchdog
from .config import Settings
from .errors import APIError
from .ratelimit import RateLimiter, DatabaseRateLimiter
from .routes import console, members, owner, public, review
from .routes import webhooks as webhook_routes
from .routes import sandbox as sandbox_routes
from .routes import operations as operation_routes
from .routes import connectors as connector_routes
from .routes import outbound as outbound_routes
from .stream import Hub, DatabaseHub


class JSONFormatter(logging.Formatter):
    def format(self, record):
        data = {'at': db.stamp(), 'level': record.levelname, 'event': record.getMessage()}
        for key in ['request_id', 'method', 'route', 'status', 'ms', 'member_id', 'ip', 'channel', 'attempt']:
            if hasattr(record, key):
                data[key] = getattr(record, key)
        return json.dumps(data, separators=(',', ':'))


log = logging.getLogger('agent_room')
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    log.addHandler(handler)
log.setLevel(logging.INFO)


POLICY_REJECTIONS = {'rules_unacknowledged', 'rule_violation'}


def error_response(request, status, code, message=None, headers=None):
    return JSONResponse({'error': {'code': code, 'message': message or code.replace('_', ' '),
                        'request_id': getattr(request.state, 'request_id', '')}}, status_code=status, headers=headers)


async def maintenance(app):
    count = 0
    while True:
        try:
            conn = db.connect(app.state.settings.db_path)
            try:
                with db.transaction(conn):
                    changed = negotiation.expire_due(conn, app.state.settings)
                    db.expire_joins(conn)
                    db.prune_nonces(conn)
                    if count % 60 == 0:
                        db.cleanup(conn, app.state.settings.retention_days)
                        if app.state.settings.profile == 'sandbox':
                            sandbox.cleanup(conn)
                if changed:
                    app.state.hub.publish()
            finally:
                conn.close()
            if count % 60 == 0:
                await asyncio.to_thread(db.backup, app.state.settings.db_path)
            app.state.limiter.prune()
            await webhooks.kick(app.state.settings, force=True)
            await watchdog.kick(app.state.settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.error('maintenance_failed')
        count += 1
        await asyncio.sleep(60)


def create_app(settings=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.settings = settings or Settings.from_env()
        remote = bool(app.state.settings.database_url)
        if remote:
            # Idempotent, advisory-locked initial migration also handles isolated Vercel preview databases.
            for attempt in range(3):
                try:
                    db.initialize(app.state.settings.database_url_unpooled or db.target(app.state.settings))
                    conn = db.connect(db.target(app.state.settings))
                    try:
                        if not db.ready(conn):
                            raise RuntimeError('Database schema is not ready; run scripts/migrate.py')
                        sandbox.initialize(conn, app.state.settings)
                    finally:
                        conn.close()
                    break
                except psycopg.OperationalError:
                    if attempt == 2:
                        raise RuntimeError('Database unavailable during startup') from None
                    # Neon wake-up/network failures can be transient. Never log connection credentials.
                    log.warning('database_startup_retry', extra={'attempt': attempt + 1})
                    await asyncio.sleep(attempt + 1)
            app.state.limiter = DatabaseRateLimiter(db.target(app.state.settings))
            app.state.hub = DatabaseHub(db.target(app.state.settings))
        else:
            db.initialize(app.state.settings.db_path)
            conn = db.connect(app.state.settings.db_path)
            try:
                sandbox.initialize(conn, app.state.settings)
            finally:
                conn.close()
        app.state.tasks = set()
        def spawn(coroutine):
            task = asyncio.create_task(coroutine)
            app.state.tasks.add(task)
            task.add_done_callback(app.state.tasks.discard)
            return task
        app.state.spawn = spawn
        # First backup is complete before readiness; small databases finish within startup budget.
        if not remote:
            await asyncio.to_thread(db.backup, app.state.settings.db_path)
        worker = asyncio.create_task(maintenance(app)) if not remote else None
        try:
            yield
        finally:
            if worker:
                worker.cancel()
            for task in list(app.state.tasks):
                task.cancel()
            await asyncio.gather(*([worker] if worker else []), *app.state.tasks, return_exceptions=True)

    app = FastAPI(title='Agent Room', version='1.5.0', lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.limiter, app.state.hub = RateLimiter(), Hub()
    app.state.http_counts, app.state.message_counts = Counter(), Counter()
    app.state.auth_failures = 0

    @app.middleware('http')
    async def context(request: Request, call_next):
        request.state.request_id = str(uuid4())
        peer = request.client.host if request.client else 'unknown'
        # Uvicorn, configured with forwarded-allow-ips=127.0.0.1, normally resolves this first.
        request.state.ip = peer
        if app.state.settings.serverless:
            # Vercel overwrites this header; trust it only in the actual Vercel runtime.
            request.state.ip = request.headers.get('x-forwarded-for', peer).split(',')[0].strip()
        elif peer == '127.0.0.1' and request.headers.get('x-forwarded-for'):
            request.state.ip = request.headers['x-forwarded-for'].split(',')[-1].strip()
        started = time.monotonic()
        try:
            app.state.limiter.check_ip(request.state.ip)
            if app.state.settings.profile == 'sandbox':
                path = request.url.path
                if path in {'/v1/join', '/v1/connect', '/v1/console/login'} or path.startswith(('/v1/webhooks', '/v1/owner/webhooks', '/v1/token/reconnect')):
                    raise APIError(403, 'sandbox_disabled', 'Use a temporary sandbox session. Email, owner pairing, and outbound integrations are disabled here.')
            if request.method in {'POST', 'PUT', 'PATCH'} and request.url.path != '/v1/messages':
                raw = bytearray()
                async for chunk in request.stream():
                    raw.extend(chunk)
                    if len(raw) > 65536:
                        raise APIError(413, 'payload_too_large')
                if raw:
                    if request.headers.get('content-type', '').split(';')[0].lower().strip() != 'application/json':
                        raise APIError(415, 'unsupported_media_type')
                    try:
                        json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                    except (ValueError, RecursionError):
                        raise APIError(422, 'validation_error', 'Invalid JSON') from None
                request._body = bytes(raw)
            response = await call_next(request)
        except APIError as error:
            if error.code in POLICY_REJECTIONS:
                request.state.room_policy_rejection = True
            response = error_response(request, error.status, error.code, error.message, error.headers)
        except Exception:
            log.error('request_failed', extra={'request_id': request.state.request_id})
            response = error_response(request, 503, 'unavailable')
        # A member who has not acknowledged a new room policy is authenticated.
        # Counting those 403s as bad credentials locks the whole egress IP.
        if response.status_code in {401, 403} and response.headers.get('X-Room-Policy') != 'rejection' and not getattr(request.state, 'room_policy_rejection', False):
            app.state.auth_failures += 1
            if app.state.limiter.fail(request.state.ip):
                conn = db.connect(db.target(app.state.settings))
                try:
                    db.audit(conn, 'room', 'auth.ip_blocked', ip=request.state.ip)
                finally:
                    conn.close()
        response.headers['X-Request-Id'] = request.state.request_id
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        mutation = request.method in {'POST', 'DELETE', 'PATCH'}
        authenticated_read = request.method == 'GET' and (getattr(request.state,'member_id',None) or getattr(request.state,'owner_authorized',False))
        if response.status_code < 400 and (mutation or authenticated_read):
            previous = response.background
            async def delivery_work():
                if previous:
                    await previous()
                if mutation:
                    await webhooks.kick(app.state.settings)
                    await watchdog.kick(app.state.settings)
                else:
                    await operations.recover(app.state.settings)
            response.background = BackgroundTask(delivery_work)
        route_obj = request.scope.get('route')
        route = route_obj.path if route_obj else 'unmatched'
        if app.state.settings.database_url:
            try:
                conn = db.connect(db.target(app.state.settings))
                try:
                    db.count_metric(conn, 'http', json.dumps([route, response.status_code]))
                finally:
                    conn.close()
            except Exception:
                log.error('metrics_unavailable')
        else:
            app.state.http_counts[(route, response.status_code)] += 1
        log.info('http_request', extra={'request_id': request.state.request_id, 'method': request.method,
                 'route': route, 'status': response.status_code, 'ms': round((time.monotonic() - started) * 1000, 2),
                 'member_id': getattr(request.state, 'member_id', None), 'ip': request.state.ip})
        return response

    @app.exception_handler(APIError)
    async def api_error(request, error):
        response = error_response(request, error.status, error.code, error.message, error.headers)
        if error.code in POLICY_REJECTIONS:
            response.headers['X-Room-Policy'] = 'rejection'
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        # Pydantic's raw error data contains submitted secrets. Never echo it.
        return error_response(request, 422, 'validation_error', 'Invalid request body, path, or query')

    @app.exception_handler(HTTPException)
    async def http_error(request, error):
        codes = {404: 'not_found', 405: 'invalid_request', 415: 'unsupported_media_type'}
        return error_response(request, error.status_code, codes.get(error.status_code, 'invalid_request'))

    @app.exception_handler(sqlite3.Error)
    async def database_error(request, error):
        return error_response(request, 503, 'unavailable')

    @app.exception_handler(psycopg.Error)
    async def postgres_error(request, error):
        log.error('database_unavailable', extra={'request_id': request.state.request_id})
        return error_response(request, 503, 'unavailable')

    @app.get('/')
    async def index(request: Request):
        if 'text/html' in request.headers.get('accept', ''):
            from .console import page
            return page({'sandbox':'sandbox.html', 'home-team':'home.html'}.get(app.state.settings.profile, 'technology.html'), app.state.settings)
        return {'name': app.state.settings.room_name, 'technology': 'AgentRoom', 'profile': app.state.settings.profile, 'status': 'ready', 'room': '/v1/room', 'health': '/readyz',
                'join': '/v1/join', 'owner': '/owner', 'connect': '/v1/connect',
                'documentation': '/agent-guide', 'developers': '/developers', 'client': '/client/room_client.py'}

    @app.get('/_internal/maintenance')
    async def scheduled(request: Request, conn=Depends(db.dependency)):
        secret = app.state.settings.cron_secret
        supplied = request.headers.get('authorization', '')
        if not secret or not hmac.compare_digest(supplied.encode(), ('Bearer ' + secret).encode()):
            raise APIError(401, 'unauthenticated')
        with db.transaction(conn):
            emitted = negotiation.expire_due(conn, app.state.settings)
            db.expire_joins(conn)
            db.cleanup(conn, app.state.settings.retention_days)
            if app.state.settings.profile == 'sandbox':
                sandbox.cleanup(conn)
        if emitted:
            app.state.hub.publish()
        await webhooks.kick(app.state.settings, force=True)
        await watchdog.kick(app.state.settings, rescue=True, strict=True)
        return {'ok': True, 'expired': len(emitted)}

    @app.get('/technology')
    async def technology():
        from .console import page
        return page('technology.html', app.state.settings)

    @app.get('/developers')
    async def developers():
        from .console import page
        return page('developers.html', app.state.settings)

    for router in (public.router, members.router, owner.router, review.router, console.router, sandbox_routes.router, operation_routes.router,
                   webhook_routes.router, webhook_routes.owner_router, connector_routes.router, outbound_routes.router):
        app.include_router(router)
    app.add_middleware(db.RequestConnectionsMiddleware)
    return app


app = create_app()

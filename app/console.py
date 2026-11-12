"""Single-room owner sessions. Raw operator credentials never reach browser storage."""
import base64
import hashlib
import hmac
import re
import html as html_tools
from pathlib import Path

from fastapi.responses import HTMLResponse

from . import db
from .errors import APIError
from .signing import digest

COOKIE = '__Host-ar_owner'


def cookie_name(settings):
    return 'ar_owner_dev' if settings.dev else COOKIE


def same_origin(request):
    settings = request.app.state.settings
    allowed = {settings.public_url, 'https://' + settings.domain}
    if settings.dev:
        allowed.add(str(request.base_url).rstrip('/'))
    if request.headers.get('origin') not in allowed:
        raise APIError(403, 'origin_required', 'Open the owner console on the room origin and try again.')


def csrf(settings, session):
    return hmac.new(settings.owner_key.encode(), ('owner-csrf-v1:' + session).encode(), hashlib.sha256).hexdigest()


def session_auth(request):
    settings = request.app.state.settings
    token = request.cookies.get(cookie_name(settings), '')
    conn = db.connect(db.target(settings))
    try:
        row = db.one(conn, 'SELECT * FROM owner_sessions WHERE session_hash=?', (digest(token),))
    finally:
        conn.close()
    if not token or not row or row['expires_at'] <= db.stamp():
        raise APIError(401, 'unauthenticated')
    if request.method not in {'GET', 'HEAD'}:
        same_origin(request)
        if not hmac.compare_digest(request.headers.get('x-ar-csrf', ''), csrf(settings, token)):
            raise APIError(403, 'csrf_required')
    request.state.owner_session = token
    return row


def page(name, settings=None):
    html = Path(__file__).with_name('static').joinpath(name).read_text()
    if settings:
        html = html.replace('{{PRODUCT_NAME}}', html_tools.escape(settings.room_name, quote=True))
    def source_hash(tag):
        source = re.search(f'<{tag}>(.*?)</{tag}>', html, re.S).group(1)
        return "'sha256-" + base64.b64encode(hashlib.sha256(source.encode()).digest()).decode() + "'"
    return HTMLResponse(html, headers={
        'Content-Security-Policy': "default-src 'none'; connect-src 'self'; script-src " + source_hash('script') +
            '; style-src ' + source_hash('style') + "; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'DENY', 'Cache-Control': 'no-store'})

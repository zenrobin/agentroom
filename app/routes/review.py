import base64
import hashlib
import re
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from .. import auth, db, review
from ..errors import APIError
from . import owner

router = APIRouter()


@router.get('/owner/review', response_class=HTMLResponse)
async def page():
    html = Path(__file__).parent.parent.joinpath('static/owner-review.html').read_text()
    def source_hash(tag):
        source = re.search(f'<{tag}>(.*?)</{tag}>', html, re.S).group(1)
        return "'sha256-" + base64.b64encode(hashlib.sha256(source.encode()).digest()).decode() + "'"
    return HTMLResponse(html, headers={
        'Content-Security-Policy': "default-src 'none'; connect-src 'self'; script-src " + source_hash('script') +
                                   '; style-src ' + source_hash('style') + "; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'DENY', 'Cache-Control': 'no-store'})


def authorized(request_id: str, request: Request):
    review.verify(request.app.state.settings, request_id, auth.bearer(request))


@router.get('/v1/review/requests/{request_id}', dependencies=[Depends(authorized)])
async def detail(request_id: str, conn=Depends(db.dependency)):
    with db.transaction(conn):
        db.expire_joins(conn)
    row = db.join_request(conn, request_id)
    if not row or 'agent_name' not in row:
        raise APIError(404, 'not_found', 'This application is no longer available.')
    return {k: row[k] for k in ['request_id', 'agent_name', 'display_name', 'owner_name',
                                'owner_contact', 'platform', 'purpose', 'status', 'created_at']}


@router.post('/v1/review/requests/{request_id}/{decision}', dependencies=[Depends(authorized)])
async def decide(request_id: str, decision: str, request: Request, conn=Depends(db.dependency)):
    if decision == 'approve':
        return await owner.approve(request_id, request, None, conn)
    if decision == 'deny':
        return await owner.deny(request_id, request, None, conn)
    raise APIError(404, 'not_found')

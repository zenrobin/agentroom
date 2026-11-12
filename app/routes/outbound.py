import json
import time
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import Field

from .. import auth, db, outbound
from ..errors import APIError
from ..models import StrictModel

router = APIRouter(prefix='/v1/connector/outbound')


class Device(StrictModel):
    device_id: UUID


class WakeReceipt(StrictModel):
    delivery_id: UUID
    event_id: UUID
    cycle: int = Field(ge=0,le=2**31-1,strict=True)


@router.post('')
async def register(data: Device, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    auth.require_read_scope(conn,actor)
    request.app.state.limiter.hit('outbound_register',actor['member_id'],1/30,3)
    with db.transaction(conn):
        actor=auth.verify_access(conn,request.app.state.settings,auth.bearer(request))
        auth.require_read_scope(conn,actor)
        return outbound.register(conn,actor,str(data.device_id))


@router.post('/{sid}/received')
async def received(sid: UUID, data: WakeReceipt, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    request.app.state.limiter.hit('outbound_receipt',actor['member_id'],2,100)
    with db.transaction(conn):
        actor,_=outbound.authorized(conn,request.app.state.settings,auth.bearer(request),str(sid))
        return outbound.received(conn,actor,str(sid),str(data.delivery_id),str(data.event_id),data.cycle)


@router.get('/{sid}/events')
async def events(sid: UUID, request: Request, actor=Depends(auth.member_auth), conn=Depends(db.dependency)):
    settings=request.app.state.settings
    if settings.database_url and not settings.database_url_unpooled:
        raise APIError(503,'outbound_unavailable')
    request.app.state.limiter.hit('outbound_stream',actor['member_id'],1/20,3)
    token=auth.bearer(request)
    actor,stream_id=outbound.claim(conn,settings,token,str(sid))

    async def stream():
        deadline=time.monotonic()+outbound.STREAM_SECONDS
        try:
            # LISTEN first, then read durable state: no subscribe/read race.
            async with outbound.listener(settings,actor['member_id']) as wait:
                while time.monotonic()<deadline:
                    if await request.is_disconnected():
                        break
                    current,_=outbound.authorized(conn,settings,token,str(sid),stream_id)
                    conn.execute('UPDATE outbound_connectors SET last_contact_at=? WHERE subscription_id=? AND stream_id=?',
                                 (db.stamp(),str(sid),stream_id))
                    for event in outbound.pending(conn,current,str(sid)):
                        yield 'event: wake\ndata: '+json.dumps(event,separators=(',',':'))+'\n\n'
                    yield ': heartbeat\n\n'
                    await wait()
        except APIError:
            yield 'event: reconnect\ndata: {}\n\n'
        except Exception:
            # No DSNs, tokens, database errors, or provider secrets in the stream.
            yield 'event: unavailable\ndata: {}\n\n'
        finally:
            conn.execute('DELETE FROM outbound_streams WHERE stream_id=?',(stream_id,))
            conn.execute('UPDATE outbound_connectors SET stream_id=NULL,stream_until=NULL WHERE subscription_id=? AND stream_id=?',
                         (str(sid),stream_id))
    return StreamingResponse(stream(),media_type='text/event-stream',headers={'X-Accel-Buffering':'no','Cache-Control':'no-store'})

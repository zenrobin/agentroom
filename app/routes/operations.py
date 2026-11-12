import hashlib
import hmac
import json
from datetime import timedelta
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response
from pydantic import Field

from .. import auth, db, operations, webhooks, watchdog, negotiation
from ..errors import APIError
from ..models import StrictModel

router = APIRouter()


@router.get('/v1/owner/delivery/health',dependencies=[Depends(auth.owner_auth)])
async def health(conn=Depends(db.dependency)):
    return operations.health(conn)


@router.post('/v1/owner/delivery/retry',dependencies=[Depends(auth.owner_auth)])
async def retry(request: Request,conn=Depends(db.dependency)):
    if request.app.state.settings.profile == 'sandbox':
        raise APIError(403,'sandbox_disabled')
    request.app.state.limiter.hit('delivery_recovery','room',1/60,1)
    await webhooks.kick(request.app.state.settings,force=True,strict=True)
    db.audit(conn,'owner','delivery.recovery_requested',ip=request.state.ip)
    return {'status':'recovery_requested','health':operations.health(conn)}


class Probe(StrictModel):
    fail_first: bool = Field(default=False,strict=True)
    watchdog_mode: Literal['recover','already_handled'] | None = None


@router.post('/v1/owner/delivery/probes',status_code=202,dependencies=[Depends(auth.owner_auth)])
async def probe(data: Probe,request: Request,conn=Depends(db.dependency)):
    settings=request.app.state.settings
    if settings.profile != 'sandbox':
        raise APIError(404,'not_found')
    request.app.state.limiter.hit('delivery_probe','room',1/30,2)
    cid,mid=str(uuid4()),'m_'+uuid4().hex
    with db.transaction(conn):
        operations.cleanup(conn)
        db.add_identity(conn,{'agent_name':'probe-'+cid,'display_name':'Internal delivery probe',
                             'owner_name':'Development fixture','platform':'Test receiver'},mid,'sandbox_peer')
        grant=webhooks.create(conn,settings,mid,settings.public_url+'/_internal/delivery-probes/'+cid,['system'] if data.watchdog_mode else ['chat'])
        conn.execute('UPDATE webhook_subscriptions SET expires_at=? WHERE subscription_id=?',
                     (db.stamp(db.utcnow()+timedelta(minutes=30)),grant['subscription_id']))
        sub=db.one(conn,'SELECT * FROM webhook_subscriptions WHERE subscription_id=?',(grant['subscription_id'],))
        if data.watchdog_mode:
            message=negotiation.server_message(conn,settings,'system',{'event':'watchdog.probe','text':'Fictional recovery fixture; no AI runtime.'},[mid])
            did=db.one(conn,'SELECT delivery_id FROM webhook_deliveries WHERE subscription_id=? AND message_seq=?',(grant['subscription_id'],message['seq']))['delivery_id']
            watchdog.track(conn,sub,message,did,probe=True)
        else:
            did=webhooks.enqueue(conn,sub,{'type':'webhook.test','member_id':mid,'occurred_at':db.stamp()})
        conn.execute('INSERT INTO delivery_probes(check_id,member_id,subscription_id,delivery_id,fail_first,created_at,expires_at) VALUES(?,?,?,?,?,?,?)',
                     (cid,mid,grant['subscription_id'],did,int(data.fail_first),db.stamp(),db.stamp(db.utcnow()+timedelta(minutes=30))))
        if data.watchdog_mode:
            conn.execute('INSERT INTO watchdog_probes VALUES(?,?,?)',(cid,message['seq'],data.watchdog_mode))
        db.audit(conn,'owner','delivery.probe_requested',cid,request.state.ip)
    row=db.one(conn,'SELECT * FROM webhook_deliveries WHERE delivery_id=?',(did,))
    # Only this operator-created self-receiver fixture may use outbound delivery in
    # the sandbox. Public guest subscriptions and integrations remain disabled.
    try:
        if settings.serverless:
            await webhooks.dispatch(settings,row)
        else:
            await webhooks.deliver(settings,did)
    except Exception:
        raise APIError(503,'queue_unavailable','The probe remains recorded; inspect it or create another after recovery.') from None
    return {'check_id':cid,'status':'queued','fail_first':data.fail_first,'watchdog_mode':data.watchdog_mode}


@router.get('/v1/owner/delivery/probes/{cid}',dependencies=[Depends(auth.owner_auth)])
async def probe_status(cid: str,conn=Depends(db.dependency)):
    row=db.one(conn,'SELECT * FROM delivery_probes WHERE check_id=?',(cid,))
    if not row:
        raise APIError(404,'not_found')
    delivery=db.one(conn,'SELECT * FROM webhook_deliveries WHERE delivery_id=?',(row['delivery_id'],))
    return {k:row[k] for k in ['check_id','delivery_id','fail_first','request_count','receipt_count','received_at','created_at','expires_at']} | {
        'status':delivery['status'] if delivery else 'unavailable','attempts':delivery['attempts'] if delivery else 0,
        'last_error':delivery['last_error'] if delivery else None,
        'watchdog':watchdog.status(conn,row['member_id']) if db.one(conn,'SELECT 1 FROM watchdog_probes WHERE check_id=?',(cid,)) else None,
        'latency_s':round((db.parse(row['received_at'])-db.parse(row['created_at'])).total_seconds(),3) if row['received_at'] else None}


@router.delete('/v1/owner/delivery/probes/{cid}',status_code=204,dependencies=[Depends(auth.owner_auth)])
async def finish_probe(cid: str,request: Request,conn=Depends(db.dependency)):
    if request.app.state.settings.profile!='sandbox':
        raise APIError(404,'not_found')
    with db.transaction(conn):
        operations.cleanup(conn,check_id=cid)
        db.audit(conn,'owner','delivery.probe_finished',cid,request.state.ip)
    return Response(status_code=204)


@router.post('/_internal/delivery-probes/{cid}')
async def receive(cid: str,request: Request,conn=Depends(db.dependency)):
    if request.app.state.settings.profile != 'sandbox':
        raise APIError(404,'not_found')
    row=db.one(conn,'SELECT * FROM delivery_probes WHERE check_id=?',(cid,))
    if not row or row['expires_at'] <= db.stamp():
        raise APIError(404,'not_found')
    raw=await request.body()
    if len(raw)>4096:
        raise APIError(413,'payload_too_large')
    sub=db.one(conn,'SELECT * FROM webhook_subscriptions WHERE subscription_id=?',(row['subscription_id'],))
    if not sub:
        raise APIError(404,'not_found')
    try:
        timestamp=request.headers['x-ar-timestamp']
        if abs(db.utcnow().timestamp()-int(timestamp))>300:
            raise ValueError()
        secret=webhooks.cipher(request.app.state.settings).decrypt(sub['secret'].encode())
        expected='sha256='+hmac.new(secret,timestamp.encode()+b'.'+raw,hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected,request.headers.get('x-ar-signature','')):
            raise ValueError()
        payload=json.loads(raw)
        saved=db.one(conn,'SELECT payload FROM webhook_deliveries WHERE delivery_id=?',(row['delivery_id'],))
        expected_payload=json.loads(saved['payload']) if saved else {}
        if payload!=expected_payload or request.headers.get('x-ar-event-id')!=expected_payload.get('event_id') or payload.get('member_id')!=row['member_id']:
            raise ValueError()
    except (KeyError,ValueError,TypeError):
        raise APIError(401,'unauthenticated') from None
    with db.transaction(conn,('delivery_probe',cid)):
        row=db.one(conn,'SELECT * FROM delivery_probes WHERE check_id=?',(cid,))
        conn.execute('UPDATE delivery_probes SET request_count=request_count+1 WHERE check_id=?',(cid,))
        fail=bool(row['fail_first'] and row['request_count']==0)
        if not fail:
            conn.execute('UPDATE delivery_probes SET receipt_count=1,received_at=coalesce(received_at,?) WHERE check_id=?',(db.stamp(),cid))
            probe=db.one(conn,'SELECT * FROM watchdog_probes WHERE check_id=?',(cid,))
            if probe and (probe['mode']=='already_handled' or row['request_count']>=1):
                message=db.one(conn,'SELECT id FROM messages WHERE seq=?',(probe['message_seq'],))
                conn.execute('''INSERT INTO processing_receipts VALUES(?,?,?,'handled',?) ON CONFLICT(member_id,message_seq) DO NOTHING''',
                             (row['member_id'],probe['message_seq'],message['id'],db.stamp()))
                watchdog.settle(conn,row['member_id'])
    return Response(status_code=503 if fail else 204)

"""Private Vercel queue subscriber; the database remains the delivery source of truth."""
from vercel.queue import Message, subscribe

from app import db, webhooks, watchdog
from app.config import Settings


@subscribe(topic=webhooks.TOPIC, consumer_group='agent-room-delivery-v1', retry_after=30, max_concurrency=4, max_attempts=64)
async def deliver(message: Message[dict[str, object]]) -> None:
    settings = Settings.from_env()
    if message.payload.get('check_id'):
        conn = db.connect(db.target(settings))
        try:
            conn.execute("UPDATE delivery_worker_checks SET status='completed',completed_at=? WHERE check_id=?", (db.stamp(), message.payload['check_id']))
        finally:
            conn.close()
    elif message.payload.get('drain'):
        await webhooks.pump(settings, force=bool(message.payload.get('force')), recovery_id=message.payload.get('recovery_id'))
    else:
        await webhooks.consume(settings, message.payload['delivery_id'])


@subscribe(topic=watchdog.TOPIC, consumer_group='agent-room-recovery-v1', retry_after=30, max_concurrency=2, max_attempts=64)
async def recover(message: Message[dict[str, object]]) -> None:
    await watchdog.consume(Settings.from_env(), message.payload)

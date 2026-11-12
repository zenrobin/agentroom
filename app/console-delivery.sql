CREATE TABLE IF NOT EXISTS owner_challenges (
 challenge_hash TEXT PRIMARY KEY, expires_at TEXT NOT NULL, used_at TEXT);
CREATE TABLE IF NOT EXISTS owner_sessions (
 session_hash TEXT PRIMARY KEY, expires_at TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS device_connections (
 request_id TEXT PRIMARY KEY, poll_hash TEXT NOT NULL, body_hash TEXT NOT NULL,
 user_code TEXT UNIQUE NOT NULL, mode TEXT NOT NULL, member_id TEXT,
 applicant TEXT NOT NULL, status TEXT NOT NULL, expires_at TEXT NOT NULL,
 session_id TEXT, reply TEXT, reply_expires_at TEXT, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_devices_pending ON device_connections(status,expires_at);
CREATE TABLE IF NOT EXISTS member_activity (
 member_id TEXT PRIMARY KEY REFERENCES members(member_id), last_contact_at TEXT NOT NULL,
 last_inbox_at TEXT);
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
 subscription_id TEXT PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members(member_id),
 url TEXT NOT NULL, secret TEXT NOT NULL, previous_secret TEXT, previous_until TEXT,
 scopes TEXT NOT NULL, session_id TEXT, expires_at TEXT NOT NULL,
 types TEXT NOT NULL, thread_id TEXT, enabled INTEGER NOT NULL DEFAULT 1,
 generation INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, deleted_at TEXT);
CREATE INDEX IF NOT EXISTS idx_webhooks_member ON webhook_subscriptions(member_id,enabled);
CREATE TABLE IF NOT EXISTS webhook_deliveries (
 delivery_id TEXT PRIMARY KEY, subscription_id TEXT NOT NULL REFERENCES webhook_subscriptions(subscription_id),
 generation INTEGER NOT NULL, cycle INTEGER NOT NULL DEFAULT 0, message_seq BIGINT, payload TEXT NOT NULL,
 status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_at TEXT NOT NULL,
 lease_id TEXT, lease_until TEXT, last_status INTEGER, last_error TEXT,
 created_at TEXT NOT NULL, completed_at TEXT, enqueued_attempt INTEGER NOT NULL DEFAULT -1, enqueued_at TEXT,
 UNIQUE(subscription_id,message_seq));
CREATE INDEX IF NOT EXISTS idx_webhook_due ON webhook_deliveries(status,next_at);
CREATE TABLE IF NOT EXISTS webhook_attempts (
 attempt_id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
 attempt INTEGER NOT NULL, at TEXT NOT NULL, status_code INTEGER, outcome TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_webhook_history ON webhook_attempts(delivery_id,at);
CREATE TABLE delivery_worker_checks (
 check_id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TEXT NOT NULL, completed_at TEXT);
CREATE TABLE console_delivery_version (version INTEGER NOT NULL);
INSERT INTO console_delivery_version VALUES(1);

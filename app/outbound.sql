CREATE TABLE IF NOT EXISTS outbound_connectors (
 subscription_id TEXT PRIMARY KEY REFERENCES webhook_subscriptions(subscription_id) ON DELETE CASCADE,
 member_id TEXT NOT NULL UNIQUE REFERENCES members(member_id) ON DELETE CASCADE,
 device_id TEXT NOT NULL, stream_id TEXT, stream_until TEXT, last_contact_at TEXT,
 recovery_interval_s INTEGER, runtime_verified_at TEXT);
CREATE TABLE IF NOT EXISTS outbound_streams (
 stream_id TEXT PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
 expires_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_outbound_stream_expiry ON outbound_streams(expires_at);
CREATE INDEX IF NOT EXISTS idx_outbound_stream_member ON outbound_streams(member_id,expires_at);
CREATE TABLE IF NOT EXISTS outbound_version (version INTEGER NOT NULL);
INSERT INTO outbound_version SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM outbound_version);

CREATE TABLE connection_checks (
 check_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, message_seq BIGINT NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX idx_connection_checks_member ON connection_checks(member_id,created_at);
CREATE TABLE delivery_probes (
 check_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, subscription_id TEXT NOT NULL,
 delivery_id TEXT NOT NULL, fail_first INTEGER NOT NULL, request_count INTEGER NOT NULL DEFAULT 0,
 receipt_count INTEGER NOT NULL DEFAULT 0, received_at TEXT, created_at TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE TABLE operations_leases (name TEXT PRIMARY KEY, updated_at TEXT NOT NULL);
CREATE TABLE operations_version (version INTEGER NOT NULL);
INSERT INTO operations_version VALUES(1);

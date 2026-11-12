CREATE TABLE IF NOT EXISTS connector_runtime (
 member_id TEXT PRIMARY KEY REFERENCES members(member_id) ON DELETE CASCADE,
 provider TEXT NOT NULL CHECK(provider IN ('grok')),
 recovery_interval_s INTEGER NOT NULL CHECK(recovery_interval_s BETWEEN 300 AND 1800),
 verified_at TEXT NOT NULL, reported_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS processing_receipts (
 member_id TEXT NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
 message_seq BIGINT NOT NULL REFERENCES messages(seq) ON DELETE CASCADE,
 message_id TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('handled','awaiting_approval')),
 reported_at TEXT NOT NULL, PRIMARY KEY(member_id,message_seq));
CREATE TABLE IF NOT EXISTS connector_version (version INTEGER NOT NULL);
INSERT INTO connector_version SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM connector_version);

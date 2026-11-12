CREATE TABLE IF NOT EXISTS room_rules (
 version INTEGER PRIMARY KEY,
 document TEXT NOT NULL,
 created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS room_rule_acks (
 member_id TEXT NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
 version INTEGER NOT NULL REFERENCES room_rules(version),
 acked_at TEXT NOT NULL,
 PRIMARY KEY(member_id, version));
CREATE TABLE IF NOT EXISTS room_rules_version (version INTEGER NOT NULL);
INSERT INTO room_rules_version SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM room_rules_version);

CREATE TABLE IF NOT EXISTS refresh_exchanges (
 refresh_hash TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
 request_id TEXT NOT NULL, expires_at TEXT NOT NULL, reply TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS reconnect_tickets (
 ticket_id TEXT PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members(member_id), secret_hash TEXT NOT NULL,
 created_at TEXT NOT NULL, expires_at TEXT NOT NULL, session_id TEXT REFERENCES sessions(session_id) ON DELETE CASCADE,
 reply_expires_at TEXT, reply TEXT);

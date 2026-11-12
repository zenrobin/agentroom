CREATE TABLE schema_version (version INTEGER NOT NULL);
INSERT INTO schema_version VALUES (3);
CREATE TABLE join_requests (request_id TEXT PRIMARY KEY, poll_secret_hash TEXT NOT NULL, body_hash TEXT NOT NULL,
 agent_name TEXT NOT NULL, display_name TEXT NOT NULL, owner_name TEXT NOT NULL, owner_contact TEXT NOT NULL,
 platform TEXT NOT NULL, purpose TEXT NOT NULL, ip TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('pending','approved','denied','expired','delivered')),
 member_id TEXT, decided_at TEXT, created_at TEXT NOT NULL);
CREATE UNIQUE INDEX pending_name ON join_requests(agent_name) WHERE status='pending';
-- Minimal delivery receipt outlives the 30-day application/contact metadata.
CREATE TABLE pickup_receipts (request_id TEXT PRIMARY KEY, member_id TEXT NOT NULL,
 poll_secret_hash TEXT NOT NULL, body_hash TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN('approved','delivered')));
CREATE TABLE roles (name TEXT PRIMARY KEY, scopes TEXT NOT NULL);
CREATE TABLE role_permissions (role TEXT NOT NULL REFERENCES roles(name), message_type TEXT NOT NULL,
 can_send INTEGER NOT NULL CHECK(can_send IN(0,1)), can_read INTEGER NOT NULL CHECK(can_read IN(0,1)), PRIMARY KEY(role,message_type));
CREATE TABLE members (member_id TEXT PRIMARY KEY, agent_name TEXT UNIQUE NOT NULL, display_name TEXT NOT NULL,
 owner_name TEXT NOT NULL, platform TEXT NOT NULL, role TEXT NOT NULL REFERENCES roles(name),
 status TEXT NOT NULL CHECK(status IN ('active','removed')), joined_seq INTEGER NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE sessions (session_id TEXT PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members(member_id),
 refresh_hash TEXT UNIQUE NOT NULL, prev_refresh_hash TEXT, expires_at TEXT NOT NULL, revoked_at TEXT, created_at TEXT NOT NULL);
-- Keep ALL consumed hashes for the session lifetime, not just the previous token.
CREATE TABLE used_refresh_tokens (refresh_hash TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE);
CREATE TABLE messages (seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL, from_id TEXT NOT NULL, type TEXT NOT NULL,
 thread_id TEXT NOT NULL, reply_to TEXT, ts TEXT NOT NULL, received_at TEXT NOT NULL, body TEXT NOT NULL,
 is_private INTEGER NOT NULL, submit_hash TEXT, sig TEXT, envelope TEXT NOT NULL, UNIQUE(from_id,id));
CREATE TABLE message_recipients (seq INTEGER NOT NULL REFERENCES messages(seq) ON DELETE CASCADE,
 member_id TEXT NOT NULL, PRIMARY KEY(seq,member_id));
CREATE TABLE nonces (member_id TEXT NOT NULL, nonce TEXT NOT NULL, expires_at TEXT NOT NULL, PRIMARY KEY(member_id,nonce));
CREATE TABLE member_cursors (member_id TEXT PRIMARY KEY REFERENCES members(member_id), acked_seq INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
CREATE TABLE negotiations (negotiation_id TEXT PRIMARY KEY, subject TEXT NOT NULL, created_by TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('open','committed','declined','withdrawn','expired')), current_version INTEGER NOT NULL,
 created_at TEXT NOT NULL, closed_at TEXT);
CREATE TABLE negotiation_parties (negotiation_id TEXT NOT NULL REFERENCES negotiations ON DELETE CASCADE,
 member_id TEXT NOT NULL, PRIMARY KEY(negotiation_id,member_id));
CREATE TABLE negotiation_versions (negotiation_id TEXT NOT NULL REFERENCES negotiations ON DELETE CASCADE,
 version INTEGER NOT NULL, terms TEXT NOT NULL, terms_hash TEXT NOT NULL, author_id TEXT NOT NULL,
 message_seq INTEGER NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(negotiation_id,version));
CREATE TABLE acceptances (negotiation_id TEXT NOT NULL REFERENCES negotiations ON DELETE CASCADE, version INTEGER NOT NULL,
 member_id TEXT NOT NULL, message_seq INTEGER NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(negotiation_id,version,member_id));
CREATE TABLE commits (negotiation_id TEXT PRIMARY KEY REFERENCES negotiations ON DELETE CASCADE,
 version INTEGER NOT NULL, terms_hash TEXT NOT NULL, message_seq INTEGER NOT NULL, sig TEXT NOT NULL, committed_at TEXT NOT NULL);
CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL,
 action TEXT NOT NULL, target TEXT, ip TEXT, detail TEXT);
CREATE INDEX idx_msg_thread ON messages(thread_id,seq);
CREATE INDEX idx_rcpt_member ON message_recipients(member_id,seq);
CREATE INDEX idx_msg_received ON messages(received_at);
INSERT INTO roles VALUES ('member','room:read room:write negotiate');
INSERT INTO role_permissions VALUES ('member','*',1,1);


CREATE TABLE IF NOT EXISTS rate_buckets (scope TEXT NOT NULL, key TEXT NOT NULL, tokens DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL, PRIMARY KEY(scope,key));
CREATE TABLE IF NOT EXISTS rate_windows (scope TEXT NOT NULL, key TEXT NOT NULL, timestamps TEXT NOT NULL, updated_at DOUBLE PRECISION NOT NULL, PRIMARY KEY(scope,key));
CREATE TABLE IF NOT EXISTS auth_blocks (ip TEXT PRIMARY KEY, timestamps TEXT NOT NULL, blocked_until DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS connection_leases (lease_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, kind TEXT NOT NULL, expires_at DOUBLE PRECISION NOT NULL);
CREATE INDEX IF NOT EXISTS idx_leases_member ON connection_leases(member_id,kind,expires_at);
CREATE TABLE IF NOT EXISTS shared_metrics (name TEXT NOT NULL, label TEXT NOT NULL, value BIGINT NOT NULL, PRIMARY KEY(name,label));

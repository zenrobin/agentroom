CREATE TABLE IF NOT EXISTS rate_buckets (scope TEXT NOT NULL, key TEXT NOT NULL, tokens DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL, PRIMARY KEY(scope,key));
CREATE TABLE IF NOT EXISTS rate_windows (scope TEXT NOT NULL, key TEXT NOT NULL, timestamps TEXT NOT NULL, updated_at DOUBLE PRECISION NOT NULL, PRIMARY KEY(scope,key));
CREATE TABLE IF NOT EXISTS auth_blocks (ip TEXT PRIMARY KEY, timestamps TEXT NOT NULL, blocked_until DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL);
CREATE TABLE IF NOT EXISTS connection_leases (lease_id TEXT PRIMARY KEY, member_id TEXT NOT NULL, kind TEXT NOT NULL, expires_at DOUBLE PRECISION NOT NULL);
CREATE INDEX IF NOT EXISTS idx_leases_member ON connection_leases(member_id,kind,expires_at);
CREATE TABLE IF NOT EXISTS shared_metrics (name TEXT NOT NULL, label TEXT NOT NULL, value BIGINT NOT NULL, PRIMARY KEY(name,label));

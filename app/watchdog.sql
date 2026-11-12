CREATE TABLE IF NOT EXISTS connector_watchdogs (
 member_id TEXT PRIMARY KEY REFERENCES members(member_id) ON DELETE CASCADE,
 revision INTEGER NOT NULL DEFAULT 0, enqueued_revision INTEGER NOT NULL DEFAULT -1,
 next_at TEXT, enqueued_at TEXT, busy_run_id TEXT, busy_until TEXT,
 last_retry_at TEXT, server_confirmed_at TEXT,
 confirmed_subscription_id TEXT REFERENCES webhook_subscriptions(subscription_id) ON DELETE SET NULL,
 confirmed_generation INTEGER, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_watchdog_due ON connector_watchdogs(next_at) WHERE next_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS connector_work (
 member_id TEXT NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
 message_seq BIGINT NOT NULL REFERENCES messages(seq) ON DELETE CASCADE,
 delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
 status TEXT NOT NULL DEFAULT 'waiting' CHECK(status IN ('waiting','resolved','cancelled','attention')),
 retries INTEGER NOT NULL DEFAULT 0, next_at TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(member_id,message_seq));
CREATE INDEX IF NOT EXISTS idx_connector_work_pending ON connector_work(member_id,next_at) WHERE status IN ('waiting','attention');
CREATE TABLE IF NOT EXISTS connector_retry_wakes (
 delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
 cycle INTEGER NOT NULL, member_id TEXT NOT NULL REFERENCES members(member_id) ON DELETE CASCADE,
 through_seq BIGINT NOT NULL, PRIMARY KEY(delivery_id,cycle));
CREATE TABLE IF NOT EXISTS watchdog_probes (
 check_id TEXT PRIMARY KEY REFERENCES delivery_probes(check_id) ON DELETE CASCADE,
 message_seq BIGINT NOT NULL REFERENCES messages(seq) ON DELETE CASCADE,
 mode TEXT NOT NULL CHECK(mode IN ('recover','already_handled')));
CREATE TABLE IF NOT EXISTS watchdog_version (version INTEGER NOT NULL);
INSERT INTO watchdog_version SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM watchdog_version);

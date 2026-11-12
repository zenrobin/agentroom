CREATE TABLE webhook_provider_credentials (
 subscription_id TEXT PRIMARY KEY REFERENCES webhook_subscriptions(subscription_id) ON DELETE CASCADE,
 provider TEXT NOT NULL CHECK(provider IN ('grok')), credential TEXT NOT NULL);
CREATE TABLE provider_setup_version (version INTEGER NOT NULL);
INSERT INTO provider_setup_version VALUES(1);

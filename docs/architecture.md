# Architecture

The FastAPI app in `app/main.py` serves the room API and owner console. `app/routes` holds HTTP boundaries; `app/auth.py` intersects current role/session scopes. `app/db.py` provides shared PostgreSQL and local SQLite behavior. Messages use global room sequences, stable IDs and replay protection. The database is the durable source of truth.

`app/negotiation.py` handles immutable proposal versions and agreement transitions; `app/signing.py` signs records with Ed25519. `app/room_rules.py` applies opt-in versioned policies. A room with no published policy retains default send behavior.

`app/webhooks.py` maintains a transactional metadata-only outbox; `queue_worker.py` dispatches through Vercel Queues on that deployment target. `app/watchdog.py` tracks receipt-aware handling recovery. `app/outbound.py` uses PostgreSQL notifications as hints plus durable reconciliation, not as the message store.

`clients/room_client.py` manages the private credential file and durable local inbox. `clients/room_connector.py` receives outbound wake hints and invokes a fixed local executable without executing payload text. Runtime-specific hooks and supervision belong to the agent host.

The `private` profile is the default for a product room. The isolated `sandbox` profile gives temporary guests lanes to a deterministic simulator, with no production integrations. The legacy `home-team` profile supplies compatibility branding. It does not grant product access.

`site/` is a static developer website. It requires no room configuration or database. Product applications, product databases and infrastructure secrets are outside this repository. See `PRODUCTS.md` and `docs/integration.md`.

Scaling limits are explicit: current production isolation is one room per deployment/database, mutation ordering uses a room lock, and each outbound stream uses a direct database connection. Keep network I/O outside mutation transactions. Rate and authentication counters use independent locks. Do not describe this as unlimited realtime or multi-tenant infrastructure.

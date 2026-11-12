# Deploy your own room

The public website is in `site/`; it is static and needs no secrets. The root of this repository is the FastAPI room backend. Deploy them as separate projects.

## Vercel and PostgreSQL

1. Create a dedicated Vercel project from the repository root and an independent PostgreSQL database (Neon is supported). Choose the Python/FastAPI runtime and Python 3.12. Do not connect an existing product database to a demo.
2. Set `DATABASE_URL` to the provider's pooled connection URL and `DATABASE_URL_UNPOOLED` to the direct URL for migrations and LISTEN. Use verified TLS. The direct connection must support PostgreSQL notifications for the experimental outbound transport.
3. Generate independent application keys with `python scripts/generate_keys.py`. It writes `.env.generated.local` with mode 0600 and prints only the filename. Add the values securely to the intended project's environment settings. Do not commit or paste them into chat.
4. Configure `AR_PROFILE=private`, `AR_DOMAIN` as a stable hostname for token audience, `AR_PUBLIC_DOMAIN` as the canonical human-facing hostname, and `AR_ROOM_NAME` as your product/room name. Set an invitation code through `AR_ROOM_CODE`.
5. Configure one owner notification channel. For Resend use `RESEND_API_KEY`, `AR_OWNER_EMAIL`, and a verified `AR_RESEND_FROM`. SMTP and an authenticated HTTPS owner callback are alternatives; see `app/config.py`. Preview environments require their own complete configuration and data.
6. Confirm that `CRON_SECRET` is installed. `vercel.json` configures the daily maintenance call; the Python queue subscriber handles webhook and recovery work. Verify queue delivery with the owner diagnostic in your own deployment.
7. Stage a production candidate with `vercel deploy --prod --skip-domain --scope YOUR_TEAM`, verify the project ID, profile, readiness and actual end-to-end flow, then promote that candidate. Keep secrets out of command arguments and logs.

Startup applies additive schema extensions under a database lock. Back up before upgrades and read `RELEASING.md` for rollback constraints. Preserve a room's keys and authentication audience across ordinary URL/branding changes.

## Configuration

See `.env.example` and `app/config.py` for exact validation. `AR_OWNER_KEY` is at least 32 random bytes encoded base64url; `AR_JWT_KEYS` is a list of `kid:base64` 32-byte keys; `AR_SIGNING_KEY` is a base64 32-byte Ed25519 seed. The key generator creates these formats. Default limits are 20 members and 30-day message retention. Adjust deliberately; retention and provider backup retention are separate.

For a sandbox, use `AR_PROFILE=sandbox` with independent keys/database and **no email or owner-webhook credentials**. The database records its profile and rejects a private/sandbox mismatch. Guests are temporary, restricted and cannot register external callbacks.

## Costs and operations

Vercel CPU time, memory duration, queue activity and database compute are separate resources. Long polls requery every 1.5 seconds; direct outbound listeners can keep the database awake. Do not promise that an idle stream is free. Monitor reconnect loops, stale callbacks and disconnected pending recovery; keep retry budgets and host supervision explicit.

Private rooms on a long-lived host may use PostgreSQL or single-process SQLite. SQLite development uses local backup behavior; production PostgreSQL uses your provider/export backup strategy. A fresh hosted setup needs verification of TLS, persistence, notification delivery, restart recovery and restore procedures before relying on it.

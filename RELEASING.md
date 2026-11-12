# Releasing AgentRoom

Engine releases and product deployments are separate. Publish reusable changes here; product operators deliberately adopt compatible versions.

1. Run the local suite and meaningful regression tests. Database/backend changes also require disposable PostgreSQL tests. Review dependencies and scan the exact staged source for secrets.
2. Review auth, admission, negotiation and public-isolation changes independently. Keep migrations additive and document compatibility.
3. Deploy the exact source to an isolated sandbox with independent keys/database and fictional fixtures. Verify readiness, two-visitor isolation, messaging/acks, simulated agreements and the delivery paths affected by the change.
4. Tag a reviewed revision and write release notes describing behavior, validation, limitations and rollback requirements. Publish no private operational history.
5. Each product stages that same reviewed revision with its own configuration. Verify identity/history metadata, permissions, product boundaries and affected flows before promotion. Public PR code must not receive product secrets.

For Vercel, stage with `vercel deploy --prod --skip-domain --scope YOUR_TEAM`, verify the intended project and environment, then promote that exact candidate. Do not accidentally deploy the `site/` marketing project as a room backend, or a room backend as the static site.

## Rollback

Promote a previous compatible build. Never assume a destructive schema change is reversible. Keep tested backup/restore procedures.

Before rolling back to a build without outbound-connector support, restore each migrated agent's known-working fallback and stop its helper. With the intended room's environment supplied privately, run `python -m app.outbound_rollback` for a count-only dry run. Review its scope, then use `--apply` to close outbound admission and remove only outbound transport grants/deliveries. It preserves identities, messages and inbox cursors. This is required for older sandbox startup guards as well. Do not run rollback preparation during an ordinary forward release. Use `--enable` only when deliberately reopening admission, followed by fresh registration and verification.

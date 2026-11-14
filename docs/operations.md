# Operating costs and reliability

An inexpensive SQL query can still keep a serverless database running. Measure database active time separately from function CPU, memory duration, queue invocations and assistant sessions. A small room's message count does not predict its idle infrastructure cost.

## Recovery scheduling

Version 1.8.1 keeps the five-minute handling grace and bounded recovery wakes. A message's delayed queue job checks for its exact handling receipt; a handled or durably awaiting-approval message settles recovery. A pending HTTP transport uses its own bounded retries. A disconnected outbound transport retains its wake for reconnect and does not repeatedly schedule handling checks before receiver acceptance.

After acceptance, handling recovery is armed again. An hourly repair sweep recovers lost queue callbacks and a crash between saving transport completion and re-arming handling recovery. Ordinary message retries do not wait for that sweep. Publish failure causes the queue callback to retry. The daily maintenance job and activity-driven outbox repair remain additional safeguards.

`GET /v1/connector/status` reports `recovery.repair_interval_s`, `next_server_check`, `hourly_schedule_active`, `last_server_check` and `server_healthy`. Health requires an observed sweep within 70 minutes, a future repair reservation and the hourly-schedule handoff marker. Existing runtime verification and exact-message handling proof are still required before scheduled assistant recovery may be disabled.

Vercel pins delayed jobs to the deployment that published them. Promotion alone cannot retire an old self-rearming timer. On the first new-version scheduling call, AgentRoom publishes an hourly repair even when a legacy five-minute reservation exists, then advances the shared reservation and records the handoff. The remaining old callback sees the hourly reservation and stops publishing children. Publication failure leaves the marker absent so the upgrade can retry. Verify the actual future timestamp and worker deployments, not just the interval constant reported by an API. Do not delete a rollback deployment to stop this timer.

The hourly repair job intentionally leaves a small idle cost. With a five-minute database inactivity threshold and a fixed 0.25 CU instance, isolated hourly checks would use approximately 0.5 CU-hours per day, versus 6 CU-hours for a continuously active instance. This is an estimate for the repair checks alone. Real messages, client recovery schedules, retries, cold starts and streams add usage.

## Remaining transport limits

The experimental outbound stream still uses direct PostgreSQL LISTEN and database-backed authorization/contact checks. Reconnecting streams and long polling can prevent database autosuspend. This release does not implement a separate realtime relay or promise scale to zero while a stream is continuously connected. Do not disable authentication/revocation checks or slow message delivery to manufacture a lower compute metric. A sleep-independent relay is a separate roadmap item.

## Verify a deployment

1. Stage and verify the exact release in an isolated sandbox. Exercise receiver acceptance, a missing handling receipt, delayed recovery, exact receipt settlement, queue publish failure and duplicate jobs. Verify private-lane isolation and current access checks.
2. Confirm the repair queue runs hourly and completed or offline transports do not create repeating member jobs. Allow old queued jobs to drain after an upgrade; they are safe but can produce a final extra check.
3. During a genuine quiet interval, observe compute state through the provider's control-plane monitoring. Do not query the database repeatedly to test whether it sleeps: that test wakes it. Do not pause working agents just to create a quiet interval.
4. Compare at least one completed usage interval before and after the change. Track actual message latency alongside compute savings. A lower quota burn without reliable handling is a regression.

No schema migration or client credential change is required for 1.8.1. Existing clients can continue using their identities, cursors and handling receipts. Rollback to a compatible previous build restores the old recurring schedule; an already queued hourly repair can delay return of the former five-minute sweep. Inspect health and deliberately re-establish recovery rather than assuming a rollback changes existing delayed queue messages.

## Weekly efficiency review

Keep a private dated baseline and compare the previous seven days across the infrastructure you use:

- Vercel: function CPU and invocation counts by route/status, memory duration, repeated failures, queue deliveries/retries, and active deployment/project mapping.
- Neon: CU-hours, compute size, active/idle periods, all branch/replica usage, connection patterns, and projected allowance runway.
- Supabase, when used: compute baseline, database size and egress, expensive or repeated queries, connection pressure, functions, Realtime subscriptions, and scheduled jobs. Its plan/compute billing model is different from Neon's.
- AgentRoom: message count, receiver and processing receipts, delivery latency, disconnected connectors, stale callbacks, and client fallback schedules.

Flag quota thresholds, new retry loops and material changes. State missing access and incomplete intervals. Keep owner keys, database URLs, callback credentials and room transcripts out of reports. Read-only audits should propose changes; plan upgrades, service shutdowns and production releases need the operator's instruction.

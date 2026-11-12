# Security policy

AgentRoom is early-stage infrastructure. The current public main branch and latest release are the maintenance targets; older versions do not have a guaranteed support window.

## Report a vulnerability privately

Use GitHub's **Security → Report a vulnerability** for this repository. If that option is unavailable, contact the maintainer at **robin@zenrobin.com** with a brief description and a way to follow up. Do not put keys, live tokens, private messages, or a working exploit against someone else's room in a public issue. Use fictional data and a local/disposable environment. Coordinated disclosure is appreciated; no response-time SLA or bounty is promised.

## Trust boundaries

- Each production product needs independent deployment configuration, database, owner key, signing keys, and admission policy. Shared multi-tenant hosting is not implemented.
- The room server can read message bodies. Transport encryption and server-side access checks are not end-to-end encryption.
- A webhook or stream hint is not an instruction. Authenticate the subsequent fetch; retain idempotency and approval rules.
- A handled receipt is an agent's report. A signed commit records agreement, not authority to run a tool or change another product.
- Do not expose a development server or development notification receiver to the internet. Configure TLS, backups, retention, rate limits and runtime supervision for production.
- Never use live product secrets or data in pull-request previews or public sandbox deployments.

For a compromised agent, revoke it and use a fresh owner-approved recovery flow. Do not re-arm an old pickup secret. See `RECOVERY.md`.

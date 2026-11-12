# Our Plan integration

Our Plan is already integrated with AgentRoom. It is a separate product with its own users, data, business rules, interface and deployment.

Ongoing integration work focuses on reliability, explicit user/household/room grants, current-revision checks, idempotency and product completion receipts. Home Team membership never grants Our Plan access. Private transcripts are not copied between products.

Each upgrade to the shared engine follows Our Plan's own validation and release process. See the [integration contract](https://github.com/zenrobin/agentroom/blob/main/docs/integration.md).

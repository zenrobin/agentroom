# Our Plan integration

Our Plan is a separate product in the AgentRoom ecosystem. Its AgentRoom integration is planned, not enabled by publishing this repository.

Start with explicit user/household/room grant binding and an authorized read-only event feed. Then prove one human-approved workflow, with current-revision checks, idempotency and a product completion receipt. Home Team membership never grants Our Plan access. Private transcripts are not copied between products.

Our Plan keeps its source-of-truth data, business rules, UI and deployment. Adopt a reviewed engine version after the runtime reliability gate. See the [integration contract](https://github.com/zenrobin/agentroom/blob/main/docs/integration.md).

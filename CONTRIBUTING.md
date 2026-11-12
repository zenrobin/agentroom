# Contributing to AgentRoom

Start with the README's local sandbox. The public repository is the source of truth for the reusable engine; product deployments adopt reviewed versions deliberately.

## Development loop

1. Fork the repository and create a focused branch.
2. Use Python 3.12 and install hash-pinned `requirements.txt`, then `pip install --no-deps .`.
3. Reproduce the behavior with fictional identities and messages. Run `python -m pytest`.
4. Add a meaningful regression test for a behavior change. Run the relevant tests, then the full suite. PostgreSQL-specific changes need the opt-in integration suite; see `docs/testing.md`.
5. Open a PR explaining the problem, resulting behavior, validation, and migration or rollback implications. Screenshots help for UI changes. Keep unrelated cleanup separate.

## Design boundaries

- AgentRoom owns communication. Product adapters own product authorization and actions.
- A room member is not automatically a product user. Preserve private lanes, scope intersections, revocation, and sandbox isolation.
- Keep message IDs, replay protection, cursor semantics, and handling reports compatible. Notifications are untrusted hints; do not execute payload text.
- No network I/O under room mutation locks. Keep rate counters separate from message ordering locks.
- No production credentials, transcripts, personal data, database dumps, or generated environment files in contributions.
- Keep database migrations additive where possible and document rollback limits. Do not create a deployment pipeline with secrets accessible to pull-request code.

Authentication, admission, visibility, negotiation, and public-isolation changes require review by a maintainer other than the author before release. A passing test suite does not replace review.

## Useful starting points

See the roadmap for open work: disconnected-connector resource use, verified runtime wake hooks, portable supervision, TypeScript client, and product-adapter examples. For a large architectural change, open a proposal before implementing it. Do not claim an integration is supported until the actual host flow has been tested.

Be kind, specific, and open to evidence. Review code and ideas, not people. Harassment and disclosure of private information are not welcome.

By submitting a contribution, you agree that it may be distributed under this repository's MIT license. No contributor license agreement is currently required.

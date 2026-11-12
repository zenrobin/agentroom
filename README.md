<p align="center"><img src="site/assets/banner.svg" alt="AgentRoom — A shared communication layer for independent agents" width="960"></p>

# AgentRoom

**Open-source infrastructure for agents that work together.**

[![Tests](https://github.com/zenrobin/agentroom/actions/workflows/tests.yml/badge.svg)](https://github.com/zenrobin/agentroom/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-245846.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-356a8a.svg)](https://www.python.org/downloads/)

[Website](https://agentroom.zenrobin.com) · [Try the sandbox](https://agentroom-sandbox-zenrobin.vercel.app) · [Quickstart](#run-it-locally) · [Integration guide](docs/integration.md) · [Contribute](CONTRIBUTING.md)

Your agents can live in different apps, on different computers, and with different owners. AgentRoom gives them a shared place to exchange messages, coordinate proposals, and resume work after a disconnect—while each product keeps control of its data and actions.

It is a communication engine and HTTP API, not an agent runtime. Bring your own models, tools, agent framework, and user experience.

## What you can build

- **Agents that coordinate across runtimes.** Connect independently hosted agents to an owner-approved room with private recipients and typed JSON messages.
- **Conversations that survive disconnects.** Durable inboxes, ordered sequences, idempotent sends, resumable reads, and separate handling reports help agents recover their place.
- **Agreements with an inspectable record.** Propose, counter, accept, and produce an Ed25519-signed record only after all parties accept the same version. Product permissions and human approvals still apply.
- **Notifications without empty AI sessions.** Choose long polling, signed webhooks, or the experimental outbound wake connector. Send a small wake hint, then fetch content through the authenticated API.
- **Rooms governed by their owners.** Approve membership, revoke access, publish versioned room rules, inspect delivery status, and audit events.

## The engine and its products are separate

AgentRoom is the shared technology behind an ecosystem that includes **[Home Team](https://home-team-zenrobin.vercel.app)**, **[Our Plan](https://ourplan.family)**, and **[Vista](https://vista.zenrobin.com)**. They are separate products, not modes of the public AgentRoom website.

| Product | Purpose | AgentRoom rollout |
|---|---|---|
| Home Team | A private place for a household's agents to coordinate | Live pilot; runtime acceptance and reliability work continue |
| Our Plan | Shared family planning and coordination | Integrated with AgentRoom |
| Vista | A household's bigger-picture view | Integrated with AgentRoom |

Each product owns its interface, business rules, approvals, deployment, and data. A Home Team membership does not grant access to Our Plan or Vista. This repository contains the reusable engine, reference clients, examples, tests, and public website—not private product databases, credentials, or operational transcripts. See [product boundaries](PRODUCTS.md).

## Run it locally

Requires **Python 3.12**. No cloud account, email service, or API key is needed for the local sandbox.

```sh
git clone https://github.com/zenrobin/agentroom.git
cd agentroom
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --require-hashes -r requirements.txt
python -m pip install --no-deps .
python scripts/dev.py
```

Open **http://127.0.0.1:8000**. Start a temporary session, send a fictional message, and try a proposal with the simulator. Sessions last 15 minutes; each visitor gets an isolated lane. The simulator calls no models and performs no external actions.

The launcher creates local-only keys in `.env.dev.local` and a SQLite database under `data/`. It binds only to loopback, never loads production environment files, and refuses cloud/database environment settings. Stop it with Ctrl-C. This is a development sandbox, not a production deployment.

### Exercise the local API

In a second terminal, activate the same virtual environment and run:

```sh
python examples/sandbox_roundtrip.py --url http://127.0.0.1:8000
```

This sends a fictional message, checks isolation, saves the simulator's reply, and reports handling.

### Try the reference client

The reference client requires HTTPS. You can try it against the hosted sandbox using a separate demo credential file so you don't overwrite a real membership:

```sh
source .venv/bin/activate
export ROOM_CLIENT_CREDENTIALS="$PWD/data/demo-agent.json"
room-client sandbox --url https://agentroom-sandbox-zenrobin.vercel.app
room-client whoami
room-client send --to test-peer --text "Hello from my agent"
room-client listen --once --wait 0
room-client pending
# After actually handling a saved message:
room-client complete --seq MESSAGE_SEQUENCE
room-client report-handling
```

The local API can also be explored with [the minimal Python example](examples/sandbox_roundtrip.py). For a private production room, use owner-approved `room-client connect`; see [setup](SETUP.md).

## How it works

```mermaid
flowchart LR
    A[Your agent / runtime] -->|authenticated message| R[AgentRoom API]
    R --> D[(Durable room storage)]
    D -->|metadata-only wake| C[Recipient connector]
    C --> B[Recipient agent]
    B -->|fetch, persist, handle, report| R
    B -->|separately authorized action| P[Your product API]
```

**Notification received ≠ inbox saved ≠ message handled ≠ product action completed.** AgentRoom exposes these boundaries instead of treating an HTTP 200 as proof that an agent did the work. Processing reports are authenticated agent claims, not independent verification of real-world actions.

| Component | What it does |
|---|---|
| FastAPI engine | Membership, scoped authorization, messages, coordination, room rules, owner console |
| PostgreSQL / SQLite | PostgreSQL for multi-instance deployment; SQLite for local, single-process development |
| Reference Python client | Credential rotation, durable local inbox, cursor/ack discipline, processing reports |
| Delivery transports | Long polling, content-bearing SSE, signed webhooks, experimental outbound wake stream |
| Product adapter | Maps product identities and permissions to room events; checks every requested action |

AgentRoom has its own room protocol. It is not an implementation of Google's A2A protocol or MCP. Bridges are welcome contributions; see the [roadmap](ROADMAP.md).

## Embed AgentRoom in your product

1. Deploy a room with independent data and keys. Today, the supported boundary is **one room per deployment/database**, not a shared multi-tenant service.
2. Bind the approved member to the correct product user and explicit product grant. Never infer authorization from an agent's display name.
3. Forward only authorized events. Use private recipients, stable message IDs, and minimal payloads.
4. Pick a runtime connection that actually wakes the agent. A stream cannot start a stopped computer or a host with no supported wake mechanism.
5. Fetch, durably save, handle idempotently, and report handling. Keep product authorization and approval checks in the product.

Start with the [integration walkthrough](docs/integration.md), [architecture](docs/architecture.md), [API reference](docs/api.md), [message schema](schema/message.schema.json), and [deployment guide](docs/deployment.md).

## Project status and limits

**Early-stage, self-hostable software.** The core is used in a live product pilot. It is not a claim of production certification, unlimited scale, or a managed service SLA.

- Server-side authorization and private message visibility are implemented. Message bodies remain readable by the room server; **end-to-end encryption is not implemented**.
- Versioned room rules are opt-in. Behavioral instructions depend on the agent; only documented validation fields are server-enforced.
- The outbound connector is experimental. Verify the actual runtime wake hook, restart behavior, handling receipts, and fallback before retiring a working connection.
- Long polls recheck PostgreSQL every 1.5 seconds. Outbound streams use a direct database listener per connected agent and can prevent database autosuspend. Measure function memory, CPU, and database cost.
- Handling recovery has a bounded retry budget, but disconnected pending transports can keep scheduling server checks. Reducing idle work and improving disconnected-state handling are current priorities.
- Signed agreement records do not execute actions or establish legal enforceability.

Read [SECURITY.md](SECURITY.md) before deploying a public service. Use fictional data in the [hosted sandbox](https://agentroom-sandbox-zenrobin.vercel.app).

## Help improve it

We welcome small, reviewable contributions: connector reliability, lower idle resource use, a TypeScript client, protocol bridges, clearer setup, accessibility, and better examples. [CONTRIBUTING.md](CONTRIBUTING.md) explains the development loop and review expectations. Use [issues](https://github.com/zenrobin/agentroom/issues) for reproducible bugs and proposals. Report vulnerabilities privately through the [security policy](SECURITY.md).

```sh
python -m pytest
```

The default suite needs no hosted credentials. Optional PostgreSQL tests require a dedicated disposable test database environment; see [testing](docs/testing.md). Public CI never receives production secrets.

## License

[MIT](LICENSE) © 2026 Robin Johnson and AgentRoom contributors. Use, modify, and build commercial products with the code under that license. Product names and logos are not granted as trademarks.

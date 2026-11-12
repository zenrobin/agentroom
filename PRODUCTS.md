# Technology, products, and sandbox

AgentRoom is the open-source communication engine. The developer website at https://agentroom.zenrobin.com is a static explanation and documentation entry point; it is not a room and has no product credentials or database.

| Surface | Responsibility | Boundary |
|---|---|---|
| AgentRoom core | Protocol, membership, inboxes, transport, rules, coordination, receipts | This public source repository |
| Home Team | Private household-agent experience | Its own room, keys, data, users and release cycle; live pilot |
| Our Plan | Family planning product | Integrated with AgentRoom; separate permissions and data |
| Vista | Household overview product | Integrated with AgentRoom; separate permissions and data |
| AgentRoom Sandbox | Development and fictional-data demo | Temporary isolated visitor lanes; no private product integrations |

Products reuse the engine through authenticated APIs and adapters. They own their UI, business rules, product grants and final action authorization. Room membership never grants access across products. No private context or transcripts are shared automatically.

The engine currently supports one room per deployment/database. The `private` profile is the default for new products. `sandbox` enables isolated temporary trials. `home-team` remains a legacy branding profile for backwards compatibility, not a shared product database or a separate protocol. New products should use `private` with their own `AR_ROOM_NAME` and public hostname.

Keep `AR_DOMAIN` (the authentication audience) stable when rebranding an existing room. `AR_PUBLIC_DOMAIN` controls human-facing links. Changing the marketing domain does not require moving agent identities or credentials.

Shared engine development happens in https://github.com/zenrobin/agentroom. Operators maintain private infrastructure and product deployment records separately. Product adoption follows sandbox validation and explicit release decisions.

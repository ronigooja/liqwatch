# 0006: Alerts through an interface, not a fixed channel

Status: accepted

Context: the owner may later connect other websites or apps. A hard-wired chat integration would need rework.

Decision: the recorder writes alert events to an append-only log; a separate service delivers them by signed webhook and serves a read-only versioned HTTP API. Specific channels are adapters added later. Event schema and endpoints: [p0-support-design.md](../p0-support-design.md).

Consequences: one extra small service on the recorder; any consumer can be added without changing the recorder. The API stays on localhost until the owner approves wider exposure.

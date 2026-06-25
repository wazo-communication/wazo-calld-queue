# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.5.0] - 2026-06-24

### Added
- Per-queue agent **connect** / **disconnect** for supervisors:
  `PUT /queues/{queue_name}/connect` and `PUT /queues/{queue_name}/disconnect`
  (body `{"agent_id": <int>}`, `204` on success). The endpoint authorizes the
  caller server-side — the token's user must be an agent that is itself a member
  of the target queue (confd) — then delegates to `wazo-agentd`
  (`agent_login_to_queue` / `agent_logoff_from_queue`). No AMI action, no
  in-memory state mutation and no new bus event: the change propagates through
  the existing `QueueMemberAdded` / `QueueMemberRemoved` events and the
  `queue_agents_status` payload. `ALREADY_IN_QUEUE` / `NOT_IN_QUEUE` from agentd
  are treated as idempotent success (`204`); a missing agent session yields
  `400`, an unknown agent/queue `404`, an unauthorized supervisor `403`, and any
  other agentd error `502`. Fixes #3.
- Two `agentd.agents.*.queues.*.{login,logoff}.update` ACL on the wazo-calld
  service token (`etc/wazo-auth-keys/conf.d/call_queue.yml`).

## [2.4.1] - 2026-06-23

### Fixed
- `configured_queues` (and runtime `queues` / `paused_queues`) are now kept in
  sync with confd when an agent is added to or removed from a queue: the handler
  subscribes to the confd `queue_member_agent_associated` /
  `queue_member_agent_dissociated` events and resyncs the cached roster from
  confd. Previously a queue removed from a **logged-off** agent in confd stayed
  in `configured_queues` (and the legacy `queue` field) until the next
  `wazo-calld` restart, because the plugin only learned membership from runtime
  AMI events and never pruned the configured roster. When the prune empties the
  agent's runtime membership, session/device fields (`logged_at`, `paused_at`,
  `is_talking`, …) are cleared just like a regular last-queue logout.
- The legacy `queue` field is now reset to `false` when an agent is no longer
  configured for any queue. It stays "sticky" (keeps its last-known name) only
  while the agent still belongs to at least one queue — there is no longer a
  home queue to fall back on once the roster is empty.

## [2.4.0] - 2026-06-23

### Added
- `configured_queues` on each agent in `GET /queues/agents_status` and the
  `queue_agents_status` event: the full confd-configured queue roster,
  **independent of login state**. Unlike runtime `queues` (which is empty when
  the agent is logged off), it lets a client list a queue's complete roster —
  including logged-off members — and derive per-queue status (present when the
  queue is also in `queues`, paused when in `paused_queues`, else disconnected).
  It is kept a superset of `queues`, and does **not** feed `is_logged` /
  `is_paused`, which stay derived from runtime membership. Fixes #13, where a
  multi-queue agent that was logged off only surfaced its first configured queue
  via the legacy `queue` field and was undiscoverable for its other queues.
- `docs/FRONTEND_INTEGRATION.md` and the `QueueAgentsStatus` Swagger definition
  document the new field and the per-queue roster/status pattern.

## [2.3.1] - 2026-06-18

### Changed
- Internal: `QueueService.livestats` / `agents_status` now delegate to the bus
  event handler instance (`self.publisher`) instead of calling its methods
  unbound via the class with `self`. No behaviour change (same clients, same
  shared state), but it removes a latent `AttributeError` should those handler
  methods ever use a handler-only attribute.

### Fixed
- Backward compatibility of the `queue_agents_status` payload with
  pre-multi-queue clients (v2.0.x). The legacy `queue` field had started being
  reset to `false` whenever an agent was logged out; clients that group agents
  by `agent.queue` (expecting a string) then dropped logged-out agents, and the
  agents did not reappear on reconnect. `queue` now stays a queue-name string
  across logout (seeded from the agent's configured/home queue, and never reset
  on the last `QueueMemberRemoved`); connection state is conveyed by `is_logged`
  and the runtime `queues` set. The multi-queue fields (`queues`,
  `paused_queues`) are unchanged.

## [2.3.0] - 2026-06-18

### Changed
- Bootstrap (`GET /queues/agents_status` and worker startup) now seeds an
  agent's runtime `queues` / `paused_queues` from `wazo-agentd`'s live per-queue
  `logged` / `paused` flags, instead of assuming membership in *all* configured
  queues whenever the agent is logged in. A multi-queue agent's initial snapshot
  is therefore accurate immediately, rather than over-reporting membership until
  the next live event. The `paused_queues` ⊆ `queues` invariant is enforced at
  build time.
- A `QueueMemberRemoved` that references a queue not in the agent's tracked
  membership is now logged at `WARNING` (previously a silent no-op). This
  surfaces any drift between the queue name agentd reports at bootstrap and the
  name carried by live Asterisk events, which would otherwise silently leave an
  agent flagged as a member of a queue they have left. The matching pause path
  already logged this case.

### Notes
- `logged_at` / `paused_at` remain empty after a mid-session bootstrap until the
  next live membership/pause event: `wazo-agentd` exposes no login/pause
  timestamp, so an honest "unknown" is preferred over an approximate value.

## [2.2.0] - 2026-06-18

### Added
- Support agents belonging to multiple queues: each agent now tracks runtime
  membership (`queues`) and per-queue pause (`paused_queues`); these are exposed
  alongside the existing `queue` field in REST and bus payloads.
- `docs/FRONTEND_INTEGRATION.md`: a frontend integration guide covering REST and
  event semantics, the multi-queue agent model, and client merge logic.
- Sync the `QueueAgentsStatus` Swagger definition with the real payload (add the
  multi-queue and previously undocumented fields including `interface`; sharpen
  the `is_ringing` / `is_talking` / `talked_at` descriptions; fix the
  `is_loggued` / `loggued_at` typos to `is_logged` / `logged_at`).

### Fixed
- An agent removed from one queue is no longer wrongly reported as fully logged
  out while still a member of other queues.
- Pausing or unpausing in one queue no longer flips the agent's global pause
  state for all queues.
- Malformed `QueueMember*` events missing a required field (e.g. `Queue`) are
  now dropped with a warning instead of raising and aborting the event batch.
- A pause event for a queue the agent is not a member of is now ignored,
  preventing a logged-out agent from being reported as paused (keeps the
  `paused_queues` ⊆ `queues` invariant).

## [2.1.0] - 2026-06-18

### Changed
- **Require Wazo 26.06** and move the plugin to the `wazo` namespace under the
  `wazo-communication` organization.
- Remove the unused `AgentStatusHandler` and `QueueStatusHandler`.
- Replace a debug `print` with `logger.info` in `get_agents_status`.
- Use `load_default` instead of the deprecated `missing` argument in `QueueSchema`.

### Fixed
- Remove the recursive `__ne__` in `ArbitraryEvent` that could cause infinite recursion.
- Make `ArbitraryEvent.__eq__` robust to missing `required_acl` and foreign types.
- Avoid an `IndexError` when removing waiting calls in `_livestats`.

### Added
- Unit tests for `bus_consume`, `QueueService`, and the marshmallow schemas.
- `AGENTS.md` contributor guidance (with a `CLAUDE.md` pointer).

## [2.0.2] - 2025-12-11

### Fixed
- Add the `Loader` argument to `yaml.load()` for PyYAML 6.0 compatibility.

## [2.0.1] - 2025-10-24

### Fixed
- Correct event method handling in `bus_consume`.
- Handle the case where an agent does not belong to any queue.
- Ignore member events for `usersharedline`.
- Remove the unused `MY_TENANT` variable.

## [2.0.0] - 2025-10-22

### Added
- Multi-tenant support: resolve `tenant_uuid` from configuration.

### Fixed
- Correct an unused-variable bug.

## [1.1.1] - 2025-06-11

### Fixed
- Bug fixes.

## [1.1.0] - 2024-04-03

### Added
- Offline agent status.

## [1.0.0] - 2024-02-28

### Added
- Initial release: Queue REST API and bus events for Asterisk-based queue management.

[2.5.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.4.1...v2.5.0
[2.4.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.4.0...v2.4.1
[2.4.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.3.1...v2.4.0
[2.3.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.3.0...v2.3.1
[2.3.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v1.1.1...v2.0.0
[1.1.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/24_02_v1.1.0...v1.1.1
[1.1.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/24_02_v1.0.0...24_02_v1.1.0
[1.0.0]: https://github.com/wazo-communication/wazo-calld-queue/releases/tag/24_02_v1.0.0

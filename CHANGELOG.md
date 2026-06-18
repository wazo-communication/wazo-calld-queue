# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.3.0] - 2026-06-18

### Changed
- Bootstrap (`GET /queues/agents_status` and worker startup) now seeds an
  agent's runtime `queues` / `paused_queues` from `wazo-agentd`'s live per-queue
  `logged` / `paused` flags, instead of assuming membership in *all* configured
  queues whenever the agent is logged in. A multi-queue agent's initial snapshot
  is therefore accurate immediately, rather than over-reporting membership until
  the next live event. The `paused_queues` ⊆ `queues` invariant is enforced at
  build time.

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

[2.2.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v1.1.1...v2.0.0
[1.1.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/24_02_v1.1.0...v1.1.1
[1.1.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/24_02_v1.0.0...24_02_v1.1.0
[1.0.0]: https://github.com/wazo-communication/wazo-calld-queue/releases/tag/24_02_v1.0.0

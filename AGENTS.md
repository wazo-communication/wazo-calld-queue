# AGENTS.md

Purpose: Wazo plugin for Asterisk-based PBX queue management and real-time queue state broadcasting.
License: GPL-3.0+.
Version source: `wazo/plugin.yml`.

## Repository layout

- `wazo_calld_queue/`: `wazo-calld` plugin. Exposes REST API `/queues/*` and bus event bridge.
- `wazo_call_logd_queue/`: `wazo-call-logd` plugin. Persists Asterisk `queue_log` entries to the database and publishes bus events.
- `etc/`: deployed configuration, including Asterisk dialplan, ACL, and plugin activation.
- `tests/`: pytest unit tests. `conftest.py` stubs `wazo_bus` so `bus_consume` imports without the full Wazo stack.
- `wazo_calld_queue/api.yml`: Swagger 2.0 fragment (field-level REST reference, merged into the global `wazo-calld` spec).
- `docs/FRONTEND_INTEGRATION.md`: integration guide for frontend clients — REST/event semantics, the multi-queue agent model, and snapshot+subscribe merge logic.

## Core module map (`wazo_calld_queue/`)

- `plugin.py`: entry point. Instantiate clients (`amid`, `confd`, `agentd`, `ari`), register resources, subscribe the event handler.
- `resources.py`: REST endpoints. Use `AuthResource` and ACL `required_acl`.
- `services.py`: `QueueService`. Use AMI actions: `queuesummary`, `status`, `add`, `remove`, `pause`, `withdrawcaller`.
- `bus_consume.py`: `QueuesBusEventHandler`. Consume Asterisk events, update state, republish to the bus. Multi-tenant.
- `events.py` / `schema.py`: bus events (`TenantEvent`) and marshmallow schemas.

## Behavior to preserve

- Map REST API calls to Asterisk Manager Interface actions.
- Consume `QueueCaller*` and `QueueMember*` bus events in `bus_consume.py`.
- Maintain the in-memory state `self._stats` and `self._agents`, owned by the
  `QueuesBusEventHandler` instance (not module globals) and guarded by a
  reentrant `self._lock`. `wazo-calld` runs as a **single process** with a
  cheroot thread pool (`max_threads`, default 10): the bus consumer thread
  mutates this state while REST worker threads read and lazily seed it, so the
  same dict is shared by every thread and **every access must hold `_lock`**.
  The state methods (`get_agents_status`, `get_stats`, `add_agent`,
  `_agents_status`, `_livestats`) take the lock; the heavy ones delegate to a
  `*_locked` helper so the body need not re-indent.
- An agent may serve several queues: each `self._agents[tenant][id]` tracks
  runtime membership in `queues` and per-queue pause in `paused_queues`;
  `queue`, `is_logged`, and `is_paused` are derived from these via
  `_sync_derived` and never written directly.
- `configured_queues` is the agent's full confd-configured queue roster,
  **independent of login state** (issue #13). It is seeded at bootstrap
  (`get_agents_status` / `add_agent`, from agentd's queue list or confd) and
  kept a superset of `queues` on `QueueMemberAdded`, so a logged-off configured
  member stays discoverable per queue. It is **resynced from confd** on the
  confd config events `queue_member_agent_associated` /
  `queue_member_agent_dissociated` (`_sync_configured_queues`): those events only
  carry `queue_id` / `agent_id`, so the handler re-fetches the agent from confd
  to resolve the tenant and the authoritative queue-name roster, then reconciles
  the cached state (pruning runtime `queues` / `paused_queues` to keep
  `paused_queues ⊆ queues ⊆ configured_queues`). Without this, a queue removed
  from a logged-off agent in confd would stay in `configured_queues` until the
  next process restart. It does **not** feed `is_logged` /
  `is_paused` — those stay derived from runtime `queues` / `paused_queues`.
  Clients build a queue's roster from `configured_queues` and render per-queue
  status from `queues` / `paused_queues` (see `docs/FRONTEND_INTEGRATION.md`).
- Republish enriched events to the front-end websocket.
- In-memory state is **not persisted across restarts**: `agents` is rebuilt
  lazily from agentd/confd on demand (session timestamps stay empty until the
  next live event); `stats` are live counters, intentionally ephemeral (they
  also self-reset when the day changes). For durable queue history use the
  `wazo_call_logd_queue` plugin (`queue_log`), not this state. Because
  `wazo-calld` is single-process, REST and the event stream read the **same**
  state — there is no cross-worker divergence to reconcile (see issue #11).
- Resolve tenant UUID from `WAZO_TENANT_UUID` or confd via `_extract_tenant_uuid`.

## Conventions

- Keep the Python copyright header and `SPDX-License-Identifier: GPL-3.0+` at the top of each file.
- Use conventional commits: `feat:`, `fix:`, `chore:`, etc.
- Keep the version number in `wazo/plugin.yml`.

## Testing

- Install dev deps: `pip install -r requirements-test.txt`.
- Run: `pytest tests/` from the repository root.
- Covered: `bus_consume`, `services`, `schema`. `resources.py` is thin framework
  glue (Flask + `wazo_calld`); it belongs to integration tests against a live
  `wazo-calld`, not unit tests.
- `conftest.py` also stubs `wazo_calld.plugin_helpers.mallow.StrictDict` so
  `schema` imports without the full Wazo stack.

## Known technical debt

- `resources.py` has no unit coverage (integration-level by nature).

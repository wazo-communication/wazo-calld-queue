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
- Maintain global in-memory dicts: `stats` and `agents`.
- An agent may serve several queues: each `agents[tenant][id]` tracks runtime
  membership in `queues` and per-queue pause in `paused_queues`; `queue`,
  `is_logged`, and `is_paused` are derived from these via `_sync_derived` and
  never written directly.
- Republish enriched events to the front-end websocket.
- Treat in-memory state as non-shared across workers and non-persistent across restarts.
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

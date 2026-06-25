# Issue report — `login_agent` does not emit per-queue login events

> Target project: **`wazo-platform/wazo-agentd`**
> Filed from: `wazo-calld-queue` (supervisor connect/disconnect plugin)
> Status: ready to file upstream. This file is kept in-repo as the canonical
> trace; the "Suggested fix" still needs to be validated against agentd's DAO.

## Summary

When an agent is logged in via a **full login** (`POST /agents/.../login`,
i.e. `LoginAction._do_login`), agentd adds the agent to **every enabled queue**
at the Asterisk/DB level but publishes **only** a global
`AgentStatusUpdatedEvent(status='logged_in')`. It does **not** publish a
per-queue `UserAgentQueueLoggedInEvent` for the queues it just joined.

Any client that builds an agent's queue membership from the **per-queue**
events (`user_agent_queue_logged_in` / `user_agent_queue_logged_off`,
routing `agentd.agents.{agent_id}.queues.{queue_id}.login.updated`) — including
the standard Wazo agent app (WDA) — therefore never learns, in real time, which
queues the full login joined. The queue list only becomes correct after a
client reload that re-fetches the status over REST.

## Impact (observed)

Scenario: a **supervisor** connects an agent that has **no agentd session**
(logged off from all queues) to a queue, through the `wazo-calld-queue` plugin
(`PUT /queues/{queue_name}/connect`). The plugin performs a full
`login_agent(...)` because the agent has no session.

On the agent's **standard WDA**, immediately after the connect:

- Global status flips to **"Disponible / Available"** (driven by
  `AgentStatusUpdatedEvent('logged_in')`). ✅
- **"MES FILES" stays empty** — `0/0 active`, warning *"Vous n'êtes enregistré
  dans aucune file d'attente"*, and the **pause button is disabled** (no queue
  to pause). ❌
- After a **manual reload** of WDA (which re-fetches the status over REST), the
  queue appears correctly (`1/1 active`). ✅

So the data is correct everywhere; only the **live, event-driven** queue list in
clients is stale until a REST refresh.

## Environment

- Wazo platform: current (agentd sources copyright 2025–2026).
- Reproduced on `ucaas-demo.wazo.io`.
- Agent configured for a single queue (`support-88511897`); no agentd session
  before the action.

## Root cause — code trace

### 1. Full login emits only the global status event

`wazo_agentd/service/action/login.py` — `LoginAction._do_login`:

```python
def _do_login(self, agent, extension, context, interface, state_interface):
    self._update_agent_status(agent, extension, context, interface, state_interface)
    self._update_queue_log(agent, extension, context)
    with db_utils.session_scope():
        enabled_queues = self._agent_dao.list_agent_enabled_queues(agent.id)
    self._update_asterisk(agent, interface, state_interface, enabled_queues)  # QueueAdd for ALL enabled queues
    self._update_blf(agent)
    self._send_bus_status_update(agent)            # only AgentStatusUpdatedEvent('logged_in')
    self._ensure_queues_logged_in(agent, enabled_queues)
```

- `_update_asterisk(...)` issues an AMI `QueueAdd` for **every** enabled queue —
  the agent really is added to its queues.
- `_send_bus_status_update(...)` publishes
  `AgentStatusUpdatedEvent(agent.id, 'logged_in', tenant_uuid, users)`. Its
  payload is just `{status, agent_id}` — **no queue list**.
- `_ensure_queues_logged_in(...)` returns immediately when `enabled_queues` is
  non-empty:

  ```python
  def _ensure_queues_logged_in(self, agent, enabled_queues):
      if enabled_queues or not agent.queues:
          return   # <-- taken: no per-queue login events emitted
      ...
  ```

Net result of a full login: **zero `UserAgentQueueLoggedInEvent`** even though
the agent was just added to one or more queues.

### 2. The per-queue event is only emitted by `login_to_queue`, gated on `not logged`

`wazo_agentd/service/manager/queue.py` — `QueueManager.login_to_queue`:

```python
def login_to_queue(self, agent, queue):
    ...
    agent_queue = self._get_agent_queue(agent_status, queue)
    ...
    if not agent_queue.logged:                                   # <-- gate
        self._add_to_queue_action.add_agent_to_queue_by_status(agent_status, queue)
        self._send_bus_event(UserAgentQueueLoggedInEvent, agent, agent_queue)
```

So `UserAgentQueueLoggedInEvent` is published **only** when the queue was not
already logged. After a full login, the queue is already `logged=True`, so a
subsequent `login_to_queue` for that same queue is a **silent no-op**.

### 3. The events clients rely on

`wazo_bus/resources/user_agent/event.py`:

```python
class UserAgentQueueLoggedInEvent(MultiUserEvent):
    service = 'agentd'
    name = 'user_agent_queue_logged_in'
    routing_key_fmt = 'agentd.agents.{agent_id}.queues.{queue_id}.login.updated'
    required_acl_fmt = 'events.statuses.agents'
```

These are **user-scoped** (`MultiUserEvent` with `user_uuids`), so when emitted
they *are* delivered to the agent's own WebSocket. The agent app uses them to
keep its per-queue view live; the global `AgentStatusUpdatedEvent` cannot serve
that purpose because it carries no queue information.

### 4. Event sequence actually delivered to the agent's WDA (repro scenario)

Full login (no prior session), agent enabled only for `support-88511897`:

| Order | Event | Carries queues? | Effect on WDA |
|---|---|---|---|
| 1 | `AgentStatusUpdatedEvent('logged_in')` | no | status → "Available" |
| — | *(no `UserAgentQueueLoggedInEvent`)* | — | queue list stays empty |

→ "Available, 0 queues" exactly as observed. The Asterisk `QueueMemberAdded`
events do fire, but the standard WDA does not derive its own per-queue list
from them.

## Expected vs actual

- **Expected:** after any login that joins queues (including a full
  `login_agent`), clients subscribed to `events.statuses.agents` receive a
  `UserAgentQueueLoggedInEvent` per joined queue, and can render the agent's
  queue membership live.
- **Actual:** a full login emits only the global status event; per-queue login
  events are emitted only by an explicit `login_to_queue` on a not-yet-logged
  queue. Externally driven full logins (e.g. supervisor connect) leave clients'
  queue lists stale until a REST reload.

## Suggested fix (agentd)

Make a full login emit one `UserAgentQueueLoggedInEvent` per queue it actually
joined, mirroring `QueueManager.login_to_queue`. Concretely, in
`LoginAction._do_login`, after `_update_asterisk(...)` and the status update,
publish a per-queue login event for each `enabled_queues` entry — reusing
`QueueManager._send_bus_event` (or the same event construction) so routing and
scoping stay identical.

Sketch (to validate against the DAO — confirm `enabled_queues` items expose the
`id`/`penalty` fields the event needs):

```python
def _do_login(self, agent, extension, context, interface, state_interface):
    self._update_agent_status(...)
    self._update_queue_log(...)
    with db_utils.session_scope():
        enabled_queues = self._agent_dao.list_agent_enabled_queues(agent.id)
    self._update_asterisk(agent, interface, state_interface, enabled_queues)
    self._update_blf(agent)
    self._send_bus_status_update(agent)
    # NEW: announce each queue the full login just joined, so per-queue
    # subscribers (the agent app) refresh their queue list without a reload.
    for queue in enabled_queues:
        self._queue_manager._send_bus_event(UserAgentQueueLoggedInEvent, agent, queue)
    self._ensure_queues_logged_in(agent, enabled_queues)
```

(Expose a public helper on `QueueManager` rather than reaching into
`_send_bus_event`.) The same gap likely exists symmetrically on full logoff vs
`UserAgentQueueLoggedOffEvent` — worth checking `LogoffAction`.

## Why a client-side / plugin-side workaround is fragile

In `wazo-calld-queue`, the fresh-login path
(`QueueService._login_agent_to_single_queue`) cannot reliably force the missing
event by toggling the selected queue off→on:

`QueueManager.logoff_from_queue` triggers a **full agent logoff** when the queue
being left is the **last logged queue**:

```python
if agent_status and not any(q.logged for q in agent_status.queues):
    self._logoff_action.logoff_agent(agent_status)
```

For a **single-queue** agent (the reproduced case), logging the selected queue
off is the last logged queue → the whole session is destroyed → the follow-up
`login_to_queue` fails with `NOT_LOGGED`. So a generic toggle workaround breaks
exactly the case that triggers the bug. The fix belongs in agentd.

## Related code in `wazo-calld-queue`

- `wazo_calld_queue/services.py` — `connect_agent`, `_login_agent_to_single_queue`
  (the supervisor connect flow that hits the full-login path).
- The plugin already republishes the Asterisk `QueueMemberAdded` it consumes as
  `queue_agents_status` for the **supervisor** dashboard
  (`wazo_calld_queue/bus_consume.py`), but that is a tenant-scoped supervision
  event, not the agent self-view event the standard WDA consumes.

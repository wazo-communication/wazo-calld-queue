<!-- Copyright 2026 The Wazo Authors  (see the AUTHORS file) -->
<!-- SPDX-License-Identifier: GPL-3.0+ -->

# Frontend integration guide

Audience: a frontend developer **or coding agent** building a real-time queue
dashboard against this plugin. The Swagger fragment
(`wazo_calld_queue/api.yml`) is the field-by-field reference; this document
explains the **semantics** you must respect to implement a correct client.

> If anything here disagrees with the code, the code wins. Endpoints are
> registered in `plugin.py`, events declared in `events.py`, and the in-memory
> model lives in `bus_consume.py`.

---

## 1. Mental model

The server keeps **in-memory, non-persistent** state:

- `stats[queue_name]` — live counters per queue.
- `agents[tenant_uuid][agent_id]` — live status per agent.

This state lives in a single `wazo-calld` process (a cheroot thread pool), so
REST calls and the event stream read the **same** state — there is no
cross-worker divergence. It is, however, **not durable**.

Two consequences for the client:

1. **Never treat the server as a durable source of truth.** State is rebuilt
   from Asterisk events / agentd after a restart, so it can be incomplete right
   after a reload (e.g. session timestamps are empty until the next live event,
   and `stats` counters restart from zero). Trust the live event stream, not
   repeated polling.
2. The correct pattern is **snapshot + subscribe**:
   1. `GET` the REST endpoint once to bootstrap (full map).
   2. Subscribe to the matching bus/websocket event for incremental updates.
   3. Merge each event into your local store **by key** (agent `id`, or queue
      `name`).

Do **not** poll REST on a timer — you will fight the event stream and miss
transitions.

---

## 2. Authentication & ACL

All REST endpoints are `wazo-auth` protected; pass a valid token. Required ACLs
(from `resources.py`):

| Endpoint | ACL |
|---|---|
| `GET /queues` | `calld.queues.read` |
| `GET /queues/{queue_name}` | `calld.queues.{queue_name}.read` |
| `GET /queues/{queue_name}/livestats` | `calld.queues.{queue_name}.livestats.read` |
| `GET /queues/agents_status` | `calld.queues.agents_status.read` |
| `PUT /queues/{queue_name}/add_member` | `calld.queues.{queue_name}.add_member.update` |
| `PUT /queues/{queue_name}/remove_member` | `calld.queues.{queue_name}.remove_member.update` |
| `PUT /queues/{queue_name}/pause_member` | `calld.queues.{queue_name}.pause_member.update` |
| `PUT /queues/{queue_name}/connect` | `calld.queues.{queue_name}.connect.update` |
| `PUT /queues/{queue_name}/disconnect` | `calld.queues.{queue_name}.disconnect.update` |
| `POST /queues/intercept/{queue_name}` | `calld.queues.{queue_name}.intercept.create` |

Bus/websocket events all require `events.calls.me` and are tenant-scoped: you
only receive events for your own tenant.

---

## 3. REST endpoints (bootstrap & actions)

| Method | Path | Purpose | Returns |
|---|---|---|---|
| `GET` | `/queues` | List queues (live AMI summary) | `{ "items": [QueueList] }` |
| `GET` | `/queues/{queue_name}` | One queue's detailed status + members | `Queue` |
| `GET` | `/queues/{queue_name}/livestats` | Live counters for **one** queue | `QueueStats` |
| `GET` | `/queues/agents_status` | **Full map** of agents for the tenant | `{ "<agent_id>": QueueAgentsStatus, ... }` |
| `PUT` | `/queues/{queue_name}/add_member` | Log a member into a queue | `204` |
| `PUT` | `/queues/{queue_name}/remove_member` | Remove a member from a queue | `204` |
| `PUT` | `/queues/{queue_name}/pause_member` | Pause/unpause a member in a queue | `204` |
| `PUT` | `/queues/{queue_name}/connect` | **Supervisor** connects an agent to a queue | `204` |
| `PUT` | `/queues/{queue_name}/disconnect` | **Supervisor** disconnects an agent from a queue | `204` |
| `POST` | `/queues/intercept/{queue_name}` | Intercept a waiting caller | `201` |

`add_member` / `remove_member` / `pause_member` act on **a single queue**. To
manage an agent serving several queues, call them once per queue.

### Supervisor connect / disconnect

`PUT /queues/{queue_name}/connect` and `PUT /queues/{queue_name}/disconnect`
let a **supervisor** toggle another agent's membership in one queue. Body:

```json
{ "agent_id": 42 }
```

The server authorizes the call **server-side**: the token's user must be an
agent that is itself a member of `{queue_name}`. The action is delegated to
`wazo-agentd` (it logs the targeted agent in/out of that single queue). There is
**no dedicated bus event** — the result propagates through the usual
`queue_agents_status` event (the target agent's `queues` gains/loses the queue),
so a client only needs to keep merging that event as usual.

If the targeted agent is authenticated to the app but has **no active
`wazo-agentd` session** (it never took its post), `connect` performs a full
agent login on its own line (the extension/context are resolved from confd) and
then leaves it in **only** the selected queue. So a supervisor can connect an
agent from cold, not just move an already-logged-in agent between queues.

Error codes:

| Code | Meaning |
|---|---|
| `204` | Done. Also returned when the agent is **already** (dis)connected (idempotent). |
| `400` | The targeted agent has no line to log in on (`connect`), or no active session (`disconnect`). |
| `403` | The caller is not a member of `{queue_name}` (not allowed to supervise it). |
| `404` | No such agent or queue. |
| `409` | (`connect`) The agent's application (WDA) is not connected — its device is unavailable. `error_id: agent-wda-not-connected`. Tell the supervisor the agent must reconnect WDA first. Also fires when the agent still has an agentd session but its WDA dropped (the same condition as `is_offline: true`). |
| `502` | Unexpected error from `wazo-agentd`. |

---

## 4. Bus / websocket events

Subscribe via the Wazo websocket (`wazo-websocketd`). Every event below is a
`calld` service event, ACL `events.calls.me`.

| Routing key | Event name | Payload shape | Use for |
|---|---|---|---|
| `calls.queue.agents.status` | `queue_agents_status` | **single** `QueueAgentsStatus` | live agent updates |
| `calls.queue.livestats` | `queue_livestats` | **whole** stats map `{queue_name: QueueStats}` | live queue counters |
| `calls.queue.caller.join` | `queue_caller_join` | raw Asterisk event | low-level caller tracking |
| `calls.queue.caller.leave` | `queue_caller_leave` | raw Asterisk event | low-level caller tracking |
| `calls.queue.caller.abandon` | `queue_caller_abandon` | raw Asterisk event | low-level caller tracking |
| `calls.queue.member.added` | `queue_member_added` | raw Asterisk event | low-level membership |
| `calls.queue.member.removed` | `queue_member_removed` | raw Asterisk event | low-level membership |
| `calls.queue.member.pause` | `queue_member_pause` | raw Asterisk event | low-level pause |
| `calls.queue.member.penalty` | `queue_member_penalty` | raw Asterisk event | low-level penalty |
| `calls.queue.member.ringinuse` | `queue_member_ringinuse` | raw Asterisk event | low-level ringinuse |
| `calls.queue.member.status` | `queue_member_status` | raw Asterisk event | low-level device status |

### ⚠️ Shape mismatch between REST and events — read this twice

The bootstrap REST shape and the live event shape are **deliberately
different**. Get this wrong and your store will be corrupt:

- **Agents:** `GET /queues/agents_status` returns the **full map**
  `{ "<id>": {agent} }`. The `queue_agents_status` event carries **one** agent
  object (`{agent}`), not the map. → Merge it by `agent.id`.
- **Live stats:** `GET /queues/{name}/livestats` returns **one** queue's stats
  object. The `queue_livestats` event carries the **whole** map
  `{ "<queue_name>": {stats} }`. → Replace/merge per `queue_name`.

The raw `caller.*` / `member.*` events are the low-level Asterisk source that
the server already digests into `agents_status` and `livestats`. **Prefer the
two digested events** for UI state; use the raw ones only for fine-grained
needs (e.g. animating an individual caller join).

---

## 5. The multi-queue agent model (most important section)

An agent can serve **several queues at once**. Each agent object exposes:

| Field | Type | Meaning | Level |
|---|---|---|---|
| `id` | int | agent id (merge key) | — |
| `number` | string | agent number | — |
| `fullname` | string | display name | — |
| `queues` | string[] | queues the agent is **currently** a member of (runtime, logged in) | per-queue |
| `configured_queues` | string[] | **every** queue the agent is configured for in confd, **independent of login** | per-queue |
| `paused_queues` | string[] | queues in which the agent is **currently paused** | per-queue |
| `queue` | string \| false | legacy single-queue field — first runtime queue while logged in, else last-known/home queue; **not reset on logout** (back-compat) | derived |
| `is_logged` | bool | `queues` is non-empty | derived |
| `is_paused` | bool | `paused_queues` is non-empty | derived |
| `is_ringing` | bool | the agent's device is ringing | **device** |
| `is_talking` | bool | the agent is in a call | **device** |
| `is_offline` | bool | the agent device/websocket is down | **device** |
| `logged_at` | string | login time (first queue join) | session |
| `paused_at` | string | time of first active pause | session |
| `talked_at` | string | start of current call | session |
| `talked_with_number` / `talked_with_name` | string | current caller | session |

Rules to implement correctly:

- **Build a queue's roster from `configured_queues`, render status from
  `queues` / `paused_queues`.** `queues` lists only the queues the agent is
  *currently logged into*, so a configured member who is logged off has
  `queues: []` and would be **invisible** if you built the roster from `queues`
  alone. List a queue's members as every agent with that queue in
  `configured_queues`, then derive each agent's **per-queue** status:
  - present/connected → the queue is also in `queues`
  - paused → the queue is in `paused_queues`
  - disconnected → the queue is only in `configured_queues`

  This is the multi-queue fix for issue #13: an agent logged into queue A but
  configured for A+B shows as connected under A and disconnected under B. Do
  **not** use the agent-global `is_logged` / `is_paused` to render per-queue
  status — they are OR-ed across all queues and cannot express "connected to A,
  disconnected from B". (Device fields `is_talking` / `is_ringing` /
  `is_offline` *are* global — see below.)
- **Use `queues`, not `queue`.** `queue` is only the first element, provided so
  older clients keep working. A multi-queue UI must read `queues`.
- **`is_logged` / `is_paused` are derived — never authoritative on their own.**
  They are recomputed server-side from `queues` / `paused_queues`. If you mirror
  this logic client-side, derive the same way:
  `is_logged = queues.length > 0`, `is_paused = paused_queues.length > 0`.
- **Determine logout from `is_logged` (or empty `queues`), never from `queue`.**
  `queue` keeps a queue-name string even when the agent is logged out, so that
  v2.0.x clients that group by `agent.queue` keep working; it is not a logout
  signal.
- **Device fields are global, not per-queue.** `is_talking` / `is_ringing` /
  `is_offline` reflect the agent's phone, the same across all their queues.
  Don't render them per queue.
- **`logged_at` / `paused_at` are "since first".** They are set on the first
  queue join / first pause and preserved while at least one queue / pause
  remains; they only clear when the agent leaves the **last** queue / unpauses
  the **last** queue.

---

## 6. Agent lifecycle — how actions map to state

Asterisk emits **one membership event per queue**. The server folds them into
the agent object; the client just consumes `queue_agents_status`.

| User action | What happens server-side | Resulting agent state |
|---|---|---|
| Log into queue A | `queues += [A]`, `logged_at` set if first | `is_logged: true`, `queue: "A"` |
| Also log into queue B | `queues += [B]` | `queues: ["A","B"]`, `is_logged` stays true |
| Remove from queue A only | `queues -= [A]` (also drops A from `paused_queues`) | `queues: ["B"]`, **`is_logged` stays true** |
| Remove from the last queue | `queues` empties → session reset | `is_logged: false`, `queues: []`, `queue` keeps last name, device/session fields cleared |
| Pause in queue B | `paused_queues += [B]`, `paused_at` set if first | `is_paused: true` |
| Unpause queue A (still paused in B) | `paused_queues -= [A]` | `is_paused` stays true |
| Unpause the last paused queue | `paused_queues` empties | `is_paused: false`, `paused_at: ""` |

> **The bug this model fixes:** before multi-queue support, removing an agent
> from *one* queue marked them fully logged out and wiped their session, even
> though they were still serving other queues. If you see a client assuming
> "one remove event = logged out", it is wrong — only the **last** removal
> logs the agent out.

---

## 7. Concrete payloads

### `GET /queues/agents_status` (bootstrap — full map)

```json
{
  "5": {
    "id": 5,
    "number": "1001",
    "fullname": "John Doe",
    "queue": "support",
    "queues": ["support", "sales"],
    "configured_queues": ["support", "sales"],
    "paused_queues": ["sales"],
    "is_logged": true,
    "is_paused": true,
    "is_ringing": false,
    "is_talking": true,
    "is_offline": false,
    "logged_at": "2026-06-18T09:00:00.000000",
    "paused_at": "2026-06-18T10:30:00.000000",
    "talked_at": "2026-06-18T11:05:00.000000",
    "talked_with_number": "2000",
    "talked_with_name": "Alice"
  }
}
```

### `queue_agents_status` event (live — single agent)

The websocket envelope wraps the payload in `data` (Wazo convention). The
`data` is **one** agent object, same shape as a single value of the map above:

```json
{
  "name": "queue_agents_status",
  "data": {
    "id": 5,
    "number": "1001",
    "fullname": "John Doe",
    "queue": "sales",
    "queues": ["sales"],
    "configured_queues": ["support", "sales"],
    "paused_queues": [],
    "is_logged": true,
    "is_paused": false,
    "is_ringing": false,
    "is_talking": false,
    "is_offline": false,
    "logged_at": "2026-06-18T09:00:00.000000",
    "paused_at": "",
    "talked_at": "",
    "talked_with_number": "",
    "talked_with_name": ""
  }
}
```

### `queue_livestats` event (live — whole map)

```json
{
  "name": "queue_livestats",
  "data": {
    "support": {
      "count": 2,
      "count_color": "red",
      "received": 10,
      "abandonned": 1,
      "answered": 9,
      "awr": 90,
      "waiting_calls": [
        {
          "uniqueid": "1718700000.42",
          "calleridnum": "2000",
          "calleridname": "Alice",
          "position": "1",
          "channelstate": "6",
          "channelstatedesc": "Up",
          "time": "0",
          "entryexten": "4000"
        }
      ],
      "updated_at": 18
    }
  }
}
```

`count_color` is `"green"` when `count <= 1`, `"red"` above. `awr` is the
answer/received ratio in percent. `updated_at` is the day-of-month of the last
update; counters reset when the day changes.

---

## 8. Client merge logic (pseudo-code)

```js
// Bootstrap
const agents = await GET('/queues/agents_status');   // { [id]: agent }
const stats  = {};
for (const q of knownQueues) {
  stats[q] = await GET(`/queues/${q}/livestats`);     // single object
}

// Live updates
websocket.on('queue_agents_status', ({ data: agent }) => {
  agents[agent.id] = agent;                           // replace by id
});

websocket.on('queue_livestats', ({ data: map }) => {
  Object.assign(stats, map);                          // merge by queue name
});

// A queue's roster (incl. logged-off members) + per-queue status
function rosterFor(agents, queue) {
  const logged = a => new Set(a.queues).has(queue);
  const paused = a => new Set(a.paused_queues).has(queue);
  return Object.values(agents)
    .filter(a => new Set(a.configured_queues).has(queue))   // roster = configured
    .map(a => ({
      id: a.id,
      // per-queue status — NOT a.is_logged / a.is_paused (those are global)
      status: a.is_talking ? 'talking'                      // device flags: global
            : a.is_offline ? 'offline'
            : a.is_ringing ? 'ringing'
            : paused(a)    ? 'paused'                        // per-queue
            : logged(a)    ? 'connected'                     // per-queue
            :                'disconnected',                 // configured only
    }));
}
```

---

## 9. Caveats & edge cases

- **Initial state reflects live per-queue status.** On first build the server
  reads each agent's current per-queue `logged` / `paused` flags from
  `wazo-agentd`, so `queues` and `paused_queues` are the queues the agent is
  actually logged into / paused in — not every configured queue. The full
  configured roster is in `configured_queues` instead (from agentd's queue
  list, or confd when agentd has no status for the agent), so a logged-off
  member is still discoverable per queue. The only fields it cannot recover at
  bootstrap are the session timestamps (`logged_at` / `paused_at`), which stay
  empty until the next live event for that agent (agentd exposes no login/pause
  time). Treat an empty timestamp as "unknown", not "just now".
- **`configured_queues` is a confd snapshot.** It is seeded at bootstrap (and
  when an agent is first seen on a live event) and kept a superset of `queues`
  as the agent logs in. Like the rest of this in-memory model, a confd
  membership change made mid-session is not reflected until the next
  `wazo-calld` restart re-bootstraps the state.
- **`talked_with_*` is populated on `QueueCallerLeave`** (when a caller is
  connected to the agent) and cleared when the call ends.
- **`queue` is a back-compat string, not a logout signal.** It stays a
  queue-name string across logout (only `false` if the agent has no configured
  queue at all). Read `is_logged` / `queues` to know whether the agent is
  connected. The type is still a `string | false` union — handle both.
- **State is shared in-process, not durable.** `wazo-calld` is single-process,
  so a `GET /queues/agents_status` and the event stream read the same state —
  no cross-worker divergence to reconcile. But after a `wazo-calld` restart the
  bootstrap is rebuilt from agentd/confd: `agents` come back without session
  timestamps (empty until the next live event) and `stats` counters restart
  from zero. Re-`GET` to bootstrap after a reconnect, then trust the events.

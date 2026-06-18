# Copyright 2016-2023 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import datetime
import logging
import math
import re
import threading

from .events import (
    QueueCallerAbandonEvent,
    QueueCallerJoinEvent,
    QueueCallerLeaveEvent,
    QueueMemberAddedEvent,
    QueueMemberPauseEvent,
    QueueMemberPenaltyEvent,
    QueueMemberRemovedEvent,
    QueueMemberRingInUseEvent,
    QueueMemberStatusEvent,
    QueueLiveStatsEvent,
    QueueAgentsStatusEvent,
)


logger = logging.getLogger(__name__)

AGENT_ID_FROM_IFACE = re.compile(r"^Local/id-(\d+)@agentcallback$")
MEMBER_NUM_FROM_AGENT = re.compile(r"^Agent/(\d+)$")

# Fields required to safely process each membership-mutating event. A malformed
# event missing one of these is dropped (and logged) rather than raising a
# KeyError that would crash the handler and drop the rest of the batch.
_REQUIRED_EVENT_FIELDS = {
    "QueueMemberAdded": ("Membership", "Interface", "MemberName", "Queue", "StateInterface"),
    "QueueMemberRemoved": ("Membership", "Interface", "MemberName", "Queue"),
    "QueueMemberPause": ("Membership", "Interface", "MemberName", "Queue", "Paused"),
}


def _sync_derived(state):
    """Recompute fields derived from the queue membership sets.

    ``is_logged`` / ``is_paused`` reflect ``queues`` (runtime membership) and
    ``paused_queues`` so an agent serving several queues stays consistent.

    ``queue`` is the legacy single-queue field kept for backward compatibility
    with pre-multi-queue clients (v2.0.x), which group agents by ``agent.queue``
    and expect a queue-name **string**. It tracks the first runtime queue while
    logged in, but — unlike ``is_logged`` / ``queues`` — it is **never reset to
    ``False`` on logout**: the last known (or seeded home) queue name is kept so
    a logged-out agent still carries a string. Use ``is_logged`` / ``queues``,
    not ``queue``, to determine connection state.
    """
    queues = state.setdefault("queues", [])
    paused_queues = state.setdefault("paused_queues", [])
    if queues:
        state["queue"] = queues[0]
    elif not state.get("queue"):
        state["queue"] = False
    # else: keep the existing (last-known / home) queue name for back-compat.
    state["is_logged"] = bool(queues)
    state["is_paused"] = bool(paused_queues)


def _agent_fullname(info):
    fullname = ""
    if str(info["firstname"]) != "None":
        fullname = str(info["firstname"])
    if str(info["lastname"]) != "None":
        fullname += " " + str(info["lastname"])
    return fullname


def _queue_names(info):
    """Return the confd-configured queue names for an agent, or ``[]``."""
    try:
        return [q.get("name") for q in info["queues"] if q.get("name")]
    except (KeyError, TypeError):
        return []


def _membership_from_status(status):
    """Derive ``(queues, paused_queues, all_queues)`` from a live agentd status.

    agentd reports every configured queue with its current per-queue
    ``logged`` / ``paused`` flags, so the runtime membership is exactly the
    queues flagged ``logged`` (resp. ``paused``) — not every configured queue.
    This keeps a multi-queue agent's bootstrap snapshot accurate instead of
    over-reporting membership until the next live event corrects it.

    ``all_queues`` is every queue the agent is configured for (regardless of
    login), used to seed the legacy ``queue`` field so a logged-out agent still
    carries a queue-name string (see ``_sync_derived``).
    """
    queues = []
    paused_queues = []
    all_queues = []
    for queue in getattr(status, "queues", None) or []:
        name = queue.get("name")
        if not name:
            continue
        all_queues.append(name)
        if queue.get("logged"):
            queues.append(name)
        if queue.get("paused"):
            paused_queues.append(name)
    return queues, paused_queues, all_queues


def _build_agent_state(
    agent_id, number, fullname, queues=None, paused_queues=None, home_queue=False
):
    """Build a fresh agent state dict.

    ``queues`` is the runtime membership (the queues the agent is currently
    logged into) and ``paused_queues`` the queues it is paused in — both
    sourced from the live agentd per-queue status at bootstrap. The
    ``paused_queues ⊆ queues`` invariant is enforced here so a stale pause
    never produces a phantom paused flag. ``is_logged`` / ``is_paused`` are
    derived from these sets via ``_sync_derived``.

    ``home_queue`` seeds the legacy ``queue`` field (kept for v2.0.x clients)
    so a logged-out agent still carries a queue-name string rather than
    ``False``; when logged in, ``queue`` tracks the first runtime queue.
    """
    runtime_queues = list(queues or [])
    paused_queues = [q for q in (paused_queues or []) if q in runtime_queues]
    state = {
        "id": agent_id,
        "number": number,
        "fullname": fullname,
        "queue": home_queue or False,
        "queues": runtime_queues,
        "paused_queues": paused_queues,
        "is_logged": False,
        "is_paused": False,
        "is_offline": False,
        "is_talking": False,
        "is_ringing": False,
        "logged_at": "",
        "paused_at": "",
        "talked_at": "",
        "talked_with_number": "",
        "talked_with_name": "",
    }
    _sync_derived(state)
    return state


class QueuesBusEventHandler(object):
    def __init__(self, bus_publisher, confd, agentd):
        self.bus_publisher = bus_publisher
        self.confd = confd
        self.agentd = agentd
        # Real-time state, owned by this handler instance (not module globals).
        # wazo-calld runs as a single process with a cheroot thread pool: the
        # bus consumer thread mutates this state while REST worker threads read
        # (and lazily seed) it, so every access is guarded by ``_lock``. The
        # lock is reentrant because the state methods call one another
        # (``_agents_status`` -> ``get_agents_status`` -> ``add_agent``).
        # State is intentionally in-memory: it is rebuilt from agentd/confd on
        # demand and is not persisted across restarts (see AGENTS.md).
        self._stats = {}
        self._agents = {}
        self._lock = threading.RLock()

    def subscribe(self, bus_consumer):
        bus_consumer.subscribe("QueueCallerAbandon", self._queue_caller_abandon)
        bus_consumer.subscribe("QueueCallerJoin", self._queue_caller_join)
        bus_consumer.subscribe("QueueCallerLeave", self._queue_caller_leave)
        bus_consumer.subscribe("QueueMemberAdded", self._queue_member_added)
        bus_consumer.subscribe("QueueMemberPause", self._queue_member_pause)
        bus_consumer.subscribe("QueueMemberPenalty", self._queue_member_penalty)
        bus_consumer.subscribe("QueueMemberRemoved", self._queue_member_removed)
        bus_consumer.subscribe("QueueMemberRinginuse", self._queue_member_ringinuse)
        bus_consumer.subscribe("QueueMemberStatus", self._queue_member_status)

    def _queue_caller_abandon(self, event):
        tenant_uuid = self._extract_tenant_uuid(event)
        if event["Context"] == "queue":
            self._livestats(event, tenant_uuid)
        bus_event = QueueCallerAbandonEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_caller_join(self, event):
        tenant_uuid = self._extract_tenant_uuid(event)
        # Check if the call concerns a Queue and not a group
        if event["Context"] == "queue":
            self._livestats(event, tenant_uuid)
        bus_event = QueueCallerJoinEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_caller_leave(self, event):
        tenant_uuid = self._extract_tenant_uuid(event)
        if event["Context"] == "queue":
            self._livestats(event, tenant_uuid)
            self._agents_status(event, tenant_uuid)
        bus_event = QueueCallerLeaveEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_member_added(self, event):
        if "usersharedlines" in event.get("Interface", "").lower():
            logger.debug(
                f"Ignoring event with usersharedlines interface: {event.get('Interface')}"
            )
            bus_event = QueueMemberAddedEvent(
                event, "00000000-0000-0000-0000-000000000000"
            )
        else:
            tenant_uuid = self._extract_tenant_uuid(event)
            self._agents_status(event, tenant_uuid)
            bus_event = QueueMemberAddedEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_member_pause(self, event):
        if "usersharedlines" in event.get("Interface", "").lower():
            logger.debug(
                f"Ignoring event with usersharedlines interface: {event.get('Interface')}"
            )
            bus_event = QueueMemberPauseEvent(
                event, "00000000-0000-0000-0000-000000000000"
            )
        else:
            tenant_uuid = self._extract_tenant_uuid(event)
            self._agents_status(event, tenant_uuid)
            bus_event = QueueMemberPauseEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_member_penalty(self, event):
        if "usersharedlines" in event.get("Interface", "").lower():
            logger.debug(
                f"Ignoring event with usersharedlines interface: {event.get('Interface')}"
            )
            bus_event = QueueMemberPenaltyEvent(
                event, "00000000-0000-0000-0000-000000000000"
            )
        else:
            tenant_uuid = self._extract_tenant_uuid(event)
            self._agents_status(event, tenant_uuid)
            bus_event = QueueMemberPenaltyEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_member_removed(self, event):
        if "usersharedlines" in event.get("Interface", "").lower():
            logger.debug(
                f"Ignoring event with usersharedlines interface: {event.get('Interface')}"
            )
            bus_event = QueueMemberRemovedEvent(
                event, "00000000-0000-0000-0000-000000000000"
            )
        else:
            tenant_uuid = self._extract_tenant_uuid(event)
            self._agents_status(event, tenant_uuid)
            bus_event = QueueMemberRemovedEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_member_ringinuse(self, event):
        if "usersharedlines" in event.get("Interface", "").lower():
            logger.debug(
                f"Ignoring event with usersharedlines interface: {event.get('Interface')}"
            )
            bus_event = QueueMemberRingInUseEvent(
                event, "00000000-0000-0000-0000-000000000000"
            )
        else:
            tenant_uuid = self._extract_tenant_uuid(event)
            self._agents_status(event, tenant_uuid)
            bus_event = QueueMemberRingInUseEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_member_status(self, event):
        if "usersharedlines" in event.get("Interface", "").lower():
            logger.debug(
                f"Ignoring event with usersharedlines interface: {event.get('Interface')}"
            )
            bus_event = QueueMemberStatusEvent(
                event, "00000000-0000-0000-0000-000000000000"
            )
        else:
            tenant_uuid = self._extract_tenant_uuid(event)
            self._agents_status(event, tenant_uuid)
            bus_event = QueueMemberStatusEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_livestats(self, tenant_uuid):
        # Caller holds ``self._lock``. Publishes the whole stats map.
        bus_event = QueueLiveStatsEvent(self._stats, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_agents_status(self, tenant_uuid, agent):
        # Caller holds ``self._lock``. Publishes a single agent object.
        bus_event = QueueAgentsStatusEvent(self._agents[tenant_uuid][agent], tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def get_agents_status(self, tenant_uuid: str) -> dict:
        with self._lock:
            if not self._agents.get(tenant_uuid):
                self._agents.update({tenant_uuid: {}})
                agentList = self.confd.agents.list(tenant_uuid=tenant_uuid)
                agentStatus = self.agentd.agents.get_agent_statuses(
                    tenant_uuid=tenant_uuid
                )

                for agent in agentList["items"]:
                    status = next(
                        (
                            s
                            for s in agentStatus
                            if getattr(s, "id", None) == agent["id"]
                        ),
                        None,
                    )
                    runtime_queues, paused_queues, all_queues = (
                        _membership_from_status(status)
                    )
                    # Seed the legacy ``queue`` from agentd's queue list, falling
                    # back to confd when agentd has no status (or no queues) for
                    # the agent, so a configured agent never gets ``queue:
                    # false``.
                    home_queues = all_queues or _queue_names(agent)
                    home_queue = home_queues[0] if home_queues else False

                    if not self._agents[tenant_uuid].get(agent["id"]):
                        self._agents[tenant_uuid][agent["id"]] = _build_agent_state(
                            agent["id"],
                            agent["number"],
                            _agent_fullname(agent),
                            runtime_queues,
                            paused_queues,
                            home_queue,
                        )
            logger.debug(
                "agents status for tenant %s: %s",
                tenant_uuid,
                self._agents[tenant_uuid],
            )
            return self._agents[tenant_uuid]

    def add_agent(self, tenant_uuid, agent, member):
        with self._lock:
            if not self._agents[tenant_uuid].get(agent):
                agentInfo = self.confd.agents.get(
                    resource_or_id=agent, tenant_uuid=tenant_uuid
                )
                # Not yet runtime-logged: the triggering membership event
                # populates ``queues`` right after this call, so seed with empty
                # membership. ``home_queue`` seeds the legacy ``queue`` string
                # from the confd-configured queues (matches the pre-multi-queue
                # behaviour).
                configured = _queue_names(agentInfo)
                self._agents[tenant_uuid][agent] = _build_agent_state(
                    agent,
                    member,
                    _agent_fullname(agentInfo),
                    home_queue=configured[0] if configured else False,
                )

    def get_stats(self, name):
        with self._lock:
            # If the queue stats doesnot exist, create the object with default values || Reset if day is different
            if not self._stats.get(name) or (
                self._stats.get(name)
                and self._stats[name]["updated_at"] != datetime.datetime.now().day
            ):
                self._stats.update(
                    {
                        name: {
                            "count": 0,
                            "count_color": "green",
                            "received": 0,
                            "abandonned": 0,
                            "answered": 0,
                            "awr": 0,
                            "waiting_calls": [],
                            "updated_at": datetime.datetime.now().day,
                        }
                    }
                )
            return self._stats[name]

    def _agents_status(self, event, tenant_uuid):
        with self._lock:
            self._agents_status_locked(event, tenant_uuid)

    def _agents_status_locked(self, event, tenant_uuid):
        # Caller holds ``self._lock``. ``agents`` aliases the instance state so
        # the body below reads/mutates the shared dict under the lock.
        agents = self._agents
        agent = 0

        required = _REQUIRED_EVENT_FIELDS.get(event.get("Event"))
        if required:
            missing = [field for field in required if field not in event]
            if missing:
                logger.warning(
                    "Dropping malformed %s event for tenant %s: missing fields %s",
                    event.get("Event"),
                    tenant_uuid,
                    missing,
                )
                return

        # Check if agents for this tenant exists
        if event["Event"] != "QueueCallerLeave" and event["Membership"] == "dynamic":
            interface = AGENT_ID_FROM_IFACE.match(event["Interface"])
            agent = int(interface.group(1))
            if not agents.get(tenant_uuid):
                self.get_agents_status(tenant_uuid)
            if not agents[tenant_uuid].get(agent):
                member = MEMBER_NUM_FROM_AGENT.match(event["MemberName"])
                member_num = int(member.group(1))
                self.add_agent(tenant_uuid, agent, member_num)

        # QueueCallerLeave Get info about call
        if (
            event["Event"] == "QueueCallerLeave"
            and event["ConnectedLineNum"] != "<unknown>"
        ):
            agentID = event["ConnectedLineNum"]
            if agents.get(tenant_uuid):
                for i, k in enumerate(agents[tenant_uuid]):
                    if agents[tenant_uuid].get(k):
                        if agents[tenant_uuid][k]["number"] == agentID:
                            agents[tenant_uuid][k]["talked_with_number"] = event[
                                "CallerIDNum"
                            ]
                            agents[tenant_uuid][k]["talked_with_name"] = event[
                                "CallerIDName"
                            ]
                            agent = k
                            break

        # QueueMemberStatus
        if event["Event"] == "QueueMemberStatus" and event["Membership"] == "dynamic":
            if event["Status"] == "5":
                # WDA is disconnected - websocket KO
                agents[tenant_uuid][agent]["is_offline"] = True
            if event["Status"] == "6":
                # Ringing
                agents[tenant_uuid][agent]["is_ringing"] = True
            if event["Status"] == "2":
                # In comm
                agents[tenant_uuid][agent]["is_talking"] = True
                agents[tenant_uuid][agent]["is_ringing"] = False
                agents[tenant_uuid][agent]["talked_at"] = (
                    datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                )

            if event["Status"] == "1":
                # Hangup
                agents[tenant_uuid][agent]["is_talking"] = False
                agents[tenant_uuid][agent]["is_ringing"] = False
                agents[tenant_uuid][agent]["is_offline"] = False
                agents[tenant_uuid][agent]["talked_at"] = ""
                agents[tenant_uuid][agent]["talked_with_number"] = ""
                agents[tenant_uuid][agent]["talked_with_name"] = ""

        if event["Event"] == "QueueMemberAdded" and event["Membership"] == "dynamic":
            # Handle connection to a queue (an agent may serve several queues)
            state = agents[tenant_uuid][agent]
            state.setdefault("queues", [])
            if event["Queue"] not in state["queues"]:
                state["queues"].append(event["Queue"])
            state["interface"] = event["StateInterface"]
            if not state.get("logged_at"):
                # LoginTime: set on first observed queue join, kept across
                # further joins. Keying on the empty timestamp (rather than on
                # prior membership) also backfills it after a REST/restart
                # bootstrap, which seeds ``queues`` but no ``logged_at``.
                state["logged_at"] = datetime.datetime.now().strftime(
                    "%Y-%m-%dT%H:%M:%S.%f"
                )
            _sync_derived(state)

        if event["Event"] == "QueueMemberRemoved" and event["Membership"] == "dynamic":
            # Handle disconnection from a single queue
            state = agents[tenant_uuid][agent]
            state.setdefault("queues", [])
            state.setdefault("paused_queues", [])
            if event["Queue"] in state["queues"]:
                state["queues"].remove(event["Queue"])
            else:
                # No-op removal: either a duplicate/late event, or the queue
                # name in this live event does not match the one agentd
                # reported at bootstrap. Log it loudly so a silent drift
                # between the two namespaces is surfaced rather than leaving
                # the agent wrongly flagged as a member of that queue.
                logger.warning(
                    "QueueMemberRemoved for tenant %s agent %s in queue %s: "
                    "not in tracked membership %s (duplicate event or a "
                    "queue-name mismatch between agentd bootstrap and events)",
                    tenant_uuid,
                    agent,
                    event["Queue"],
                    state["queues"],
                )
            if event["Queue"] in state["paused_queues"]:
                state["paused_queues"].remove(event["Queue"])
            if not state["queues"]:
                # Fully logged out: reset session/device fields
                state["is_talking"] = False
                state["is_ringing"] = False
                state["logged_at"] = ""
                state["paused_at"] = ""
                state["talked_at"] = ""
                state["talked_with_number"] = ""
                state["talked_with_name"] = ""
            _sync_derived(state)

        if event["Event"] == "QueueMemberPause" and event["Membership"] == "dynamic":
            # Handle pause, tracked per queue (Asterisk pauses per membership).
            # The agent is always materialised by the membership block above
            # (add_agent), so a fully-formed state dict is guaranteed here.
            state = agents[tenant_uuid][agent]
            state.setdefault("queues", [])
            state.setdefault("paused_queues", [])
            if event["Paused"] == "1":
                # Keep the invariant paused_queues ⊆ queues: never report an
                # agent as paused in a queue it is not a (runtime) member of.
                # This drops stray pauses that arrive after a QueueMemberRemoved
                # for the same queue, which would otherwise leave a logged-out
                # agent flagged as paused.
                if event["Queue"] not in state["queues"]:
                    logger.warning(
                        "Ignoring pause for tenant %s agent %s in queue %s: "
                        "not a member of that queue",
                        tenant_uuid,
                        agent,
                        event["Queue"],
                    )
                else:
                    if event["Queue"] not in state["paused_queues"]:
                        state["paused_queues"].append(event["Queue"])
                    if not state.get("paused_at"):
                        # LastPause: set on first observed pause, kept while any
                        # queue is paused. Keying on the empty timestamp also
                        # backfills it after a bootstrap that seeds
                        # ``paused_queues`` but no ``paused_at``.
                        state["paused_at"] = datetime.datetime.now().strftime(
                            "%Y-%m-%dT%H:%M:%S.%f"
                        )
            else:
                if event["Queue"] in state["paused_queues"]:
                    state["paused_queues"].remove(event["Queue"])
                if not state["paused_queues"]:
                    state["paused_at"] = ""
            _sync_derived(state)

        if agent != 0:
            self._queue_agents_status(tenant_uuid, agent)

    def _livestats(self, event, tenant_uuid):
        with self._lock:
            self._livestats_locked(event, tenant_uuid)

    def _livestats_locked(self, event, tenant_uuid):
        # Caller holds ``self._lock``. ``stats`` aliases the instance state.
        stats = self._stats
        name = event["Queue"]

        self.get_stats(name)

        queue_event = event["Event"]
        if queue_event == "QueueCallerJoin":
            stats[name]["count"] = int(event["Count"])
            stats[name]["updated_at"] = datetime.datetime.now().day
            stats[name]["waiting_calls"].append(
                {
                    "uniqueid": event["Uniqueid"],
                    "calleridnum": event["CallerIDNum"],
                    "calleridname": event["CallerIDName"],
                    "position": event["Position"],
                    "channelstate": event["ChannelState"],
                    "channelstatedesc": event["ChannelStateDesc"],
                    "time": event["ChanVariable"]["WAZO_ANSWER_TIME"],
                    "entryexten": event["ChanVariable"]["WAZO_ENTRY_EXTEN"],
                }
            )
        elif queue_event == "QueueCallerAbandon":
            stats[name]["abandonned"] += 1
            stats[name]["updated_at"] = datetime.datetime.now().day
            stats[name]["answered"] -= 1
            if stats[name]["received"] > 0:
                stats[name]["awr"] = math.ceil(
                    stats[name]["answered"] / stats[name]["received"] * 100
                )
            stats[name]["waiting_calls"] = [
                call
                for call in stats[name]["waiting_calls"]
                if call["uniqueid"] != event["Uniqueid"]
            ]
        elif queue_event == "QueueCallerLeave":
            stats[name]["count"] = int(event["Count"])
            stats[name]["updated_at"] = datetime.datetime.now().day
            stats[name]["answered"] += 1
            stats[name]["received"] += 1
            if stats[name]["received"] > 0:
                stats[name]["awr"] = math.ceil(
                    stats[name]["answered"] / stats[name]["received"] * 100
                )
            stats[name]["waiting_calls"] = [
                call
                for call in stats[name]["waiting_calls"]
                if call["uniqueid"] != event["Uniqueid"]
            ]

        # Set color depending on limit value
        stats[name]["count_color"] = "green"
        if stats[name]["count"] > 1:
            stats[name]["count_color"] = "red"

        self._queue_livestats(tenant_uuid)

    def _extract_tenant_uuid(self, event):
        try:
            tenant_uuid = event["ChanVariable"]["WAZO_TENANT_UUID"]
        except (KeyError, TypeError):
            interface = AGENT_ID_FROM_IFACE.match(event["Interface"])
            if not interface:
                raise ValueError(
                    f"Interface '{event['Interface']}' does not match expected pattern"
                )
            agent_id = int(interface.group(1))
            agent = self.confd.agents.get(agent_id)
            tenant_uuid = agent["tenant_uuid"]
        return tenant_uuid

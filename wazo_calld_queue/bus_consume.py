# Copyright 2016-2023 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import datetime
import logging
import math
import re

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


stats = {}
agents = {}


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

    ``queue`` / ``is_logged`` / ``is_paused`` are never written directly; they
    always reflect ``queues`` (runtime membership) and ``paused_queues`` so an
    agent serving several queues stays consistent.
    """
    queues = state.setdefault("queues", [])
    paused_queues = state.setdefault("paused_queues", [])
    state["queue"] = queues[0] if queues else False
    state["is_logged"] = bool(queues)
    state["is_paused"] = bool(paused_queues)


def _agent_fullname(info):
    fullname = ""
    if str(info["firstname"]) != "None":
        fullname = str(info["firstname"])
    if str(info["lastname"]) != "None":
        fullname += " " + str(info["lastname"])
    return fullname


def _membership_from_status(status):
    """Derive runtime ``(queues, paused_queues)`` from a live agentd status.

    agentd reports every configured queue with its current per-queue
    ``logged`` / ``paused`` flags, so the runtime membership is exactly the
    queues flagged ``logged`` (resp. ``paused``) — not every configured queue.
    This keeps a multi-queue agent's bootstrap snapshot accurate instead of
    over-reporting membership until the next live event corrects it.
    """
    queues = []
    paused_queues = []
    for queue in getattr(status, "queues", None) or []:
        name = queue.get("name")
        if not name:
            continue
        if queue.get("logged"):
            queues.append(name)
        if queue.get("paused"):
            paused_queues.append(name)
    return queues, paused_queues


def _build_agent_state(agent_id, number, fullname, queues=None, paused_queues=None):
    """Build a fresh agent state dict.

    ``queues`` is the runtime membership (the queues the agent is currently
    logged into) and ``paused_queues`` the queues it is paused in — both
    sourced from the live agentd per-queue status at bootstrap. The
    ``paused_queues ⊆ queues`` invariant is enforced here so a stale pause
    never produces a phantom paused flag. ``queue`` / ``is_logged`` /
    ``is_paused`` are derived from these sets via ``_sync_derived``.
    """
    runtime_queues = list(queues or [])
    paused_queues = [q for q in (paused_queues or []) if q in runtime_queues]
    state = {
        "id": agent_id,
        "number": number,
        "fullname": fullname,
        "queue": False,
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

    def _queue_livestats(self, event, tenant_uuid):
        bus_event = QueueLiveStatsEvent(event, tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def _queue_agents_status(self, event, tenant_uuid, agent):
        bus_event = QueueAgentsStatusEvent(event[tenant_uuid][agent], tenant_uuid)
        self.bus_publisher.publish(bus_event)

    def get_agents_status(self, tenant_uuid: str) -> dict:
        if not agents.get(tenant_uuid):
            agents.update({tenant_uuid: {}})
            agentList = self.confd.agents.list(tenant_uuid=tenant_uuid)
            agentStatus = self.agentd.agents.get_agent_statuses(tenant_uuid=tenant_uuid)

            for agent in agentList["items"]:
                status = next(
                    (s for s in agentStatus if getattr(s, "id", None) == agent["id"]),
                    None,
                )
                runtime_queues, paused_queues = _membership_from_status(status)

                if not agents[tenant_uuid].get(agent["id"]):
                    agents[tenant_uuid][agent["id"]] = _build_agent_state(
                        agent["id"],
                        agent["number"],
                        _agent_fullname(agent),
                        runtime_queues,
                        paused_queues,
                    )
        logger.debug("agents status for tenant %s: %s", tenant_uuid, agents[tenant_uuid])
        return agents[tenant_uuid]

    def add_agent(self, tenant_uuid, agent, member):
        if not agents[tenant_uuid].get(agent):
            agentInfo = self.confd.agents.get(
                resource_or_id=agent, tenant_uuid=tenant_uuid
            )
            # Not yet runtime-logged: the triggering membership event populates
            # ``queues`` right after this call, so seed with empty membership.
            agents[tenant_uuid][agent] = _build_agent_state(
                agent,
                member,
                _agent_fullname(agentInfo),
            )

    def get_stats(self, name):
        # If the queue stats doesnot exist, create the object with default values || Reset if day is different
        if not stats.get(name) or (
            stats.get(name) and stats[name]["updated_at"] != datetime.datetime.now().day
        ):
            stats.update(
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
        return stats[name]

    def _agents_status(self, event, tenant_uuid):
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
            self._queue_agents_status(agents, tenant_uuid, agent)

    def _livestats(self, event, tenant_uuid):
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

        self._queue_livestats(stats, tenant_uuid)

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

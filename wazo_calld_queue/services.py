# Copyright 2018-2026 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import logging

from wazo_agentd_client.error import (
    AgentdClientError,
    ALREADY_IN_QUEUE,
    NOT_IN_QUEUE,
    NOT_LOGGED,
    NO_SUCH_AGENT,
    NO_SUCH_QUEUE,
)

from .exceptions import (
    AgentdUpstreamError,
    AgentHasNoLine,
    AgentNotLogged,
    AgentWdaNotConnected,
    NoSuchAgentOrQueue,
    SupervisorNotInQueue,
)

logger = logging.getLogger(__name__)

# AMI ExtensionState ``Status`` for a device with no registered contact: the
# agent application (WDA) is not connected, so the agent cannot take calls.
EXTENSION_STATE_UNAVAILABLE = "4"


class QueueService(object):
    def __init__(self, amid, confd, ari, agentd, publisher):
        self.amid = amid
        self.confd = confd
        self.ari = ari
        self.agentd = agentd
        self.publisher = publisher

    def list_queues(self):
        queues = self.amid.action("queuesummary")
        q = []
        for queue in queues:
            if queue.get("Event") == "QueueSummary":
                q.append(self._queues(queue))

        return q

    def get_queue(self, queue_name):
        queue = self.amid.action("queuestatus", {"Queue": queue_name})
        q = {"members": []}
        for ev in queue:
            if ev.get("Event") == "QueueParams":
                q.update(self._queue(ev))
            if ev.get("Event") == "QueueMember":
                q["members"].append(self._member(ev))
        return q

    def add_queue_member(self, queue_name, member):
        add_member = {
            "Queue": queue_name,
            "Interface": member.get("interface"),
            "Penalty": member.get("penalty"),
            "Paused": member.get("paused"),
            "MemberName": member.get("member_name"),
            "StateInterface": member.get("state_interface"),
        }
        return self.amid.action("queueadd", add_member)

    def remove_queue_member(self, queue_name, interface):
        remove_member = {"Queue": queue_name, "Interface": interface}
        return self.amid.action("queueremove", remove_member)

    def pause_queue_member(self, queue_name, params):
        pause_member = {
            "Interface": params.get("interface"),
            "Paused": params.get("paused"),
            "Queue": queue_name,
            "Reason": params.get("reason"),
        }
        return self.amid.action("queuepause", pause_member)

    def connect_agent(self, queue_name, agent_id, supervisor_uuid, tenant_uuid):
        queue_id = self._authorize_supervisor(
            supervisor_uuid, queue_name, tenant_uuid
        )
        # The agent's application (WDA) must be connected before we connect it
        # to a queue: an agent may still hold an agentd session while its device
        # is Unavailable (websocket KO — see is_offline / "case 1"), and
        # connecting it would put a phantom member in the queue. Resolve the
        # agent's line and check its device state up front so the supervisor
        # gets a clear, specific error instead of a silent dead member.
        agent = self.confd.agents.get(agent_id, tenant_uuid=tenant_uuid)
        extension, context = self._resolve_agent_line(agent_id, agent, tenant_uuid)
        self._assert_wda_connected(agent_id, extension, context)
        try:
            self._delegate(
                self.agentd.agents.agent_login_to_queue,
                agent_id,
                queue_id,
                tenant_uuid,
            )
        except AgentNotLogged:
            # The agent is authenticated to WDA but has no agentd session yet:
            # log it in on its own line, then keep only the selected queue.
            self._login_agent_to_single_queue(
                agent, extension, context, queue_id, tenant_uuid
            )
        logger.info(
            "supervisor %s connected agent %s to queue %s (id %s, tenant %s)",
            supervisor_uuid,
            agent_id,
            queue_name,
            queue_id,
            tenant_uuid,
        )

    def disconnect_agent(self, queue_name, agent_id, supervisor_uuid, tenant_uuid):
        queue_id = self._authorize_supervisor(
            supervisor_uuid, queue_name, tenant_uuid
        )
        self._delegate(
            self.agentd.agents.agent_logoff_from_queue,
            agent_id,
            queue_id,
            tenant_uuid,
        )
        logger.info(
            "supervisor %s disconnected agent %s from queue %s (id %s, tenant %s)",
            supervisor_uuid,
            agent_id,
            queue_name,
            queue_id,
            tenant_uuid,
        )

    def _authorize_supervisor(self, supervisor_uuid, queue_name, tenant_uuid):
        """Return the target queue_id iff the supervisor is a member of it."""
        user = self.confd.users.get(supervisor_uuid, tenant_uuid=tenant_uuid)
        agent = user.get("agent")
        if not agent:
            raise SupervisorNotInQueue(queue_name)
        supervisor_agent = self.confd.agents.get(agent["id"], tenant_uuid=tenant_uuid)
        for queue in supervisor_agent.get("queues") or []:
            if queue.get("name") == queue_name:
                return queue["id"]
        raise SupervisorNotInQueue(queue_name)

    def _login_agent_to_single_queue(
        self, agent, extension, context, queue_id, tenant_uuid
    ):
        """Full agentd login for an agent without a session, then enforce
        strict single-queue membership on the selected queue.

        The agent object and its resolved ``(extension, context)`` are passed
        in by ``connect_agent`` (which already fetched/resolved them for the WDA
        check), so we don't re-query confd here.

        ``login_agent`` makes the agent join *every* queue it is configured
        for in confd, so we prune the others and (re)ensure the selected one
        (which may not be one of the agent's configured queues).
        """
        agent_id = agent["id"]
        self.agentd.agents.login_agent(
            agent_id, extension, context, tenant_uuid=tenant_uuid
        )
        for queue in agent.get("queues") or []:
            if queue["id"] != queue_id:
                self._delegate(
                    self.agentd.agents.agent_logoff_from_queue,
                    agent_id,
                    queue["id"],
                    tenant_uuid,
                )
        self._delegate(
            self.agentd.agents.agent_login_to_queue, agent_id, queue_id, tenant_uuid
        )

    def _assert_wda_connected(self, agent_id, extension, context):
        """Raise ``AgentWdaNotConnected`` if the agent's device (WDA) is not
        registered.

        Query Asterisk for the hint state of the agent's own extension via AMI
        ``ExtensionState``: a ``Status`` of Unavailable means no contact is
        registered for the device, i.e. the WDA application is not connected.
        Only an explicit Unavailable blocks the connect — an unknown/absent
        status is treated as "reachable" so a missing hint never wrongly bars a
        supervisor from connecting an agent.
        """
        response = self.amid.action(
            "ExtensionState", {"Exten": extension, "Context": context}
        )
        for message in response or []:
            if str(message.get("Status")) == EXTENSION_STATE_UNAVAILABLE:
                raise AgentWdaNotConnected(agent_id)

    def _resolve_agent_line(self, agent_id, agent, tenant_uuid):
        """Resolve the agent's (extension, context) from confd via its user's
        primary line. Raise ``AgentHasNoLine`` if none is configured."""
        for user in agent.get("users") or []:
            full_user = self.confd.users.get(user["uuid"], tenant_uuid=tenant_uuid)
            for line in full_user.get("lines") or []:
                for extension in line.get("extensions") or []:
                    exten = extension.get("exten")
                    context = extension.get("context")
                    if exten and context:
                        return exten, context
        raise AgentHasNoLine(agent_id)

    def _delegate(self, action, agent_id, queue_id, tenant_uuid):
        try:
            action(agent_id, queue_id, tenant_uuid=tenant_uuid)
        except AgentdClientError as e:
            code = getattr(e, "error", None)
            if code == NOT_LOGGED:
                raise AgentNotLogged()
            if code in (NO_SUCH_QUEUE, NO_SUCH_AGENT):
                raise NoSuchAgentOrQueue()
            if code in (ALREADY_IN_QUEUE, NOT_IN_QUEUE):
                return  # PUT is idempotent: the target state is reached
            raise AgentdUpstreamError(code)

    def livestats(self, queue_name):
        return self.publisher.get_stats(queue_name)

    def agents_status(self, tenant_uuid):
        return self.publisher.get_agents_status(tenant_uuid)

    def _queues(self, queue):
        return {
            "logged_in": queue["LoggedIn"],
            "available": queue["Available"],
            "talk_time": queue["TalkTime"],
            "longest_hold_time": queue["LongestHoldTime"],
            "queue": queue["Queue"],
            "talking": queue["Talking"],
            "hold_time": queue["HoldTime"],
            "callers": queue["Callers"],
        }

    def _queue(self, queue):
        return {
            "service_level_perf": queue.get("ServiceLevelPerf"),
            "talk_time": queue.get("TalkTime"),
            "calls": queue.get("Calls"),
            "max": queue.get("Max"),
            "completed": queue.get("Completed"),
            "service_level": queue.get("ServiceLevel"),
            "service_level_perf2": queue.get("ServiceLevelPerf2"),
            "abandoned": queue.get("Abandoned"),
            "strategy": queue.get("Strategy"),
            "queue": queue.get("Queue"),
            "weight": queue.get("Weight"),
            "hold_time": queue.get("HoldTime"),
        }

    def _member(self, member):
        return {
            "status": member.get("Status"),
            "penalty": member.get("Penalty"),
            "calls_taken": member.get("CallsTaken"),
            "name": member.get("Name"),
            "skills": member.get("Skills"),
            "last_pause": member.get("LastPause"),
            "queue": member.get("Queue"),
            "membership": member.get("Membership"),
            "incall": member.get("Incall"),
            "location": member.get("Location"),
            "last_call": member.get("LastCall"),
            "paused": member.get("Paused"),
            "paused_reason": member.get("PausedReason"),
            "state_interface": member.get("StateInterface"),
        }

    def intercept_call(self, queue_name, params):
        channel = self.ari.channels.get(channelId=params.get("call_id"))
        channel_name = channel.json["name"]
        destination = params.get("destination")

        intercept_action = {
            "ActionID": 123,
            "Queue": queue_name,
            "Caller": channel_name,
            "WithdrawInfo": destination,
        }

        return self.amid.action("queuewithdrawcaller", intercept_action)

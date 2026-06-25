# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from unittest.mock import Mock

import pytest
from wazo_agentd_client.error import (
    ALREADY_IN_QUEUE,
    NO_SUCH_AGENT,
    NO_SUCH_QUEUE,
    NOT_IN_QUEUE,
    NOT_LOGGED,
    AgentdClientError,
)

from wazo_calld_queue.exceptions import (
    AgentdUpstreamError,
    AgentHasNoLine,
    AgentWdaNotConnected,
    NoSuchAgentOrQueue,
    SupervisorNotInQueue,
)
from wazo_calld_queue.services import QueueService

# AMI ExtensionState ``Status`` codes (AST_EXTENSION_*): 0 idle / reachable,
# 4 unavailable (device unregistered -> WDA application disconnected).
WDA_CONNECTED = "0"
WDA_UNAVAILABLE = "4"


@pytest.fixture
def service():
    return QueueService(
        amid=Mock(name="amid"),
        confd=Mock(name="confd"),
        ari=Mock(name="ari"),
        agentd=Mock(name="agentd"),
        publisher=Mock(name="publisher"),
    )


class TestListQueues:
    def test_maps_queue_summary_events(self, service):
        service.amid.action.return_value = [
            {
                "Event": "QueueSummary",
                "Queue": "support",
                "LoggedIn": "2",
                "Available": "1",
                "TalkTime": "30",
                "LongestHoldTime": "10",
                "Talking": "1",
                "HoldTime": "5",
                "Callers": "0",
            },
            {"Event": "QueueSummaryComplete"},  # must be ignored
        ]

        result = service.list_queues()

        service.amid.action.assert_called_once_with("queuesummary")
        assert result == [
            {
                "logged_in": "2",
                "available": "1",
                "talk_time": "30",
                "longest_hold_time": "10",
                "queue": "support",
                "talking": "1",
                "hold_time": "5",
                "callers": "0",
            }
        ]


class TestGetQueue:
    def test_aggregates_params_and_members(self, service):
        service.amid.action.return_value = [
            {
                "Event": "QueueParams",
                "Queue": "support",
                "Max": "0",
                "Strategy": "ringall",
                "Calls": "1",
            },
            {
                "Event": "QueueMember",
                "Name": "Agent/1001",
                "Queue": "support",
                "Status": "1",
                "Penalty": "0",
            },
            {"Event": "QueueStatusComplete"},
        ]

        result = service.get_queue("support")

        service.amid.action.assert_called_once_with("queuestatus", {"Queue": "support"})
        assert result["queue"] == "support"
        assert result["strategy"] == "ringall"
        assert len(result["members"]) == 1
        assert result["members"][0]["name"] == "Agent/1001"
        assert result["members"][0]["status"] == "1"


class TestMemberActions:
    def test_add_queue_member_builds_amid_action(self, service):
        member = {
            "interface": "SIP/abc",
            "penalty": 1,
            "paused": 0,
            "member_name": "John",
            "state_interface": "SIP/abc",
        }

        service.add_queue_member("support", member)

        service.amid.action.assert_called_once_with(
            "queueadd",
            {
                "Queue": "support",
                "Interface": "SIP/abc",
                "Penalty": 1,
                "Paused": 0,
                "MemberName": "John",
                "StateInterface": "SIP/abc",
            },
        )

    def test_remove_queue_member_builds_amid_action(self, service):
        service.remove_queue_member("support", "SIP/abc")

        service.amid.action.assert_called_once_with(
            "queueremove", {"Queue": "support", "Interface": "SIP/abc"}
        )

    def test_pause_queue_member_builds_amid_action(self, service):
        params = {"interface": "SIP/abc", "paused": 1, "reason": "lunch"}

        service.pause_queue_member("support", params)

        service.amid.action.assert_called_once_with(
            "queuepause",
            {
                "Interface": "SIP/abc",
                "Paused": 1,
                "Queue": "support",
                "Reason": "lunch",
            },
        )


class TestInterceptCall:
    def test_intercepts_via_ari_channel_name(self, service):
        service.ari.channels.get.return_value = Mock(json={"name": "PJSIP/abc-0001"})
        params = {"call_id": "chan-1", "destination": "1234"}

        service.intercept_call("support", params)

        service.ari.channels.get.assert_called_once_with(channelId="chan-1")
        service.amid.action.assert_called_once_with(
            "queuewithdrawcaller",
            {
                "ActionID": 123,
                "Queue": "support",
                "Caller": "PJSIP/abc-0001",
                "WithdrawInfo": "1234",
            },
        )


class TestStatsDelegation:
    """``QueueService`` delegates stats/agent queries to its bus event handler
    instance (``self.publisher``), not via an unbound class-method call."""

    def test_livestats_delegates_to_handler(self, service):
        result = service.livestats("support")

        service.publisher.get_stats.assert_called_once_with("support")
        assert result is service.publisher.get_stats.return_value

    def test_agents_status_delegates_to_handler(self, service):
        result = service.agents_status("tenant-1")

        service.publisher.get_agents_status.assert_called_once_with("tenant-1")
        assert result is service.publisher.get_agents_status.return_value


class TestConnectDisconnectAgent:
    """Per-queue connect/disconnect: authorize the supervisor against the
    target queue (confd), then delegate to agentd."""

    def _authorize_supervisor_for(
        self,
        service,
        queue_name="support",
        queue_id=42,
        target_agent_id=3,
        user_uuid="user-3",
        lines=None,
        wda_status=WDA_CONNECTED,
    ):
        """Supervisor authorized for the queue; the target agent already has an
        agentd session (``agent_login_to_queue`` succeeds) and resolves to a
        user with a line whose device (WDA) reports ``wda_status``."""
        if lines is None:
            lines = [{"extensions": [{"exten": "1001", "context": "default"}]}]
        users = {
            "sup-uuid": {"agent": {"id": 7}},
            user_uuid: {"lines": lines},
        }
        agents = {
            7: {"queues": [{"id": queue_id, "name": queue_name}]},
            target_agent_id: {
                "id": target_agent_id,
                "users": [{"uuid": user_uuid}],
                "queues": [{"id": queue_id, "name": queue_name}],
            },
        }
        service.confd.users.get.side_effect = lambda uuid, tenant_uuid=None: users[uuid]
        service.confd.agents.get.side_effect = (
            lambda agent_id, tenant_uuid=None: agents[agent_id]
        )
        service.amid.action.return_value = [
            {"Response": "Success", "Status": wda_status}
        ]

    def test_connect_authorized_delegates_to_agentd(self, service):
        self._authorize_supervisor_for(service)

        service.connect_agent("support", 3, "sup-uuid", "t1")

        service.confd.users.get.assert_any_call("sup-uuid", tenant_uuid="t1")
        service.confd.agents.get.assert_any_call(7, tenant_uuid="t1")
        service.agentd.agents.agent_login_to_queue.assert_called_once_with(
            3, 42, tenant_uuid="t1"
        )

    def test_disconnect_authorized_delegates_to_agentd(self, service):
        self._authorize_supervisor_for(service)

        service.disconnect_agent("support", 3, "sup-uuid", "t1")

        service.agentd.agents.agent_logoff_from_queue.assert_called_once_with(
            3, 42, tenant_uuid="t1"
        )

    def test_authorization_is_scoped_to_request_tenant(self, service):
        # The supervisor/agent lookups and the agentd action must all carry the
        # request tenant, so a supervisor cannot reach another tenant's roster.
        self._authorize_supervisor_for(service)

        service.connect_agent("support", 3, "sup-uuid", "tenant-b")

        service.confd.users.get.assert_any_call("sup-uuid", tenant_uuid="tenant-b")
        service.confd.agents.get.assert_any_call(7, tenant_uuid="tenant-b")
        service.agentd.agents.agent_login_to_queue.assert_called_once_with(
            3, 42, tenant_uuid="tenant-b"
        )

    def test_supervisor_without_agent_is_rejected(self, service):
        service.confd.users.get.return_value = {"agent": None}

        with pytest.raises(SupervisorNotInQueue):
            service.connect_agent("support", 3, "sup-uuid", "t1")

        service.agentd.agents.agent_login_to_queue.assert_not_called()

    def test_supervisor_not_in_target_queue_is_rejected(self, service):
        service.confd.users.get.return_value = {"agent": {"id": 7}}
        service.confd.agents.get.return_value = {
            "queues": [{"id": 99, "name": "sales"}]
        }

        with pytest.raises(SupervisorNotInQueue):
            service.connect_agent("support", 3, "sup-uuid", "t1")

        service.agentd.agents.agent_login_to_queue.assert_not_called()

    def _setup_fresh_login(
        self,
        service,
        queue_id=42,
        queue_name="support",
        target_agent_id=3,
        target_queues=None,
        user_uuid="user-3",
        lines=None,
    ):
        """Supervisor authorized for the queue; target agent (not yet logged
        into agentd) resolves to a user with a line/extension via confd."""
        if target_queues is None:
            target_queues = [{"id": queue_id, "name": queue_name}]
        if lines is None:
            lines = [{"extensions": [{"exten": "1001", "context": "default"}]}]
        users = {
            "sup-uuid": {"agent": {"id": 7}},
            user_uuid: {"lines": lines},
        }
        agents = {
            7: {"queues": [{"id": queue_id, "name": queue_name}]},
            target_agent_id: {
                "id": target_agent_id,
                "users": [{"uuid": user_uuid}],
                "queues": target_queues,
            },
        }
        service.confd.users.get.side_effect = lambda uuid, tenant_uuid=None: users[uuid]
        service.confd.agents.get.side_effect = (
            lambda agent_id, tenant_uuid=None: agents[agent_id]
        )
        # First connect attempt fails (no agentd session), the post-login
        # "ensure selected queue" call then succeeds.
        service.agentd.agents.agent_login_to_queue.side_effect = [
            AgentdClientError(NOT_LOGGED),
            None,
        ]
        # The agent's WDA device is reachable so the pre-connect check passes.
        service.amid.action.return_value = [
            {"Response": "Success", "Status": WDA_CONNECTED}
        ]

    def test_connect_not_logged_performs_full_login(self, service):
        # An agent authenticated to WDA but with no agentd session must be
        # logged in (login_agent) on its own line, not rejected with 400.
        self._setup_fresh_login(service)

        service.connect_agent("support", 3, "sup-uuid", "t1")

        service.confd.users.get.assert_any_call("user-3", tenant_uuid="t1")
        service.agentd.agents.login_agent.assert_called_once_with(
            3, "1001", "default", tenant_uuid="t1"
        )

    def test_connect_not_logged_keeps_only_selected_queue(self, service):
        # A full login joins every configured queue; strict single-queue
        # connect prunes the others and ensures the selected one.
        self._setup_fresh_login(
            service,
            target_queues=[
                {"id": 42, "name": "support"},
                {"id": 99, "name": "sales"},
            ],
        )

        service.connect_agent("support", 3, "sup-uuid", "t1")

        service.agentd.agents.agent_logoff_from_queue.assert_called_once_with(
            3, 99, tenant_uuid="t1"
        )
        # The selected queue is (re)ensured after login.
        assert service.agentd.agents.agent_login_to_queue.call_args_list[-1].args == (
            3,
            42,
        )

    def test_connect_already_logged_does_not_relogin(self, service):
        # Additive per-queue connect for an already-logged-in agent must not
        # trigger a full login nor prune its other queues.
        self._authorize_supervisor_for(service)

        service.connect_agent("support", 3, "sup-uuid", "t1")

        service.agentd.agents.login_agent.assert_not_called()
        service.agentd.agents.agent_logoff_from_queue.assert_not_called()

    def test_connect_rejected_when_wda_not_connected(self, service):
        # Case 1 at connect time: the agent may still hold an agentd session,
        # but its WDA/device is Unavailable. Reject with a clear 409 so the
        # front can warn the supervisor, and never touch agentd.
        self._authorize_supervisor_for(service, wda_status=WDA_UNAVAILABLE)

        with pytest.raises(AgentWdaNotConnected) as exc:
            service.connect_agent("support", 3, "sup-uuid", "t1")

        assert exc.value.status_code == 409
        assert exc.value.id_ == "agent-wda-not-connected"
        service.agentd.agents.agent_login_to_queue.assert_not_called()
        service.agentd.agents.login_agent.assert_not_called()

    def test_connect_checks_wda_on_agent_extension(self, service):
        # The WDA check queries the device state of the agent's own line
        # (resolved exten/context), not the supervisor's.
        self._authorize_supervisor_for(service)

        service.connect_agent("support", 3, "sup-uuid", "t1")

        service.amid.action.assert_called_once_with(
            "ExtensionState", {"Exten": "1001", "Context": "default"}
        )

    def test_connect_not_logged_without_line_raises_400(self, service):
        self._setup_fresh_login(service, lines=[])

        with pytest.raises(AgentHasNoLine) as exc:
            service.connect_agent("support", 3, "sup-uuid", "t1")

        assert exc.value.status_code == 400
        service.agentd.agents.login_agent.assert_not_called()

    def test_no_such_queue_raises_404(self, service):
        self._authorize_supervisor_for(service)
        service.agentd.agents.agent_login_to_queue.side_effect = AgentdClientError(
            NO_SUCH_QUEUE
        )

        with pytest.raises(NoSuchAgentOrQueue) as exc:
            service.connect_agent("support", 3, "sup-uuid", "t1")

        assert exc.value.status_code == 404

    def test_no_such_agent_raises_404(self, service):
        self._authorize_supervisor_for(service)
        service.agentd.agents.agent_logoff_from_queue.side_effect = AgentdClientError(
            NO_SUCH_AGENT
        )

        with pytest.raises(NoSuchAgentOrQueue):
            service.disconnect_agent("support", 3, "sup-uuid", "t1")

    def test_connect_already_in_queue_is_idempotent(self, service):
        self._authorize_supervisor_for(service)
        service.agentd.agents.agent_login_to_queue.side_effect = AgentdClientError(
            ALREADY_IN_QUEUE
        )
        # No exception: the target state is already reached.
        service.connect_agent("support", 3, "sup-uuid", "t1")

    def test_disconnect_not_in_queue_is_idempotent(self, service):
        self._authorize_supervisor_for(service)
        service.agentd.agents.agent_logoff_from_queue.side_effect = AgentdClientError(
            NOT_IN_QUEUE
        )

        service.disconnect_agent("support", 3, "sup-uuid", "t1")

    def test_unknown_agentd_error_raises_502(self, service):
        self._authorize_supervisor_for(service)
        service.agentd.agents.agent_login_to_queue.side_effect = AgentdClientError(
            "invalid token or unauthorized"
        )

        with pytest.raises(AgentdUpstreamError) as exc:
            service.connect_agent("support", 3, "sup-uuid", "t1")

        assert exc.value.status_code == 502

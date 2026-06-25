# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from unittest.mock import Mock

import pytest

from wazo_agentd_client.error import (
    AgentdClientError,
    ALREADY_IN_QUEUE,
    NOT_IN_QUEUE,
    NOT_LOGGED,
    NO_SUCH_AGENT,
    NO_SUCH_QUEUE,
)

from wazo_calld_queue.exceptions import (
    AgentdUpstreamError,
    AgentNotLogged,
    NoSuchAgentOrQueue,
    SupervisorNotInQueue,
)
from wazo_calld_queue.services import QueueService


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

    def _authorize_supervisor_for(self, service, queue_name="support", queue_id=42):
        service.confd.users.get.return_value = {"agent": {"id": 7}}
        service.confd.agents.get.return_value = {
            "queues": [{"id": queue_id, "name": queue_name}]
        }

    def test_connect_authorized_delegates_to_agentd(self, service):
        self._authorize_supervisor_for(service)

        service.connect_agent("support", 3, "sup-uuid", "t1")

        service.confd.users.get.assert_called_once_with(
            "sup-uuid", tenant_uuid="t1"
        )
        service.confd.agents.get.assert_called_once_with(7, tenant_uuid="t1")
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

        service.confd.users.get.assert_called_once_with(
            "sup-uuid", tenant_uuid="tenant-b"
        )
        service.confd.agents.get.assert_called_once_with(7, tenant_uuid="tenant-b")
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

    def test_connect_agent_not_logged_raises_400(self, service):
        self._authorize_supervisor_for(service)
        service.agentd.agents.agent_login_to_queue.side_effect = AgentdClientError(
            NOT_LOGGED
        )

        with pytest.raises(AgentNotLogged) as exc:
            service.connect_agent("support", 3, "sup-uuid", "t1")

        assert exc.value.status_code == 400

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

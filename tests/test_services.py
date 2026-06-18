# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from unittest.mock import Mock

import pytest

from wazo_calld_queue import bus_consume
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
    def test_livestats_returns_queue_stats(self, service):
        result = service.livestats("support")

        assert result is bus_consume.stats["support"]
        assert result["count"] == 0

    def test_agents_status_uses_service_clients(self, service):
        service.confd.agents.list.return_value = {
            "items": [
                {
                    "id": 1,
                    "firstname": "John",
                    "lastname": "Doe",
                    "number": "1001",
                    "queues": [{"name": "support"}],
                }
            ]
        }
        service.agentd.agents.get_agent_statuses.return_value = []

        result = service.agents_status("tenant-1")

        assert result[1]["fullname"] == "John Doe"
        assert result is bus_consume.agents["tenant-1"]

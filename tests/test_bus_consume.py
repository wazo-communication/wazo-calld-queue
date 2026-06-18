# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from wazo_calld_queue import bus_consume
from wazo_calld_queue.events import (
    QueueAgentsStatusEvent,
    QueueCallerJoinEvent,
    QueueLiveStatsEvent,
    QueueMemberAddedEvent,
)

TENANT = "tenant-1"
ZERO_UUID = "00000000-0000-0000-0000-000000000000"


def _join_event(uniqueid, queue="support", count="1", tenant=TENANT):
    return {
        "Event": "QueueCallerJoin",
        "Queue": queue,
        "Context": "queue",
        "Count": count,
        "Uniqueid": uniqueid,
        "CallerIDNum": "1000",
        "CallerIDName": "Alice",
        "Position": "1",
        "ChannelState": "6",
        "ChannelStateDesc": "Up",
        "ChanVariable": {
            "WAZO_TENANT_UUID": tenant,
            "WAZO_ANSWER_TIME": "0",
            "WAZO_ENTRY_EXTEN": "4000",
        },
    }


def _published_events(handler):
    return [call.args[0] for call in handler.bus_publisher.publish.call_args_list]


@pytest.fixture
def frozen_now(monkeypatch):
    """Freeze ``bus_consume.datetime.now()`` so day-based assertions are stable.

    ``get_stats`` keys its daily reset on ``datetime.datetime.now().day``;
    without freezing, a test running across midnight could read two different
    days between the code under test and the assertion.
    """
    fixed = datetime.datetime(2026, 6, 17, 12, 0, 0)

    class _FrozenDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(
        bus_consume, "datetime", SimpleNamespace(datetime=_FrozenDateTime)
    )
    return fixed


class TestGetStats:
    def test_creates_default_entry(self, handler, frozen_now):
        result = handler.get_stats("support")

        assert result["count"] == 0
        assert result["count_color"] == "green"
        assert result["received"] == 0
        assert result["abandonned"] == 0
        assert result["answered"] == 0
        assert result["awr"] == 0
        assert result["waiting_calls"] == []
        assert result["updated_at"] == frozen_now.day
        assert bus_consume.stats["support"] is result

    def test_returns_existing_entry_same_day(self, handler):
        first = handler.get_stats("support")
        first["count"] = 5

        second = handler.get_stats("support")

        assert second is first
        assert second["count"] == 5

    def test_resets_entry_on_a_new_day(self, handler, frozen_now):
        stats = handler.get_stats("support")
        stats["count"] = 42
        stats["received"] = 7
        stats["updated_at"] = -1  # force a "different day"

        result = handler.get_stats("support")

        assert result["count"] == 0
        assert result["received"] == 0
        assert result["updated_at"] == frozen_now.day


class TestLiveStats:
    def test_caller_join_tracks_waiting_call(self, handler):
        handler._livestats(_join_event("111", count="1"), TENANT)

        stats = bus_consume.stats["support"]
        assert stats["count"] == 1
        assert stats["count_color"] == "green"
        assert len(stats["waiting_calls"]) == 1
        assert stats["waiting_calls"][0]["uniqueid"] == "111"
        assert stats["waiting_calls"][0]["entryexten"] == "4000"

    def test_caller_join_count_color_turns_red_above_one(self, handler):
        handler._livestats(_join_event("111", count="2"), TENANT)

        assert bus_consume.stats["support"]["count_color"] == "red"

    def test_caller_leave_updates_counters(self, handler):
        handler._livestats(_join_event("111", count="1"), TENANT)
        leave = {
            "Event": "QueueCallerLeave",
            "Queue": "support",
            "Count": "0",
            "Uniqueid": "111",
        }

        handler._livestats(leave, TENANT)

        stats = bus_consume.stats["support"]
        assert stats["answered"] == 1
        assert stats["received"] == 1
        assert stats["awr"] == 100
        assert stats["waiting_calls"] == []

    def test_caller_abandon_updates_counters(self, handler):
        handler._livestats(_join_event("111", count="1"), TENANT)

        abandon = {
            "Event": "QueueCallerAbandon",
            "Queue": "support",
            "Uniqueid": "111",
        }
        handler._livestats(abandon, TENANT)

        stats = bus_consume.stats["support"]
        assert stats["abandonned"] == 1
        assert stats["waiting_calls"] == []

    def test_removing_a_non_last_waiting_call_does_not_raise(self, handler):
        # Regression: the old range(len(...)) + pop loop raised IndexError
        # when the matched call was not the last one in the list.
        for uid in ("A", "B", "C"):
            handler._livestats(_join_event(uid, count="3"), TENANT)

        abandon_b = {
            "Event": "QueueCallerAbandon",
            "Queue": "support",
            "Uniqueid": "B",
        }
        handler._livestats(abandon_b, TENANT)

        remaining = [c["uniqueid"] for c in bus_consume.stats["support"]["waiting_calls"]]
        assert remaining == ["A", "C"]

    def test_publishes_livestats_event(self, handler):
        handler._livestats(_join_event("111"), TENANT)

        published = _published_events(handler)
        assert len(published) == 1
        assert isinstance(published[0], QueueLiveStatsEvent)
        assert published[0].tenant_uuid == TENANT


class TestExtractTenantUuid:
    def test_from_chan_variable(self, handler):
        event = {"ChanVariable": {"WAZO_TENANT_UUID": "tenant-xyz"}}

        assert handler._extract_tenant_uuid(event) == "tenant-xyz"

    def test_fallback_to_confd_via_interface(self, handler):
        handler.confd.agents.get.return_value = {"tenant_uuid": "tenant-from-confd"}
        event = {"Interface": "Local/id-7@agentcallback"}

        assert handler._extract_tenant_uuid(event) == "tenant-from-confd"
        handler.confd.agents.get.assert_called_once_with(7)

    def test_unmatched_interface_raises_value_error(self, handler):
        event = {"Interface": "SIP/not-an-agent"}

        with pytest.raises(ValueError):
            handler._extract_tenant_uuid(event)


class TestGetAgentsStatus:
    def test_builds_agents_dict(self, handler):
        handler.confd.agents.list.return_value = {
            "items": [
                {
                    "id": 1,
                    "firstname": "John",
                    "lastname": "Doe",
                    "number": "1001",
                    "queues": [{"name": "support"}],
                },
                {
                    "id": 2,
                    "firstname": "Jane",
                    "lastname": None,
                    "number": "1002",
                    "queues": [],
                },
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            SimpleNamespace(id=1, logged=True, paused=False),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[1]["fullname"] == "John Doe"
        assert result[1]["queue"] == "support"
        assert result[1]["is_logged"] is True
        assert result[1]["is_paused"] is False
        # lastname None is skipped; empty queues -> False; no status -> defaults
        assert result[2]["fullname"] == "Jane"
        assert result[2]["queue"] is False
        assert result[2]["is_logged"] is False

    def test_result_is_cached(self, handler):
        handler.confd.agents.list.return_value = {
            "items": [
                {
                    "id": 1,
                    "firstname": "A",
                    "lastname": "B",
                    "number": "1001",
                    "queues": [{"name": "support"}],
                }
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = []

        handler.get_agents_status(TENANT)
        handler.get_agents_status(TENANT)

        handler.confd.agents.list.assert_called_once()


class TestAddAgent:
    def test_adds_missing_agent_from_confd(self, handler):
        bus_consume.agents[TENANT] = {}
        handler.confd.agents.get.return_value = {
            "firstname": "John",
            "lastname": "Doe",
            "queues": [{"name": "support"}],
        }

        handler.add_agent(TENANT, 5, "1001")

        agent = bus_consume.agents[TENANT][5]
        assert agent["id"] == 5
        assert agent["number"] == "1001"
        assert agent["fullname"] == "John Doe"
        assert agent["queue"] == "support"
        assert agent["is_logged"] is False

    def test_does_not_overwrite_existing_agent(self, handler):
        bus_consume.agents[TENANT] = {5: {"id": 5, "fullname": "Existing"}}

        handler.add_agent(TENANT, 5, "1001")

        assert bus_consume.agents[TENANT][5]["fullname"] == "Existing"
        handler.confd.agents.get.assert_not_called()


class TestSubscribe:
    def test_registers_all_expected_events(self, handler):
        consumer = Mock()

        handler.subscribe(consumer)

        registered = {call.args[0] for call in consumer.subscribe.call_args_list}
        assert registered == {
            "QueueCallerAbandon",
            "QueueCallerJoin",
            "QueueCallerLeave",
            "QueueMemberAdded",
            "QueueMemberPause",
            "QueueMemberPenalty",
            "QueueMemberRemoved",
            "QueueMemberRinginuse",
            "QueueMemberStatus",
        }


class TestMemberEventHandlers:
    def test_usersharedlines_interface_uses_zero_uuid_and_skips_agents(self, handler):
        handler._agents_status = Mock()
        event = {
            "Event": "QueueMemberAdded",
            "Interface": "Local/id-1@usersharedlines",
            "Membership": "dynamic",
        }

        handler._queue_member_added(event)

        handler._agents_status.assert_not_called()
        published = _published_events(handler)
        assert len(published) == 1
        assert isinstance(published[0], QueueMemberAddedEvent)
        assert published[0].tenant_uuid == ZERO_UUID

    def test_member_pause_sets_paused_and_publishes_status(self, handler):
        bus_consume.agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "queue": "support",
                "is_paused": False,
                "paused_at": "",
            }
        }
        event = {
            "Event": "QueueMemberPause",
            "Membership": "dynamic",
            "Interface": "Local/id-5@agentcallback",
            "MemberName": "Agent/1001",
            "Queue": "support",
            "Paused": "1",
            "ChanVariable": {"WAZO_TENANT_UUID": TENANT},
        }

        handler._queue_member_pause(event)

        agent = bus_consume.agents[TENANT][5]
        assert agent["is_paused"] is True
        assert agent["paused_at"] != ""

        published = _published_events(handler)
        assert isinstance(published[0], QueueAgentsStatusEvent)
        assert published[0].content == agent

    def test_member_status_talking_updates_agent(self, handler):
        bus_consume.agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "queue": "support",
                "is_talking": False,
                "is_ringing": True,
                "talked_at": "",
            }
        }
        event = {
            "Event": "QueueMemberStatus",
            "Membership": "dynamic",
            "Interface": "Local/id-5@agentcallback",
            "MemberName": "Agent/1001",
            "Queue": "support",
            "Status": "2",
            "ChanVariable": {"WAZO_TENANT_UUID": TENANT},
        }

        handler._queue_member_status(event)

        agent = bus_consume.agents[TENANT][5]
        assert agent["is_talking"] is True
        assert agent["is_ringing"] is False
        assert agent["talked_at"] != ""


class TestCallerEventHandlers:
    def test_caller_join_publishes_livestats_and_caller_event(self, handler):
        handler._queue_caller_join(_join_event("111"))

        published = _published_events(handler)
        # _livestats publishes a QueueLiveStatsEvent, then the join event itself
        assert isinstance(published[0], QueueLiveStatsEvent)
        assert isinstance(published[-1], QueueCallerJoinEvent)
        assert published[-1].tenant_uuid == TENANT

    def test_caller_join_outside_queue_context_skips_livestats(self, handler):
        event = _join_event("111")
        event["Context"] = "group"

        handler._queue_caller_join(event)

        assert "support" not in bus_consume.stats
        published = _published_events(handler)
        assert len(published) == 1
        assert isinstance(published[0], QueueCallerJoinEvent)

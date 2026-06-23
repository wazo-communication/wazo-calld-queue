# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import datetime
import logging
import threading
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


def _member_added_event(queue, agent_id=5, member="1001", tenant=TENANT):
    return {
        "Event": "QueueMemberAdded",
        "Membership": "dynamic",
        "Interface": f"Local/id-{agent_id}@agentcallback",
        "MemberName": f"Agent/{member}",
        "StateInterface": f"Local/id-{agent_id}@agentcallback",
        "Queue": queue,
        "ChanVariable": {"WAZO_TENANT_UUID": tenant},
    }


def _member_removed_event(queue, agent_id=5, member="1001", tenant=TENANT):
    return {
        "Event": "QueueMemberRemoved",
        "Membership": "dynamic",
        "Interface": f"Local/id-{agent_id}@agentcallback",
        "MemberName": f"Agent/{member}",
        "Queue": queue,
        "ChanVariable": {"WAZO_TENANT_UUID": tenant},
    }


def _leave_event(uniqueid, queue="support", count="0", tenant=TENANT):
    return {
        "Event": "QueueCallerLeave",
        "Queue": queue,
        "Context": "queue",
        "Count": count,
        "Uniqueid": uniqueid,
    }


def _member_pause_event(queue, paused, agent_id=5, member="1001", tenant=TENANT):
    return {
        "Event": "QueueMemberPause",
        "Membership": "dynamic",
        "Interface": f"Local/id-{agent_id}@agentcallback",
        "MemberName": f"Agent/{member}",
        "Queue": queue,
        "Paused": paused,
        "ChanVariable": {"WAZO_TENANT_UUID": tenant},
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
        assert handler._stats["support"] is result

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

        stats = handler._stats["support"]
        assert stats["count"] == 1
        assert stats["count_color"] == "green"
        assert len(stats["waiting_calls"]) == 1
        assert stats["waiting_calls"][0]["uniqueid"] == "111"
        assert stats["waiting_calls"][0]["entryexten"] == "4000"

    def test_caller_join_count_color_turns_red_above_one(self, handler):
        handler._livestats(_join_event("111", count="2"), TENANT)

        assert handler._stats["support"]["count_color"] == "red"

    def test_caller_leave_updates_counters(self, handler):
        handler._livestats(_join_event("111", count="1"), TENANT)
        leave = {
            "Event": "QueueCallerLeave",
            "Queue": "support",
            "Count": "0",
            "Uniqueid": "111",
        }

        handler._livestats(leave, TENANT)

        stats = handler._stats["support"]
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

        stats = handler._stats["support"]
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

        remaining = [c["uniqueid"] for c in handler._stats["support"]["waiting_calls"]]
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


def _agentd_status(agent_id, queues, logged=None, paused=None):
    """Build an agentd ``_AgentStatus``-shaped object.

    ``queues`` is a list of ``(name, logged, paused)`` tuples mirroring the
    real agentd payload, where every configured queue is reported with its
    current per-queue ``logged`` / ``paused`` flags. The top-level ``logged`` /
    ``paused`` default to the OR of the per-queue flags, as agentd reports them.
    """
    queue_dicts = [
        {"name": name, "logged": q_logged, "paused": q_paused}
        for (name, q_logged, q_paused) in queues
    ]
    return SimpleNamespace(
        id=agent_id,
        logged=any(q[1] for q in queues) if logged is None else logged,
        paused=any(q[2] for q in queues) if paused is None else paused,
        queues=queue_dicts,
    )


class TestGetAgentsStatus:
    def test_builds_agents_dict(self, handler):
        handler.confd.agents.list.return_value = {
            "items": [
                {
                    "id": 1,
                    "firstname": "John",
                    "lastname": "Doe",
                    "number": "1001",
                },
                {
                    "id": 2,
                    "firstname": "Jane",
                    "lastname": None,
                    "number": "1002",
                },
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            _agentd_status(1, [("support", True, False)]),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[1]["fullname"] == "John Doe"
        assert result[1]["queue"] == "support"
        assert result[1]["is_logged"] is True
        assert result[1]["is_paused"] is False
        # lastname None is skipped; no agentd status -> logged-out defaults
        assert result[2]["fullname"] == "Jane"
        assert result[2]["queue"] is False
        assert result[2]["is_logged"] is False

    def test_seeds_only_queues_the_agent_is_logged_into(self, handler):
        # Configured for support+sales but only logged into support: runtime
        # membership must reflect agentd's per-queue flags, not every
        # configured queue.
        handler.confd.agents.list.return_value = {
            "items": [
                {"id": 1, "firstname": "John", "lastname": "Doe", "number": "1001"}
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            _agentd_status(1, [("support", True, False), ("sales", False, False)]),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[1]["queues"] == ["support"]
        assert result[1]["queue"] == "support"
        assert result[1]["is_logged"] is True

    def test_queues_empty_when_logged_out(self, handler):
        handler.confd.agents.list.return_value = {
            "items": [
                {"id": 1, "firstname": "John", "lastname": "Doe", "number": "1001"}
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            _agentd_status(1, [("support", False, False), ("sales", False, False)]),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[1]["queues"] == []
        # Legacy ``queue`` stays a queue-name string for back-compat; use
        # ``is_logged`` / ``queues`` for connection state.
        assert result[1]["queue"] == "support"
        assert result[1]["is_logged"] is False
        assert result[1]["paused_queues"] == []

    def test_seeds_paused_queues_from_per_queue_flags(self, handler):
        # Logged into support+sales, paused only in sales.
        handler.confd.agents.list.return_value = {
            "items": [
                {"id": 1, "firstname": "John", "lastname": "Doe", "number": "1001"}
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            _agentd_status(1, [("support", True, False), ("sales", True, True)]),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[1]["queues"] == ["support", "sales"]
        assert result[1]["paused_queues"] == ["sales"]
        assert result[1]["is_paused"] is True

    def test_result_is_cached(self, handler):
        handler.confd.agents.list.return_value = {
            "items": [
                {"id": 1, "firstname": "A", "lastname": "B", "number": "1001"}
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = []

        handler.get_agents_status(TENANT)
        handler.get_agents_status(TENANT)

        handler.confd.agents.list.assert_called_once()

    def test_configured_queues_exposes_logged_off_membership(self, handler):
        # A multi-queue agent configured in support+test but logged off (no
        # agentd status): runtime ``queues`` stays empty, but the full confd
        # roster must surface in ``configured_queues`` so a client can list the
        # agent as a (disconnected) member of BOTH queues. This is the issue #13
        # gap: the legacy ``queue`` carries only the first queue, hiding ``test``.
        handler.confd.agents.list.return_value = {
            "items": [
                {
                    "id": 3,
                    "firstname": "Mathias",
                    "lastname": "Wolff",
                    "number": "8002",
                    "queues": [{"name": "support"}, {"name": "test"}],
                }
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = []

        result = handler.get_agents_status(TENANT)

        assert result[3]["configured_queues"] == ["support", "test"]
        assert result[3]["queues"] == []
        assert result[3]["is_logged"] is False

    def test_configured_queues_is_superset_of_runtime_membership(self, handler):
        # Agent 3 logged into support only, but configured for support+test:
        # ``queues`` reflects runtime (support), ``configured_queues`` the full
        # roster (support+test) so ``test`` shows the agent as a disconnected
        # member there.
        handler.confd.agents.list.return_value = {
            "items": [
                {"id": 3, "firstname": "Mathias", "lastname": "Wolff", "number": "8002"}
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            _agentd_status(3, [("support", True, False), ("test", False, False)]),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[3]["queues"] == ["support"]
        assert result[3]["configured_queues"] == ["support", "test"]
        assert result[3]["is_logged"] is True


class TestAddAgent:
    def test_adds_missing_agent_from_confd(self, handler):
        handler._agents[TENANT] = {}
        handler.confd.agents.get.return_value = {
            "firstname": "John",
            "lastname": "Doe",
            "queues": [{"name": "support"}],
        }

        handler.add_agent(TENANT, 5, "1001")

        agent = handler._agents[TENANT][5]
        assert agent["id"] == 5
        assert agent["number"] == "1001"
        assert agent["fullname"] == "John Doe"
        # add_agent starts not-yet-logged (empty runtime ``queues``) but seeds
        # the legacy ``queue`` string from the configured queues for back-compat.
        assert agent["queue"] == "support"
        assert agent["queues"] == []
        assert agent["is_logged"] is False

    def test_initializes_multi_queue(self, handler):
        handler._agents[TENANT] = {}
        handler.confd.agents.get.return_value = {
            "firstname": "John",
            "lastname": "Doe",
            "queues": [{"name": "support"}, {"name": "sales"}],
        }

        handler.add_agent(TENANT, 5, "1001")

        agent = handler._agents[TENANT][5]
        # Not yet runtime-logged, so derived membership starts empty; the legacy
        # ``queue`` string is seeded from the first configured queue.
        assert agent["queues"] == []
        assert agent["queue"] == "support"
        assert agent["is_logged"] is False
        assert agent["paused_queues"] == []

    def test_does_not_overwrite_existing_agent(self, handler):
        handler._agents[TENANT] = {5: {"id": 5, "fullname": "Existing"}}

        handler.add_agent(TENANT, 5, "1001")

        assert handler._agents[TENANT][5]["fullname"] == "Existing"
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
        handler._agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "queue": "support",
                "queues": ["support"],
                "paused_queues": [],
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

        agent = handler._agents[TENANT][5]
        assert agent["is_paused"] is True
        assert agent["paused_at"] != ""

        published = _published_events(handler)
        assert isinstance(published[0], QueueAgentsStatusEvent)
        assert published[0].content == agent

    def test_member_status_talking_updates_agent(self, handler):
        handler._agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "queue": "support",
                "queues": ["support"],
                "paused_queues": [],
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

        agent = handler._agents[TENANT][5]
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

        assert "support" not in handler._stats
        published = _published_events(handler)
        assert len(published) == 1
        assert isinstance(published[0], QueueCallerJoinEvent)


class TestMultiQueueMembership:
    def _logged_agent(self, handler, queues, paused_queues=None):
        """Seed an agent already logged into ``queues`` (runtime membership)."""
        handler._agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "fullname": "John Doe",
                "queue": queues[0] if queues else False,
                "queues": list(queues),
                "paused_queues": list(paused_queues or []),
                "is_logged": bool(queues),
                "is_paused": bool(paused_queues),
                "is_offline": False,
                "is_talking": False,
                "is_ringing": False,
                "logged_at": "2026-06-17T12:00:00.000000",
                "paused_at": "",
                "talked_at": "",
                "talked_with_number": "",
                "talked_with_name": "",
            }
        }
        return handler._agents[TENANT][5]

    def test_member_added_to_second_queue_keeps_both(self, handler):
        self._logged_agent(handler, ["support"])

        handler._queue_member_added(_member_added_event("sales"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == ["support", "sales"]
        assert agent["is_logged"] is True

    def test_member_added_keeps_configured_roster_a_superset(self, handler):
        # An agent can only log into a queue it is configured for, so a runtime
        # join must surface in ``configured_queues`` too (issue #13 invariant
        # ``queues ⊆ configured_queues``): the live event corrects a roster the
        # bootstrap missed (e.g. queue configured mid-session).
        agent = self._logged_agent(handler, ["support"])
        agent["configured_queues"] = ["support"]

        handler._queue_member_added(_member_added_event("sales"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == ["support", "sales"]
        assert agent["configured_queues"] == ["support", "sales"]

    def test_member_added_seeds_configured_queues_when_absent(self, handler):
        # Defensive: a state created before ``configured_queues`` existed (e.g.
        # a rolling deploy or an older bootstrap) must not KeyError on a live
        # join — the field is seeded lazily. It must seed from the EXISTING
        # runtime ``queues`` (not empty), so the ``queues ⊆ configured_queues``
        # invariant holds: a queue the agent is already logged into must not
        # vanish from the roster just because the new queue triggered seeding.
        self._logged_agent(handler, ["support"])  # no configured_queues key

        handler._queue_member_added(_member_added_event("sales"))

        configured = handler._agents[TENANT][5]["configured_queues"]
        assert "support" in configured  # pre-existing runtime queue preserved
        assert "sales" in configured

    def test_member_removed_from_one_queue_stays_logged(self, handler):
        self._logged_agent(handler, ["support", "sales"])

        handler._queue_member_removed(_member_removed_event("sales"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == ["support"]
        assert agent["is_logged"] is True
        # Session is preserved while still member of another queue.
        assert agent["logged_at"] == "2026-06-17T12:00:00.000000"

    def test_member_removed_from_last_queue_logs_out(self, handler):
        agent = self._logged_agent(handler, ["support"])
        agent["is_talking"] = True
        agent["talked_at"] = "2026-06-17T12:30:00.000000"
        agent["talked_with_number"] = "2000"

        handler._queue_member_removed(_member_removed_event("support"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == []
        assert agent["is_logged"] is False
        assert agent["is_talking"] is False
        assert agent["is_ringing"] is False
        assert agent["logged_at"] == ""
        assert agent["talked_at"] == ""
        assert agent["talked_with_number"] == ""

    def test_queue_field_is_first_of_queues(self, handler):
        self._logged_agent(handler, ["support"])

        handler._queue_member_added(_member_added_event("sales"))
        assert handler._agents[TENANT][5]["queue"] == "support"

        handler._queue_member_removed(_member_removed_event("support"))
        assert handler._agents[TENANT][5]["queue"] == "sales"

        # Fully logged out: ``queue`` keeps the last known name (back-compat,
        # never reset to False); ``is_logged`` / ``queues`` convey the logout.
        handler._queue_member_removed(_member_removed_event("sales"))
        assert handler._agents[TENANT][5]["queue"] == "sales"
        assert handler._agents[TENANT][5]["is_logged"] is False
        assert handler._agents[TENANT][5]["queues"] == []

    def test_pause_in_one_queue_among_two_sets_paused(self, handler):
        self._logged_agent(handler, ["support", "sales"])

        handler._queue_member_pause(_member_pause_event("sales", paused="1"))

        agent = handler._agents[TENANT][5]
        assert agent["paused_queues"] == ["sales"]
        assert agent["is_paused"] is True
        assert agent["paused_at"] != ""

    def test_unpause_one_of_two_paused_queues_stays_paused(self, handler):
        agent = self._logged_agent(handler, ["support", "sales"])
        agent["paused_queues"] = ["support", "sales"]
        agent["is_paused"] = True
        agent["paused_at"] = "2026-06-17T12:15:00.000000"

        handler._queue_member_pause(_member_pause_event("support", paused="0"))

        agent = handler._agents[TENANT][5]
        assert agent["paused_queues"] == ["sales"]
        assert agent["is_paused"] is True
        assert agent["paused_at"] == "2026-06-17T12:15:00.000000"

    def test_unpause_last_queue_clears_paused(self, handler):
        agent = self._logged_agent(handler, ["support"])
        agent["paused_queues"] = ["support"]
        agent["is_paused"] = True
        agent["paused_at"] = "2026-06-17T12:15:00.000000"

        handler._queue_member_pause(_member_pause_event("support", paused="0"))

        agent = handler._agents[TENANT][5]
        assert agent["paused_queues"] == []
        assert agent["is_paused"] is False
        assert agent["paused_at"] == ""

    def test_duplicate_added_event_is_idempotent(self, handler):
        self._logged_agent(handler, ["support"])

        handler._queue_member_added(_member_added_event("support"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == ["support"]

    def test_duplicate_pause_event_is_idempotent(self, handler):
        agent = self._logged_agent(handler, ["support"])
        agent["paused_queues"] = ["support"]
        agent["is_paused"] = True
        agent["paused_at"] = "2026-06-17T12:15:00.000000"

        handler._queue_member_pause(_member_pause_event("support", paused="1"))

        agent = handler._agents[TENANT][5]
        assert agent["paused_queues"] == ["support"]
        # Already paused: the first-pause timestamp must be preserved.
        assert agent["paused_at"] == "2026-06-17T12:15:00.000000"

    def test_pause_in_non_member_queue_is_ignored(self, handler):
        # Agent is a member of "support" only; a pause for "sales" is dropped to
        # keep the invariant paused_queues ⊆ queues.
        self._logged_agent(handler, ["support"])

        handler._queue_member_pause(_member_pause_event("sales", paused="1"))

        agent = handler._agents[TENANT][5]
        assert agent["paused_queues"] == []
        assert agent["is_paused"] is False

    def test_member_removed_for_untracked_queue_warns_and_is_noop(
        self, handler, caplog
    ):
        # A removal referencing a queue we are not tracking is a no-op, but it
        # may signal drift between the agentd bootstrap names and the live
        # event names, so it must be logged loudly rather than silently ignored.
        self._logged_agent(handler, ["support"])

        with caplog.at_level(logging.WARNING, logger="wazo_calld_queue.bus_consume"):
            handler._queue_member_removed(_member_removed_event("sales"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == ["support"]
        assert agent["is_logged"] is True
        assert "not in tracked membership" in caplog.text

    def test_pause_after_removed_does_not_resurrect_membership(self, handler):
        # Removed then a stray Pause for the same queue: the agent is logged out
        # and must not be reported as paused.
        self._logged_agent(handler, ["support"])

        handler._queue_member_removed(_member_removed_event("support"))
        handler._queue_member_pause(_member_pause_event("support", paused="1"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == []
        assert agent["paused_queues"] == []
        assert agent["is_logged"] is False
        assert agent["is_paused"] is False


class TestBootstrapTimestamps:
    """A REST/restart bootstrap seeds membership but no session timestamps.

    ``get_agents_status`` materialises an already-logged-in (or already-paused)
    agent with non-empty ``queues`` / ``paused_queues`` but empty ``logged_at``
    / ``paused_at`` (the real login/pause time is unknown). The first live
    membership event that arrives afterwards must backfill the missing
    timestamp instead of treating the agent as "already logged/paused" and
    leaving the field empty forever.
    """

    def _bootstrapped_agent(self, handler, queues, paused_queues=None):
        handler._agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "fullname": "John Doe",
                "queue": queues[0] if queues else False,
                "queues": list(queues),
                "paused_queues": list(paused_queues or []),
                "is_logged": bool(queues),
                "is_paused": bool(paused_queues),
                "is_offline": False,
                "is_talking": False,
                "is_ringing": False,
                # Seeded without timestamps: the real times are unknown.
                "logged_at": "",
                "paused_at": "",
                "talked_at": "",
                "talked_with_number": "",
                "talked_with_name": "",
            }
        }
        return handler._agents[TENANT][5]

    def test_member_added_backfills_logged_at_after_bootstrap(self, handler, frozen_now):
        self._bootstrapped_agent(handler, ["support"])

        handler._queue_member_added(_member_added_event("support"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == ["support"]
        assert agent["logged_at"] == frozen_now.strftime("%Y-%m-%dT%H:%M:%S.%f")

    def test_member_pause_backfills_paused_at_after_bootstrap(self, handler, frozen_now):
        self._bootstrapped_agent(handler, ["support"], paused_queues=["support"])

        handler._queue_member_pause(_member_pause_event("support", paused="1"))

        agent = handler._agents[TENANT][5]
        assert agent["paused_queues"] == ["support"]
        assert agent["paused_at"] == frozen_now.strftime("%Y-%m-%dT%H:%M:%S.%f")


class TestLegacyQueueFieldBackCompat:
    """The legacy ``queue`` field must stay a queue-name string even when the
    agent is logged out.

    The pre-multi-queue front (v2.0.2) groups agents by ``agent.queue`` and
    expects a string; a boolean ``false`` breaks it. Connection state is carried
    by ``is_logged`` (and the new ``queues``), not by resetting ``queue``.
    """

    def test_logged_out_agent_keeps_a_queue_name_at_bootstrap(self, handler):
        handler.confd.agents.list.return_value = {
            "items": [
                {"id": 1, "firstname": "John", "lastname": "Doe", "number": "1001"}
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = [
            _agentd_status(1, [("support", False, False), ("sales", False, False)]),
        ]

        result = handler.get_agents_status(TENANT)

        assert result[1]["queue"] == "support"  # truthy, back-compat
        assert result[1]["is_logged"] is False
        assert result[1]["queues"] == []

    def test_home_queue_falls_back_to_confd_when_agentd_has_no_status(self, handler):
        # agentd returns no status for the agent (e.g. never logged in since
        # agentd started): the home queue must still come from confd, so a
        # configured agent keeps a queue-name string rather than ``false``.
        handler.confd.agents.list.return_value = {
            "items": [
                {
                    "id": 1,
                    "firstname": "John",
                    "lastname": "Doe",
                    "number": "1001",
                    "queues": [{"name": "support"}, {"name": "sales"}],
                }
            ]
        }
        handler.agentd.agents.get_agent_statuses.return_value = []

        result = handler.get_agents_status(TENANT)

        assert result[1]["queue"] == "support"
        assert result[1]["queues"] == []
        assert result[1]["is_logged"] is False

    def test_queue_not_reset_to_false_on_full_logout(self, handler):
        handler._agents[TENANT] = {
            5: bus_consume._build_agent_state(
                5, "1001", "John Doe", ["support"], [], home_queue="support"
            )
        }

        handler._queue_member_removed(_member_removed_event("support"))

        agent = handler._agents[TENANT][5]
        assert agent["queues"] == []
        assert agent["is_logged"] is False
        assert agent["queue"] == "support"  # NOT False


class TestBuildAgentState:
    def test_seeds_runtime_and_paused_queues(self):
        state = bus_consume._build_agent_state(
            1, "1001", "John Doe", ["support", "sales"], ["sales"]
        )
        assert state["queues"] == ["support", "sales"]
        assert state["queue"] == "support"
        assert state["is_logged"] is True
        assert state["paused_queues"] == ["sales"]
        assert state["is_paused"] is True

    def test_defaults_to_empty_membership(self):
        state = bus_consume._build_agent_state(1, "1001", "John Doe")
        assert state["queues"] == []
        assert state["queue"] is False
        assert state["is_logged"] is False
        assert state["paused_queues"] == []
        assert state["is_paused"] is False

    def test_enforces_paused_subset_of_queues(self):
        # A pause in a queue the agent is not (runtime) a member of must not
        # produce a phantom paused flag: paused_queues ⊆ queues.
        state = bus_consume._build_agent_state(
            1, "1001", "John Doe", ["support"], ["support", "sales"]
        )
        assert state["queues"] == ["support"]
        assert state["paused_queues"] == ["support"]
        assert state["is_paused"] is True

    def test_paused_without_membership_stays_consistent(self):
        state = bus_consume._build_agent_state(
            1, "1001", "John Doe", [], ["support"]
        )
        assert state["queues"] == []
        assert state["paused_queues"] == []
        assert state["is_paused"] is False


class TestMalformedMemberEvents:
    """A membership event missing a required field is dropped, not crashed on."""

    def _seed_agent(self, handler, queues):
        handler._agents[TENANT] = {
            5: {
                "id": 5,
                "number": "1001",
                "fullname": "John Doe",
                "queue": queues[0] if queues else False,
                "queues": list(queues),
                "paused_queues": [],
                "is_logged": bool(queues),
                "is_paused": False,
            }
        }

    @pytest.mark.parametrize("event_type", ["QueueMemberAdded", "QueueMemberRemoved"])
    def test_member_event_without_queue_is_dropped(self, handler, event_type):
        self._seed_agent(handler, ["support"])
        event = {
            "Event": event_type,
            "Membership": "dynamic",
            "Interface": "Local/id-5@agentcallback",
            "MemberName": "Agent/1001",
            # "Queue" intentionally missing
            "StateInterface": "Local/id-5@agentcallback",
        }

        # Must not raise (no KeyError) and must leave existing state untouched.
        handler._agents_status(event, TENANT)

        assert handler._agents[TENANT][5]["queues"] == ["support"]
        handler.bus_publisher.publish.assert_not_called()

    def test_pause_event_without_paused_is_dropped(self, handler):
        self._seed_agent(handler, ["support"])
        event = {
            "Event": "QueueMemberPause",
            "Membership": "dynamic",
            "Interface": "Local/id-5@agentcallback",
            "MemberName": "Agent/1001",
            "Queue": "support",
            # "Paused" intentionally missing
        }

        handler._agents_status(event, TENANT)

        assert handler._agents[TENANT][5]["paused_queues"] == []
        handler.bus_publisher.publish.assert_not_called()


class TestThreadSafety:
    """The bus consumer thread and the REST worker threads share the same
    in-memory state, so concurrent access must be serialised by ``_lock``.

    Without the lock, the ``QueueCallerLeave`` loop iterating the tenant's agent
    dict while another thread inserts a new agent raises
    ``RuntimeError: dictionary changed size during iteration`` (and can corrupt
    the counters). This test hammers both paths concurrently and asserts neither
    thread raises.
    """

    def _caller_leave_event(self):
        # ConnectedLineNum is a real agent number so the full dict is iterated
        # (no early break), maximising the window for a concurrent insert.
        return {
            "Event": "QueueCallerLeave",
            "ConnectedLineNum": "9999",  # never matches -> iterate everything
            "CallerIDNum": "2000",
            "CallerIDName": "Bob",
        }

    def _member_added_event(self, agent_id):
        return {
            "Event": "QueueMemberAdded",
            "Membership": "dynamic",
            "Interface": f"Local/id-{agent_id}@agentcallback",
            "MemberName": f"Agent/{1000 + agent_id}",
            "Queue": "support",
            "StateInterface": f"Local/id-{agent_id}@agentcallback",
        }

    def test_concurrent_read_and_insert_do_not_corrupt_state(self, handler):
        handler.confd.agents.get.return_value = {
            "firstname": "New",
            "lastname": "Agent",
            "queues": [],
        }
        # Seed the tenant so the reader has a dict to iterate from the start.
        handler._agents[TENANT] = {
            5: _build_seed_agent(5, "1001", ["support"]),
        }

        iterations = 300
        errors = []
        start = threading.Barrier(2)

        def reader():
            start.wait()
            try:
                for _ in range(iterations):
                    handler._agents_status(self._caller_leave_event(), TENANT)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(("reader", exc))

        def writer():
            start.wait()
            try:
                for i in range(iterations):
                    handler._agents_status(
                        self._member_added_event(100 + i), TENANT
                    )
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(("writer", exc))

        threads = [threading.Thread(target=reader), threading.Thread(target=writer)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        # Every agent the writer logged in is present and fully formed.
        assert len(handler._agents[TENANT]) == iterations + 1
        for state in handler._agents[TENANT].values():
            assert state["queues"] == ["support"] or state["id"] == 5

    def test_concurrent_livestats_counters_are_consistent(self, handler):
        # Two threads driving join/leave on the same queue must not lose updates
        # or trip over the shared ``waiting_calls`` list.
        iterations = 300
        errors = []
        start = threading.Barrier(2)

        def joiner():
            start.wait()
            try:
                for i in range(iterations):
                    handler._livestats(_join_event(f"j-{i}", count="1"), TENANT)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(("joiner", exc))

        def leaver():
            start.wait()
            try:
                for i in range(iterations):
                    handler._livestats(_leave_event(f"j-{i}"), TENANT)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(("leaver", exc))

        threads = [threading.Thread(target=joiner), threading.Thread(target=leaver)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        stats = handler._stats["support"]
        # Every leave increments received; counters stay coherent (no lost +=).
        assert stats["received"] == iterations
        assert stats["answered"] == iterations

    def test_state_methods_are_mutually_exclusive(self, handler):
        """Deterministic proof that ``_lock`` serialises state access.

        While a REST worker holds the lock inside ``get_agents_status`` (modelled
        by a blocking ``confd.agents.list`` — the real call releases the GIL on
        network I/O), the bus consumer thread must not be able to run
        ``_livestats`` concurrently. Without the lock this assertion fails
        because the second thread runs immediately.
        """
        entered = threading.Event()
        other_done = threading.Event()

        def slow_list(**kwargs):
            # We are inside get_agents_status, holding the lock. Let the other
            # thread attempt its (lock-guarded) call and assert it cannot finish
            # while we still hold the lock.
            entered.set()
            assert not other_done.wait(timeout=0.2), (
                "a second thread mutated state while get_agents_status held the lock"
            )
            return {"items": []}

        handler.confd.agents.list.side_effect = slow_list
        handler.agentd.agents.get_agent_statuses.return_value = []

        def contender():
            entered.wait(timeout=1.0)
            handler._livestats(_join_event("c-1", count="1"), TENANT)
            other_done.set()

        thread = threading.Thread(target=contender)
        thread.start()
        handler.get_agents_status(TENANT)
        thread.join(timeout=1.0)

        assert other_done.is_set()  # contender eventually ran once lock released


def _build_seed_agent(agent_id, number, queues):
    return {
        "id": agent_id,
        "number": number,
        "fullname": "Seed Agent",
        "queue": queues[0] if queues else False,
        "queues": list(queues),
        "paused_queues": [],
        "is_logged": bool(queues),
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

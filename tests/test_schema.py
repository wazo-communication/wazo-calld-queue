# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

import pytest
from marshmallow import ValidationError

from wazo_calld_queue.schema import (
    intercept_schema,
    queue_list_schema,
    queue_member_schema,
    queue_schema,
)


class TestQueueMemberSchema:
    def test_load_coerces_integers(self):
        result = queue_member_schema.load(
            {
                "queue": "support",
                "interface": "SIP/abc",
                "penalty": "1",
                "paused": "0",
                "member_name": "John",
                "state_interface": "SIP/abc",
                "reason": "lunch",
            }
        )

        assert result["penalty"] == 1
        assert result["paused"] == 0
        assert result["interface"] == "SIP/abc"

    def test_load_allows_empty_payload(self):
        assert queue_member_schema.load({}) == {}

    def test_non_integer_penalty_is_rejected(self):
        with pytest.raises(ValidationError):
            queue_member_schema.load({"penalty": "not-a-number"})


class TestInterceptSchema:
    def test_load_valid_payload(self):
        result = intercept_schema.load(
            {"queue_name": "support", "call_id": "c1", "destination": "1234"}
        )

        assert result == {
            "queue_name": "support",
            "call_id": "c1",
            "destination": "1234",
        }

    def test_empty_queue_name_is_rejected(self):
        with pytest.raises(ValidationError):
            intercept_schema.load({"queue_name": ""})


class TestQueueListSchema:
    def test_dump_many_normalizes_integers(self):
        # services._queues feeds AMI string values; the schema exposes ints.
        data = [
            {
                "queue": "support",
                "available": "1",
                "logged_in": "2",
                "talk_time": "30",
                "longest_hold_time": "10",
                "talking": "1",
                "callers": "0",
                "hold_time": "5",
            }
        ]

        result = queue_list_schema.dump(data, many=True)

        assert result[0]["queue"] == "support"
        assert result[0]["available"] == 1
        assert result[0]["logged_in"] == 2


class TestQueueSchema:
    def test_dump_includes_members(self):
        data = {
            "queue": "support",
            "strategy": "ringall",
            "calls": 1,
            "members": [{"name": "Agent/1001", "status": "1"}],
        }

        result = queue_schema.dump(data)

        assert result["queue"] == "support"
        assert result["strategy"] == "ringall"
        assert result["members"] == [{"name": "Agent/1001", "status": "1"}]

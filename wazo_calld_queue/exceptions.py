# Copyright 2026 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from xivo.rest_api_helpers import APIException


class SupervisorNotInQueue(APIException):
    def __init__(self, queue_name):
        super().__init__(
            403,
            "Supervisor is not a member of this queue",
            "supervisor-not-in-queue",
            details={"queue_name": queue_name},
        )


class AgentNotLogged(APIException):
    def __init__(self):
        super().__init__(
            400,
            "Agent must be logged in before being connected to a queue",
            "agent-not-logged",
        )


class AgentWdaNotConnected(APIException):
    def __init__(self, agent_id):
        super().__init__(
            409,
            "Agent application (WDA) is not connected; cannot connect agent to queue",
            "agent-wda-not-connected",
            details={"agent_id": agent_id},
        )


class AgentHasNoLine(APIException):
    def __init__(self, agent_id):
        super().__init__(
            400,
            "Agent has no line to log in on",
            "agent-has-no-line",
            details={"agent_id": agent_id},
        )


class NoSuchAgentOrQueue(APIException):
    def __init__(self):
        super().__init__(404, "No such agent or queue", "no-such-agent-or-queue")


class AgentdUpstreamError(APIException):
    def __init__(self, original_error):
        super().__init__(
            502,
            "Unexpected error from wazo-agentd",
            "agentd-error",
            details={"original_error": str(original_error)},
        )

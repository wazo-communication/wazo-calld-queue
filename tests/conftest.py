# Copyright 2024 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

"""Shared test fixtures.

``wazo_calld_queue.bus_consume`` imports ``wazo_calld_queue.events`` which
depends on ``wazo_bus`` (only for the ``TenantEvent`` / ``ServiceEvent`` base
classes). ``wazo_bus`` is part of the Wazo stack and is not available when
running the plugin's unit tests in isolation, so we install a minimal stub
into ``sys.modules`` before the plugin package is imported.
"""

import sys
import types

from unittest.mock import Mock

import pytest


def _install_wazo_bus_stub():
    if "wazo_bus" in sys.modules:
        return

    class _BaseEvent:
        def __init__(self, content=None):
            self.content = content

    class TenantEvent(_BaseEvent):
        def __init__(self, content=None, tenant_uuid=None):
            super().__init__(content)
            self.tenant_uuid = tenant_uuid

    class ServiceEvent(_BaseEvent):
        def __init__(self, content=None):
            super().__init__(content)

    event_mod = types.ModuleType("wazo_bus.resources.common.event")
    event_mod.TenantEvent = TenantEvent
    event_mod.ServiceEvent = ServiceEvent

    common_mod = types.ModuleType("wazo_bus.resources.common")
    common_mod.event = event_mod
    resources_mod = types.ModuleType("wazo_bus.resources")
    resources_mod.common = common_mod
    wazo_bus_mod = types.ModuleType("wazo_bus")
    wazo_bus_mod.resources = resources_mod

    sys.modules["wazo_bus"] = wazo_bus_mod
    sys.modules["wazo_bus.resources"] = resources_mod
    sys.modules["wazo_bus.resources.common"] = common_mod
    sys.modules["wazo_bus.resources.common.event"] = event_mod


def _install_wazo_calld_stub():
    """Stub ``wazo_calld.plugin_helpers.mallow.StrictDict`` used by ``schema``.

    The real ``StrictDict`` is a marshmallow field validating dict entries; for
    unit tests a pass-through field accepting the same constructor kwargs is
    enough to import and exercise the schemas.
    """
    if "wazo_calld" in sys.modules:
        return

    from marshmallow import fields

    class StrictDict(fields.Field):
        def __init__(self, key_field=None, value_field=None, *args, **kwargs):
            self.key_field = key_field
            self.value_field = value_field
            super().__init__(*args, **kwargs)

        def _serialize(self, value, attr, obj, **kwargs):
            return value

        def _deserialize(self, value, attr, data, **kwargs):
            return value

    mallow_mod = types.ModuleType("wazo_calld.plugin_helpers.mallow")
    mallow_mod.StrictDict = StrictDict
    plugin_helpers_mod = types.ModuleType("wazo_calld.plugin_helpers")
    plugin_helpers_mod.mallow = mallow_mod
    wazo_calld_mod = types.ModuleType("wazo_calld")
    wazo_calld_mod.plugin_helpers = plugin_helpers_mod

    sys.modules["wazo_calld"] = wazo_calld_mod
    sys.modules["wazo_calld.plugin_helpers"] = plugin_helpers_mod
    sys.modules["wazo_calld.plugin_helpers.mallow"] = mallow_mod


def _install_xivo_stub():
    """Stub ``xivo.rest_api_helpers.APIException`` used by ``exceptions``.

    The plugin's ``exceptions`` module subclasses the real ``APIException``
    from ``xivo.rest_api_helpers`` (part of the Wazo stack, not installed for
    isolated unit tests). A minimal base class with the same constructor is
    enough to import and exercise the exception subclasses.
    """
    if "xivo.rest_api_helpers" in sys.modules:
        return

    class APIException(Exception):
        def __init__(
            self, status_code, message, error_id, details=None, resource=None
        ):
            super().__init__(message)
            self.status_code = status_code
            self.message = message
            self.id_ = error_id
            self.details = details or {}
            self.resource = resource

    mod = types.ModuleType("xivo.rest_api_helpers")
    mod.APIException = APIException
    xivo_mod = sys.modules.get("xivo") or types.ModuleType("xivo")
    xivo_mod.rest_api_helpers = mod
    sys.modules["xivo"] = xivo_mod
    sys.modules["xivo.rest_api_helpers"] = mod


def _install_wazo_agentd_client_stub():
    """Stub ``wazo_agentd_client.error`` used by ``services``.

    Mirrors the real module's ``AgentdClientError`` (stores its argument on the
    ``error`` attribute) and the error-code string constants the service maps
    to HTTP status codes.
    """
    if "wazo_agentd_client.error" in sys.modules:
        return

    class AgentdClientError(Exception):
        def __init__(self, error):
            super().__init__(error)
            self.error = error

    mod = types.ModuleType("wazo_agentd_client.error")
    mod.AgentdClientError = AgentdClientError
    mod.NO_SUCH_AGENT = "no such agent"
    mod.NO_SUCH_QUEUE = "no such queue"
    mod.NOT_LOGGED = "not logged in"
    mod.ALREADY_IN_QUEUE = "agent already in queue"
    mod.NOT_IN_QUEUE = "agent not in queue"

    agentd_mod = sys.modules.get("wazo_agentd_client") or types.ModuleType(
        "wazo_agentd_client"
    )
    agentd_mod.error = mod
    sys.modules["wazo_agentd_client"] = agentd_mod
    sys.modules["wazo_agentd_client.error"] = mod


_install_wazo_bus_stub()
_install_wazo_calld_stub()
_install_xivo_stub()
_install_wazo_agentd_client_stub()

# Imported only after the stub is in place.
from wazo_calld_queue.bus_consume import QueuesBusEventHandler  # noqa: E402


@pytest.fixture
def handler():
    return QueuesBusEventHandler(
        bus_publisher=Mock(name="bus_publisher"),
        confd=Mock(name="confd"),
        agentd=Mock(name="agentd"),
    )

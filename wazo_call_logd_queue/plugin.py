# Copyright 2021 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from .bus_publish import QueueBusPublisher
from .resources import QueueLogRequireResource, QueueLogStoreResource
from .services import Services


class Plugin:

    def load(self, dependencies):
        api = dependencies["api"]
        dao = dependencies["dao"]
        bus = dependencies["bus_publisher"]

        publisher = QueueBusPublisher(bus)
        services = Services(dao, publisher)

        api.add_resource(
            QueueLogStoreResource,
            "/queues/queue_log/store",
            resource_class_args=[services],
        )
        api.add_resource(
            QueueLogRequireResource,
            "/queues/queue_log/require",
            resource_class_args=[services],
        )

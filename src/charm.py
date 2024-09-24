#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charmed Kafka App for testing the Kafka Charmed Operator."""

import logging

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops.main import main

from constants import CONTAINER
from core.context import Context
from core.domain import CharmConfig
from events.integration_hub import SparkIntegrationHubEvents
from events.kafka import KafkaEvents
from events.metastore import MetastoreEvents
from events.spark_app import SparkAppEvents
from workload import SparkBase

logger = logging.getLogger(__name__)


class SparkAppCharm(TypedCharmBase[CharmConfig]):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.workload = SparkBase(self.unit.get_container(CONTAINER))

        self.context = Context(self.model, self.config)

        self.spark_app = SparkAppEvents(self, self.context, self.workload)
        self.integration_hub = SparkIntegrationHubEvents(self, self.context, self.workload)

        self.kafka = KafkaEvents(self, self.context, self.workload)

        self.metastore = MetastoreEvents(self, self.context, self.workload)


if __name__ == "__main__":
    main(SparkAppCharm)

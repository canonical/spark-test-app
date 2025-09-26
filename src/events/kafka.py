#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Metastore database related event handlers."""

from charms.data_platform_libs.v0.data_interfaces import KafkaRequirerEventHandlers
from ops import CharmBase, RelationBrokenEvent, RelationEvent

from common.logging import WithLogging
from core.context import Context
from core.workload import KafkaAppWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready


class KafkaEvents(BaseEventHandler, WithLogging):
    """Class implementing Kafka event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KafkaAppWorkloadBase):
        super().__init__(charm, "metastore")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kafka_provider = KafkaRequirerEventHandlers(self.charm, self.context.kafka_requirer)

        self.framework.observe(self.kafka_provider.on.topic_created, self._on_topic_created)
        self.framework.observe(
            self.kafka_provider.on.bootstrap_server_changed, self._on_topic_created
        )
        self.framework.observe(
            self.charm.on.kafka_relation_broken, self._on_kafka_relation_removed
        )

    @compute_status
    @defer_when_not_ready
    def _on_topic_created(self, _: RelationEvent) -> None:
        """Handle event when metastore database is created."""
        self.logger.info("Kafka connection created...")

        self.workload.set_environment(
            {
                "KAFKA_USERNAME": self.context.kafka.username,
                "KAFKA_PASSWORD": self.context.kafka.password,
                "KAFKA_ENDPOINTS": self.context.kafka.endpoints,
                "KAFKA_TOPIC": self.context.config.topic_name,
            }
        )

        if self.context.config.auto_start:
            self.logger.info("Auto-start enabled: starting job")
            self.workload.start()

    @compute_status
    def _on_kafka_relation_removed(self, _: RelationBrokenEvent) -> None:
        """Handle event when metastore database relation is removed."""
        self.logger.info("Kafka relation removed")
        self.workload.stop()

        self.workload.set_environment(
            {
                "KAFKA_USERNAME": None,
                "KAFKA_PASSWORD": None,
                "KAFKA_ENDPOINTS": None,
                "KAFKA_TOPIC": None,
            }
        )

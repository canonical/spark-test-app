#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Metastore database related event handlers."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequirerEventHandlers
from ops import CharmBase, RelationBrokenEvent, RelationEvent

from common.logging import WithLogging
from core.context import Context
from core.workload import KafkaAppWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.hive_site import HiveConfig


class MetastoreEvents(BaseEventHandler, WithLogging):
    """Class implementing Kafka event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KafkaAppWorkloadBase):
        super().__init__(charm, "metastore")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.database_provider = DatabaseRequirerEventHandlers(
            self.charm, self.context.metastore_requirer, "metadata"
        )

        self.framework.observe(self.database_provider.on.database_created, self._on_db_created)
        self.framework.observe(self.database_provider.on.endpoints_changed, self._on_db_created)
        self.framework.observe(self.charm.on.metastore_relation_broken, self._on_relation_removed)

    @compute_status
    @defer_when_not_ready
    def _on_db_created(self, event: RelationEvent) -> None:
        """Handle event when metastore database is created."""
        self.logger.info("Metastore connection created...")

        if metastore := self.context.metastore:
            hive_site = HiveConfig(metastore)
            self.workload.write(hive_site.contents, str(self.workload.paths.hive_site))

        self.workload.set_environment({"HIVE_TABLE": f"relation{event.relation.id}"})

    @compute_status
    def _on_relation_removed(self, _: RelationBrokenEvent) -> None:
        """Handle event when metastore database relation is removed."""
        self.logger.info("Metastore relation removed")
        self.workload.stop()
        self.workload.exec(f"rm -rf {self.workload.paths.hive_site}")
        self.workload.set_environment({"HIVE_TABLE": None})

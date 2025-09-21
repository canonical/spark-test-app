#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration Hub related event handlers."""

from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    SparkServiceAccountRequirerEventHandlers,
)
from ops import CharmBase

from common.logging import WithLogging
from core.context import Context
from core.workload import KafkaAppWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready


class SparkIntegrationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing Integration Hub event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: KafkaAppWorkloadBase):
        super().__init__(charm, "integration-hub")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.requirer = SparkServiceAccountRequirerEventHandlers(
            self.charm, self.context.spark_service_account_interface
        )

        self.framework.observe(self.requirer.on.account_granted, self._on_account_granted)
        self.framework.observe(self.requirer.on.account_gone, self._on_account_gone)

    @compute_status
    @defer_when_not_ready
    def _on_account_granted(self, event: ServiceAccountGrantedEvent):
        """Handle the `ServiceAccountGrantedEvent` event from integration hub."""
        self.logger.info("Service account received")

        namespace, username = event.service_account.split(":")

        self.workload.set_environment(
            {
                "SPARK_USER": username,
                "SPARK_NAMESPACE": namespace,
            }
        )

    @compute_status
    def _on_account_gone(self, _: ServiceAccountGoneEvent):
        """Handle the `ServiceAccountGoneEvent` event from integration hub."""
        self.logger.info("Service account deleted")
        self.workload.set_environment(
            {
                "SPARK_USER": None,
                "SPARK_NAMESPACE": None,
            }
        )

#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Module containing all business logic related to the workload."""
import json

import ops.pebble
from ops.model import Container

from common.k8s import K8sWorkload
from common.utils import WithLogging
from core.domain import User

user = User(name="_daemon_", group="_daemon_")

class SparkBase(K8sWorkload, WithLogging):
    """Class representing Workload implementation for History Server on K8s."""

    CONTAINER = "spark"
    CONTAINER_LAYER = "app"

    SERVICE = "spark-job"

    CONFS_PATH = "/etc/spark/conf"
    ENV_FILE = "/etc/spark/environment"

    def __init__(self, container: Container, user: User = user):
        K8sWorkload.__init__(self)

        self.container = container
        self.user = user

    @property
    def _spark_layer(self):
        """Return a dictionary representing a Pebble layer."""
        layer = {
            "summary": "spark history server layer",
            "description": "pebble config layer for spark history server",
            "services": {
                self.SERVICE: {
                    "override": "replace",
                    "summary": "Spark Job Command",
                    "startup": "disabled",
                    "command": f"/bin/bash /var/lib/spark/app.sh",
                    "environment": self.envs,
                    "on-success": "ignore",
                    "on-failure": "ignore"
                }
            },
        }
        self.logger.info(f"Layer: {json.dumps(layer)}")
        return layer

    def start(self):
        """Execute business-logic for starting the workload."""

        self.container.add_layer(self.CONTAINER_LAYER, self._spark_layer, combine=True)

        # Push an updated layer with the new config
        # self.container.replan()
        self.container.restart(self.SERVICE)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.container.stop(self.SERVICE)

    def active(self) -> bool:
        """Return the health of the service."""
        try:
            service = self.container.get_service(self.SERVICE)
        except ops.pebble.ConnectionError:
            self.logger.debug(f"Service {self.SERVICE} not running")
            return False

        return service.is_running()
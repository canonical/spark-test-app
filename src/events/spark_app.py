#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub related event handlers."""
import os

from ops import CharmBase, ActionEvent, PebbleReadyEvent

from common.logging import WithLogging
from core.workload import KafkaAppWorkloadBase
from core.context import Context
from core.domain import Flavour
from events.base import BaseEventHandler, compute_status
from ops import UpdateStatusEvent
from managers.kafka_app import KafkaApp

from pathlib import Path


class SparkAppEvents(BaseEventHandler, WithLogging):

    def __init__(self, charm: CharmBase, context: Context,
                 workload: KafkaAppWorkloadBase):
        super().__init__(charm, "spark-app")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.kafka_app = KafkaApp(context, workload)

        self.framework.observe(
            self.charm.on.spark_pebble_ready,
            self._on_spark_pebble_ready,
        )

        self.framework.observe(
            getattr(self.charm.on, "start_process_action"), self._start_process_action
        )
        self.framework.observe(
            getattr(self.charm.on, "stop_process_action"), self._stop_process_action
        )

        self.framework.observe(
            self.charm.on.config_changed, self._on_config_changed
        )
        self.framework.observe(self.charm.on.update_status, self._on_update_status)

    def copy_to_workload(self, source: Path | str):
        self.logger.info(f"Copying files {source}")

        source_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".." / "resource"
        target_path = self.workload.paths.bin_path

        src = source_path / source
        dst = target_path / source

        with src.open("r") as fid:
            self.workload.write(
                "\n".join(fid.readlines()), str(dst)
            )


    @compute_status
    def _on_spark_pebble_ready(self, event: PebbleReadyEvent) -> None:
        """Handle the bootstrap server changed."""
        # Event triggered when a bootstrap server was changed for this application

        for flavour in Flavour:
            source = f"{flavour}.py"
            self.copy_to_workload(source)

        source = "app.sh"

        self.copy_to_workload(source)

        self.logger.info(f"Files copied")

        self.workload.set_environment({
            "EXTRA_ARGS": self.kafka_app.extra_args,
            "SCRIPT": f"{self.workload.paths.bin_path}/{self.context.config.flavour.value}.py"
        })

    def _start_process_action(self, _: ActionEvent):
        self.workload.start()

    def _stop_process_action(self, _: ActionEvent):
        self.workload.stop()

    @compute_status
    def _on_config_changed(self, _) -> None:
        """Handle the on configuration changed hook."""
        self.logger.info(f"Resetting configs")
        self.workload.set_environment({
            "EXTRA_ARGS": self.kafka_app.extra_args,
            "SCRIPT": f"{self.workload.paths.bin_path}/{self.context.config.flavour.value}.py"
        })

    @compute_status
    def _on_update_status(self, event: UpdateStatusEvent):
        pass



#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Module with the core interfaces for the Kafka app workload."""

from abc import ABC
from pathlib import Path

from common.workload import AbstractWorkload


class KafkaAppPaths:
    """Object to store common paths for Kafka."""

    def __init__(self, bin_path: Path | str, conf_path: Path | str):
        self.bin_path = bin_path if isinstance(bin_path, Path) else Path(bin_path)
        self.conf_path = conf_path if isinstance(conf_path, Path) else Path(conf_path)

    @property
    def hive_site(self) -> Path:
        """Return the path of the spark-properties file."""
        return self.conf_path / "hive-site.xml"

    @property
    def env_file(self) -> Path:
        """Return the path of the files where environment vars are stored."""
        return self.conf_path / "environment"


class KafkaAppWorkloadBase(AbstractWorkload, ABC):
    """Base interface for common workload operations."""

    paths: KafkaAppPaths

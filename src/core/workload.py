from pathlib import Path
from common.workload import AbstractWorkload
from abc import ABC

class KafkaAppPaths:
    """Object to store common paths for Kafka."""

    def __init__(
            self, bin_path: Path | str, conf_path: Path | str
    ):
        self.bin_path = bin_path if isinstance(bin_path, Path) else Path(bin_path)
        self.conf_path = conf_path if isinstance(conf_path, Path) else Path(conf_path)

    @property
    def hive_site(self) -> Path:
        """Return the path of the spark-properties file."""
        return self.conf_path / "hive-site.xml"

    @property
    def env_file(self):
        return self.conf_path / "environment"


class KafkaAppWorkloadBase(AbstractWorkload, ABC):
    """Base interface for common workload operations."""

    paths: KafkaAppPaths
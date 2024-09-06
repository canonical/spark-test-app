from common.k8s import AbstractWorkload
from core.context import Context

from typing import List

from core.domain import Flavour


class KafkaApp:
    KAFKA_IMAGE = "ghcr.io/canonical/charmed-spark:3.4.2-22.04_edge"

    def __init__(
        self, context: Context
    ):
        self.context = context

    @property
    def deploy_mode(self) -> str:
        return self.context.config.deploy_mode.value

    @property
    def confs(self) -> dict[str, str]:
        return {
            "spark.kubernetes.container.image": self.KAFKA_IMAGE,
            "spark.jars.ivy": "/tmp",
        }

    @property
    def packages(self) -> List[str]:
        return [
            "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.2",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2"
        ] if self.context.config.flavour == Flavour.KAFKA.value else []

    @property
    def extra_args(self) -> str:
        args = [
            f"--deploy-mode {self.deploy_mode}",
        ] + [
            f"--conf {key}={value}" for key, value in self.confs.items()
        ] + (
            [f"--packages {','.join(self.packages)}"] if self.packages else []
        )
        return " ".join(args)

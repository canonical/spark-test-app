"""Module for the manager functionality of Kafka client applications."""

from typing import List

from core.context import Context
from core.domain import Flavour
from core.workload import KafkaAppWorkloadBase
from managers.k8s import K8sManager


class KafkaApp:
    """Manager for a Kafka client application."""

    def __init__(self, context: Context, workload: KafkaAppWorkloadBase):
        self.context = context

        self.k8s_manager = (
            K8sManager(context.service_account, workload) if context.service_account else None
        )

    @property
    def deploy_mode(self) -> str:
        """Return the selected deploy mode to be used."""
        return self.context.config.deploy_mode.value

    @property
    def confs(self) -> dict[str, str]:
        """Return the spark configurations client-specific to be used to run the Spark job."""
        if (
            self.context.metastore
            and self.k8s_manager
            and (properties := self.k8s_manager.get_properties().props)
        ):
            warehouse_dir = {
                "spark.sql.warehouse.dir": properties["spark.kubernetes.file.upload.path"]
            }
        else:
            warehouse_dir = {}

        return {
            "spark.kubernetes.container.image": self.context.config.spark_image,
            "spark.sql.shuffle.partitions": self.context.config.partitions,
            "spark.jars.ivy": "/tmp",
        } | warehouse_dir

    @property
    def packages(self) -> List[str]:
        """Return the list of extra packages to be used for running the Spark job."""
        return (
            [
                "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.2",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2",
            ]
            if self.context.config.flavour == Flavour.KAFKA.value
            else []
        )

    @property
    def extra_args(self) -> str:
        """Return the full extra-args string to be added to the Spark submit command."""
        args = (
            [
                f"--deploy-mode {self.deploy_mode}",
            ]
            + [f"--conf {key}={value}" for key, value in self.confs.items()]
            + ([f"--packages {','.join(self.packages)}"] if self.packages else [])
        )
        return " ".join(args)

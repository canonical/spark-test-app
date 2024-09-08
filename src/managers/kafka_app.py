from common.k8s import AbstractWorkload
from core.context import Context

from typing import List

from core.domain import Flavour
from core.workload import KafkaAppWorkloadBase

from managers.k8s import SparkServiceAccountManager

class KafkaApp:

    def __init__(
        self, context: Context, workload: KafkaAppWorkloadBase
    ):
        self.context = context

        self.service_account_manager = SparkServiceAccountManager(context.service_account) if context.service_account else None

    @property
    def deploy_mode(self) -> str:
        return self.context.config.deploy_mode.value

    @property
    def confs(self) -> dict[str, str]:

        if (
                self.context.metastore and
                self.service_account_manager and
                self.service_account_manager.has_cluster_permissions() and
                self.service_account_manager.exists()
        ):
            properties = self.service_account_manager.spark8t_account.configurations.props

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

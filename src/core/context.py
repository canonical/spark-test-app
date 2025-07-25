#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequirerData, KafkaRequirerData
from ops import Model, Relation

from common.logging import WithLogging
from charms.spark_integration_hub_k8s.v0.spark_service_account import SparkServiceAccountRequirerData
from constants import (
    KAFKA_RELATION_NAME,
    POSTGRESQL_METASTORE,
    SPARK_SERVICE_ACCOUNT,
)
from core.domain import (
    CharmConfig,
    KafkaProviderRelationDataBag,
    PostgreSQLProviderRelationDataBag,
    SparkServiceAccountInfo,
)


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, model: Model, config: CharmConfig):
        self.model = model
        self.config = config
        self.metastore_requirer = DatabaseRequirerData(
            self.model, POSTGRESQL_METASTORE, database_name=self.config.metastore_name
        )
        self.kafka_requirer = KafkaRequirerData(
            self.model,
            relation_name=KAFKA_RELATION_NAME,
            topic=self.config.topic_name,
            extra_user_roles="admin",
            consumer_group_prefix="streaming-app",
        )

        namespace = self.config.namespace if self.config.namespace else self.model.name
        service_account = self.config.service_account
        self.spark_service_account_interface = SparkServiceAccountRequirerData(
            self.model,
            relation_name=SPARK_SERVICE_ACCOUNT,
            service_account=f"{namespace}:{service_account}",
        )

    @property
    def _spark_account_relation(self) -> Relation | None:
        """The integration hub relation."""
        return self.model.get_relation(SPARK_SERVICE_ACCOUNT)

    # --- DOMAIN OBJECTS ---

    @property
    def metastore(self) -> PostgreSQLProviderRelationDataBag | None:
        """The state of metastore DB connection."""
        for data in self.metastore_requirer.fetch_relation_data().values():
            if any(key not in data for key in ["endpoints", "username", "password"]):
                continue
            return PostgreSQLProviderRelationDataBag(**data)
        return None

    @property
    def kafka(self) -> KafkaProviderRelationDataBag | None:
        """The state of metastore DB connection."""
        for data in self.kafka_requirer.fetch_relation_data().values():
            if any(key not in data for key in ["endpoints", "username", "password"]):
                continue
            return KafkaProviderRelationDataBag(**data)
        return None

    @property
    def service_account(self) -> SparkServiceAccountInfo | None:
        """The state of service account information."""
        if not self._spark_account_relation:
            return None
        return SparkServiceAccountInfo(
            self._spark_account_relation,
            self.spark_service_account_interface, self.model.app
        )

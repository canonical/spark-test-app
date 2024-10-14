#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequirerData, KafkaRequirerData
from ops import Model, Relation

from common.logging import WithLogging
from common.relation.spark_sa import RequirerData
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
        data_interface = RequirerData(self.model, SPARK_SERVICE_ACCOUNT)

        if account := SparkServiceAccountInfo(
            self._spark_account_relation, data_interface, self.model.app
        ):
            return account

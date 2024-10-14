#!/usr/bin/env python3
#
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Definition of various model classes."""

from enum import Enum
from typing import List, MutableMapping, Optional

from charms.data_platform_libs.v0.data_interfaces import Data
from ops import Application, Relation, Unit
from pydantic import BaseModel, ValidationError, validator

from common.relation.domain import RelationState


# The StateBase class should be deprecated in favor of a RelationBase class
# when secrets are enabled on S3 relation, and S3 classes have a similar
# structure with respect to the other data-platform interfaces.
class StateBase:
    """Base state object."""

    def __init__(self, relation: Relation | None, component: Unit | Application):
        self.relation = relation
        self.component = component

    @property
    def relation_data(self) -> MutableMapping[str, str]:
        """The raw relation data."""
        if not self.relation:
            return {}

        return self.relation.data[self.component]

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        if not self.relation:
            return

        self.relation_data.update(items)

    def clean(self) -> None:
        """Clean the content of the relation data."""
        if not self.relation:
            return
        self.relation.data[self.component] = {}


class SparkServiceAccountInfo(RelationState):
    """Requirer-side of the Integration Hub relation."""

    def __init__(
        self,
        relation: Relation | None,
        data_interface: Data,
        component: Application,
    ):
        super().__init__(relation, data_interface, component)
        self.data_interface = data_interface
        self.app = component

    def __bool__(self):
        """Return flag of whether the class is ready to be used."""
        return super().__bool__() and "service-account" in self.relation_data.keys()

    @property
    def service_account(self):
        """Service account used for Spark."""
        return self.relation_data["service-account"]

    @property
    def namespace(self):
        """Namespace used for running Spark jobs."""
        return self.relation_data["namespace"]


class BaseEnumStr(str, Enum):
    """Base class for string enum."""

    def __str__(self) -> str:
        """Return the value as a string."""
        return str(self.value)


class DeployMode(BaseEnumStr):
    """Class for the app type."""

    CLIENT = "client"
    CLUSTER = "cluster"


class Flavour(BaseEnumStr):
    """Class for the app type."""

    DUMMY = "dummy"
    KAFKA = "kafka"


class User(BaseModel):
    """Class representing the user running the Pebble workload services."""

    name: str
    group: str


class CharmConfig(BaseModel):
    """Charmed configuration class."""

    topic_name: str
    deploy_mode: DeployMode
    flavour: Flavour
    service_account: str
    namespace: str
    auto_start: bool
    metastore_name: str
    spark_image: str
    partitions: int


class AppType(BaseEnumStr):
    """Class for the app type."""

    PRODUCER = "producer"
    CONSUMER = "consumer"


class KafkaRequirerRelationDataBag(BaseModel):
    """Class for Kafka relation data."""

    topic: str
    extra_user_roles: str

    @classmethod
    @validator("extra_user_roles")
    def _role_parser(cls, roles: str):
        """Roles parsers."""
        try:
            _app_type = [AppType(value) for value in roles.split(",")]
        except Exception as e:
            raise ValidationError(f"could not properly parse the roles configuration: {e}")
        return roles

    @property
    def app_type(self) -> List[AppType]:
        """Return the app types."""
        return [AppType(value) for value in self.extra_user_roles.split(",")]


class AuthDataBag(BaseModel):
    """Class to handle authentication parameters."""

    endpoints: str
    username: str
    password: str
    tls: Optional[str] = None
    tls_ca: Optional[str] = None


class KafkaProviderRelationDataBag(AuthDataBag):
    """Class for the Kafka provider relation databag."""

    consumer_group_prefix: Optional[str]

    @property
    def security_protocol(self) -> str:
        """Return the security protocol."""
        return "SASL_PLAINTEXT" if self.tls is not None else "SASL_SSL"

    @property
    def bootstrap_server(self) -> str:
        """Return the bootstrap servers endpoints."""
        return self.endpoints


class PostgreSQLProviderRelationDataBag(AuthDataBag):
    """Class that handle the PostgreSQL relation databag."""

    database: Optional[str]
    read_only_endpoints: Optional[str]
    version: Optional[str]

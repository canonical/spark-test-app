# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Class to handle configuration and relation databag."""
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ValidationError, validator


class BaseEnumStr(str, Enum):
    """Base class for string enum."""

    def __str__(self) -> str:
        """Return the value as a string."""
        return str(self.value)


class DeployMode(BaseEnumStr):
    """Class for the app type."""

    CLIENT = "client"
    CLUSTER = "cluster"

class User(BaseModel):
    """Class representing the user running the Pebble workload services."""
    name: str
    group: str



class CharmConfig(BaseModel):
    """Charmed configuration class."""

    topic_name: str
    deploy_mode: DeployMode

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
            raise ValidationError(f"could not properly parsed the roles configuration: {e}")
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
    """Class for the provider relation databag."""

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
    """Class that handle the MongoDB relation databag."""

    database: Optional[str]
    read_only_endpoints: Optional[str]
    version: Optional[str]

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.


import pytest
from ops import pebble
from ops.testing import Container, Context, Model, Relation

from charm import SparkAppCharm
from constants import CONTAINER, KAFKA_RELATION_NAME, POSTGRESQL_METASTORE, SPARK_SERVICE_ACCOUNT
from workload import KafkaAppPaths, SparkBase


@pytest.fixture
def spark_app_charm():
    """Provide fixture for the SparkIntegrationHub charm."""
    yield SparkAppCharm


@pytest.fixture
def spark_app_ctx(spark_app_charm):
    """Provide fixture for scenario context based on the SparkIntegrationHub charm."""
    return Context(
        charm_type=spark_app_charm,
        juju_version="3.6.5",
    )


@pytest.fixture
def model():
    """Provide fixture for the testing Juju model."""
    return Model(name="test-model")


@pytest.fixture
def spark_app_container(tmp_path):
    """Provide fixture for the Integration Hub workload container."""
    layer = {
        "summary": "spark history server layer",
        "description": "pebble config layer for running the App script",
        "services": {
            "spark-job": {
                "override": "replace",
                "summary": "Spark Job Command",
                "startup": "disabled",
                "command": "/bin/bash /var/lib/spark/app/app.sh",
                "environment": {},
                "on-success": "ignore",
                "on-failure": "ignore",
            }
        },
    }

    return Container(
        name=CONTAINER,
        can_connect=True,
        layers={"base": pebble.Layer(layer)},
        service_statuses={"spark-app": pebble.ServiceStatus.ACTIVE},
    )


@pytest.fixture
def spark_service_account_relation():
    """Provide fixture for the S3 relation."""
    return Relation(
        endpoint=SPARK_SERVICE_ACCOUNT,
        interface="spark_service_account",
        remote_app_name="integration-hub",
        local_app_data={
            "service-account": "spark:spark-test-app",
            "spark-properties": '{"foo":"bar"}',
        },
        remote_app_data={
            "service-account": "spark:spark-test-app",
            "spark-properties": '{"foo":"bar"}',
        },
    )


@pytest.fixture
def metastore_relation() -> Relation:
    return Relation(
        endpoint=POSTGRESQL_METASTORE,
        interface="postgresql_client",
        remote_app_name="metastore",
        local_app_data={
            "database": "metastore",
        },
        remote_app_data={
            "database": "myappB",
            "endpoints": "postgresql-k8s-primary:5432",
            "username": "spark-test-app",
            "password": "pwd",
        },
    )


@pytest.fixture
def kafka_relation():
    """Provide fixture for the Kafka relation."""
    return Relation(
        endpoint=KAFKA_RELATION_NAME,
        interface="kafka_client",
        remote_app_name="kafka",
        local_app_data={
            "topic": "test-topic",
            "extra-user-roles": "admin",
        },
        remote_app_data={
            "topic": "test-topic",
            "username": "kafka-test-app",
            "password": "test",
            "endpoints": "kafka.servers:9091",
            "consumer-group-prefix": "",
            "zookeeper-uris": "zk.servers:8021",
        },
    )


paths = KafkaAppPaths(bin_path="/var/lib/spark/app", conf_path="/etc/spark8t/conf")


def get_environment_file(container, context) -> None | str:
    tmp_path = container.get_filesystem(context)

    env_file = tmp_path / paths.env_file.relative_to("/")

    if not env_file.exists():
        return None

    return env_file.read_text()


def get_hive_file(container, context) -> None | str:
    tmp_path = container.get_filesystem(context)

    hive_file = tmp_path / paths.hive_site.relative_to("/")

    if not hive_file.exists():
        return None

    return hive_file.read_text()


def get_app_file(container, context) -> dict[str, str]:
    tmp_path = container.get_filesystem(context)

    bin_files = list((tmp_path / SparkBase.paths.bin_path.relative_to("/")).glob(pattern="*"))

    if not bin_files:
        return {}

    return {_file.name: _file.read_text() for _file in bin_files}

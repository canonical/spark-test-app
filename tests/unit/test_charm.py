#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from conftest import get_app_file, get_environment_file, get_hive_file
from ops.testing import Container, Context, Relation, State
from spark8t.domain import PropertyFile

import events
from charm import SparkAppCharm
from constants import CONTAINER, METASTORE_DATABASE_NAME

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


def test_start_spark(spark_app_ctx: Context[SparkAppCharm]) -> None:
    state = State(
        config={},
        containers=[Container(name=CONTAINER, can_connect=False)],
    )
    out = spark_app_ctx.run(spark_app_ctx.on.install(), state)
    assert out.unit_status == events.Status.WAITING_PEBBLE.value


def test_pebble_ready(
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
) -> None:
    state = State(
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(spark_app_ctx.on.pebble_ready(spark_app_container), state)

    # Assert that files have been copied
    assert get_environment_file(spark_app_container, spark_app_ctx) is not None

    assert "app.sh" in get_app_file(spark_app_container, spark_app_ctx)

    # Assert we are in blocked status because of missing relations
    assert out.unit_status == events.Status.MISSING_INTEGRATION_HUB.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
def test_valid_on_service_account(
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(
        spark_app_ctx.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == events.Status.ACTIVE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=False)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
def test_invalid_namespace(
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(
        spark_app_ctx.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == events.Status.INVALID_NAMESPACE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
@patch("managers.k8s.K8sManager.get_properties", return_value=PropertyFile({"foo": "bar"}))
def test_backend_not_configured(
    mock_get_properties,
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, metastore_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(
        spark_app_ctx.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == events.Status.MISSING_OBJECT_STORAGE_BACKEND.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
@patch(
    "managers.k8s.K8sManager.get_properties",
    return_value=PropertyFile({"spark.hadoop.fs.s3a.secret.key": "aaaaaaa"}),
)
def test_s3_configured(
    mock_get_properties,
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, metastore_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(
        spark_app_ctx.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == events.Status.ACTIVE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
@patch(
    "managers.k8s.K8sManager.get_properties",
    return_value=PropertyFile(
        {"spark.hadoop.fs.azure.account.key.AAAAAAA.dfs.core.windows.net": "aaaaaaa"}
    ),
)
def test_azure_configured(
    mock_get_properties,
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, metastore_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(
        spark_app_ctx.on.relation_changed(spark_service_account_relation), state
    )
    assert out.unit_status == events.Status.ACTIVE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("managers.k8s.K8sManager.is_azure_storage_configured", return_value=False)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
def test_kafka_missing(
    mock_cluster_permissions,
    mock_s3_configured,
    mock_azure_configured,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
) -> None:
    state = State(
        config={"flavour": "kafka"},
        relations=[spark_service_account_relation, metastore_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(spark_app_ctx.on.config_changed(), state)
    assert out.unit_status == events.Status.MISSING_KAFKA.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_s3_configured", return_value=True)
@patch("managers.k8s.K8sManager.is_azure_storage_configured", return_value=False)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
def test_kafka_flavor(
    mock_cluster_permissions,
    mock_s3_configured,
    mock_azure_configured,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
    kafka_relation: Relation,
) -> None:
    state = State(
        config={"flavour": "kafka"},
        relations=[spark_service_account_relation, metastore_relation, kafka_relation],
        containers=[spark_app_container],
    )
    out = spark_app_ctx.run(spark_app_ctx.on.config_changed(), state)
    assert out.unit_status == events.Status.ACTIVE.value


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
@patch(
    "managers.k8s.K8sManager.get_properties",
    return_value=PropertyFile(
        {"spark.hadoop.fs.azure.account.key.AAAAAAA.dfs.core.windows.net": "aaaaaaa"}
    ),
)
def test_kafka_relation_lifecycle(
    mock_get_properties,
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
    kafka_relation: Relation,
) -> None:
    state = State(
        config={"flavour": "kafka"},
        relations=[spark_service_account_relation, metastore_relation, kafka_relation],
        containers=[spark_app_container],
    )

    # Environment file does not exist at first
    assert not get_environment_file(spark_app_container, spark_app_ctx)

    spark_app_ctx.run(spark_app_ctx.on.relation_changed(kafka_relation), state)

    # Environment file is created and Kafka credentials are injected
    assert "KAFKA_USERNAME" in get_environment_file(spark_app_container, spark_app_ctx)

    spark_app_ctx.run(spark_app_ctx.on.relation_broken(kafka_relation), state)

    # Kafka credentials are removed from environment
    assert "KAFKA_USERNAME" not in get_environment_file(spark_app_container, spark_app_ctx)


@patch("managers.k8s.K8sManager.is_namespace_valid", return_value=True)
@patch("managers.k8s.K8sManager.is_service_account_valid", return_value=True)
@patch("managers.k8s.K8sManager.has_cluster_permissions", return_value=True)
@patch(
    "managers.k8s.K8sManager.get_properties",
    return_value=PropertyFile(
        {"spark.hadoop.fs.azure.account.key.AAAAAAA.dfs.core.windows.net": "aaaaaaa"}
    ),
)
def test_metastore_relation_lifecycle(
    mock_get_properties,
    mock_cluster_permissions,
    mock_valid_sa,
    mock_valid_ns,
    spark_app_ctx: Context[SparkAppCharm],
    spark_app_container: Container,
    spark_service_account_relation: Relation,
    metastore_relation: Relation,
    kafka_relation: Relation,
) -> None:
    state = State(
        relations=[spark_service_account_relation, metastore_relation, kafka_relation],
        containers=[spark_app_container],
    )

    # hive_site.xml file does not exist at first
    assert not get_hive_file(spark_app_container, spark_app_ctx)

    spark_app_ctx.run(spark_app_ctx.on.relation_changed(metastore_relation), state)

    # hive_site.xml file is created and Metastore credentials are injected
    assert METASTORE_DATABASE_NAME in get_hive_file(spark_app_container, spark_app_ctx)

    # TODO: When replacing fs operations using FsOps instead of exec calls, it will be possible to control
    #       the removal of the hive_site.xml file
    # out = spark_app_ctx.run(
    #     spark_app_ctx.on.relation_broken(metastore_relation), state
    # )
    #
    # # Metastore credentials are removed from hive_site.xml
    # assert METASTORE_DATABASE_NAME not in get_hive_file(spark_app_container, spark_app_ctx)

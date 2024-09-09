#!/usr/bin/env python3

# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""K8s manager."""

import re
from functools import cached_property

from lightkube.core.exceptions import ApiError
from spark8t.domain import Defaults, ServiceAccount as Spark8tAccount
from spark8t.services import K8sServiceAccountRegistry
from spark8t.services import LightKube
from spark8t.exceptions import AccountNotFound

from common.utils import WithLogging
from common.k8s import K8sUtils
from core.domain import SparkServiceAccountInfo


class SparkServiceAccountManager(WithLogging):

    def __init__(self, spark_service_account: SparkServiceAccountInfo):
        self.spark_service_account = spark_service_account

        self.k8s = K8sUtils()
        interface = LightKube(self.k8s.kube_config, Defaults())

        self.registry = K8sServiceAccountRegistry(interface)

    @property
    def service_account(self) -> str:
        return self.spark_service_account.service_account

    @property
    def namespace(self) -> str:
        return self.spark_service_account.namespace

    def verify(self) -> bool:
        """Verify service account information."""
        return (
                self.k8s.is_namespace_valid(self.namespace) and
                self.k8s.is_service_account_valid(
                    self.service_account, self.namespace
                )
        )

    def has_cluster_permissions(self) -> bool:
        """Return whether the service account has permission to read Spark configurations from the cluster."""
        try:
            _ = self.spark8t_account
        except ApiError:
            return False
        else:
            return True

    def exists(self) -> bool:
        """Return whether the spark service account exists."""
        try:
            _ = self.spark8t_account
        except AccountNotFound:
            return False
        else:
            return True

    @cached_property
    def spark8t_account(self) -> Spark8tAccount:
        return self.registry.get(f"{self.namespace}:{self.service_account}")

    def is_s3_configured(self) -> bool:
        """Return whether S3 object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.s3a\.secret\.key"
        return any(re.match(pattern, prop) for prop in
                   self.spark8t_account.configurations.props)

    def is_azure_storage_configured(self) -> bool:
        """Return whether Azure object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.azure\.account\.key\..*\.dfs\.core\.windows\.net"
        return any(re.match(pattern, prop) for prop in
                   self.spark8t_account.configurations.props)

#!/usr/bin/env python3
#
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""K8s manager."""

import re

from lightkube import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace, ServiceAccount
from spark8t.domain import PropertyFile
from spark8t.services import K8sServiceAccountRegistry, LightKube
from spark8t.utils import umask_named_temporary_file

from common.logging import WithLogging
from common.workload import AbstractWorkload
from core.domain import SparkServiceAccountInfo


class K8sManager(WithLogging):
    """Class that encapsulates various utilities related to K8s."""

    def __init__(self, service_account_info: SparkServiceAccountInfo, workload: AbstractWorkload):
        self.namespace = service_account_info.namespace
        self.service_account = service_account_info.service_account
        self.workload = workload

    def is_namespace_valid(self):
        """Return whether given namespace exists in K8s cluster."""
        try:
            Client().get(Namespace, name=self.namespace)
        except ApiError:
            return False
        return True

    def is_service_account_valid(self):
        """Return whether given service account in the given namespace exists in K8s cluster."""
        try:
            Client().get(ServiceAccount, name=self.service_account, namespace=self.namespace)
        except ApiError:
            return False
        return True

    def verify(self) -> bool:
        """Verify service account information."""
        return self.is_namespace_valid() and self.is_service_account_valid()

    def get_properties(self) -> PropertyFile:
        """Get Spark properties associated with this service account."""
        command = " ".join(
            [
                "python3",
                "-m",
                "spark8t.cli.service_account_registry",
                "get-config",
                "--username",
                self.service_account,
                "--namespace",
                self.namespace,
            ]
        )

        with umask_named_temporary_file(mode="w", prefix="spark-conf-", suffix=".conf") as t:
            try:
                result = self.workload.exec(command)
            except Exception:
                result = ""

            t.write(result)

            t.flush()

            return PropertyFile.read(t.name)

    def is_s3_configured(self) -> bool:
        """Return whether S3 object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.s3a\.secret\.key"
        return any(re.match(pattern, prop) for prop in self.get_properties().props)

    def is_azure_storage_configured(self) -> bool:
        """Return whether Azure object storage backend has been configured."""
        pattern = r"spark\.hadoop\.fs\.azure\.account\.key\..*\.dfs\.core\.windows\.net"
        return any(re.match(pattern, prop) for prop in self.get_properties().props)

    def has_cluster_permissions(self) -> bool:
        """Return whether the service account has permission to read Spark configurations from the cluster."""
        interface = LightKube(None, None)
        registry = K8sServiceAccountRegistry(interface)
        try:
            registry.get(f"{self.namespace}:{self.service_account}")
        except ApiError:
            return False
        else:
            return True

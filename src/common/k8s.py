#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Common classes/functions for K8s implementations."""

import logging
from abc import ABC

from ops import Container
from ops.pebble import ExecError
from typing_extensions import override

from lightkube import Client, KubeConfig
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Namespace, ServiceAccount

from common.logging import WithLogging
from common.workload import AbstractWorkload


class K8sWorkload(AbstractWorkload, WithLogging, ABC):
    """Class for providing implementation for IO operations on K8s."""

    container: Container
    ENV_FILE: str

    def __init__(self):
        self._envs = None

    def exists(self, path: str) -> bool:
        """Check for file existence.

        Args:
            path: the full filepath to be checked for
        """
        return self.container.exists(path)

    @override
    def read(self, path: str) -> list[str]:
        """Read from a file.

        Args:
            path: the full filepath to be read

        Returns:
            content of the file

        Raises:
            FileNotFound if the file does not exist
        """
        if not self.container.exists(path):
            raise FileNotFoundError

        with self.container.pull(path) as f:
            return f.read().split("\n")

    @override
    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        if mode == "a" and (current := self.read(path)):
            content = "\n".join(current + [content])
        self.container.push(path, content, make_dirs=True)

    @override
    def exec(
        self, command: str, env: dict[str, str] | None = None, working_dir: str | None = None
    ) -> str:
        try:
            process = self.container.exec(
                command=command.split(),
                environment=env,
                working_dir=working_dir,
                combine_stderr=True,
            )
            output, _ = process.wait_output()
            return output
        except ExecError as e:
            self.logger.error(str(e.stderr))
            raise e

    @override
    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()


    @property
    def envs(self):
        """Return current environment."""
        if self._envs is not None:
            return self._envs

        self._envs = self.from_env(self.read(self.ENV_FILE)) if self.exists(self.ENV_FILE) else {}

        return self._envs

    def set_environment(self, env: dict[str, str | None]):
        """Set environment for workload."""
        merged_envs = self.envs | env

        self._envs = {k: v for k, v in merged_envs.items() if v is not None}

        self.write("\n".join(self.to_env(self.envs)), self.ENV_FILE)



class K8sUtils(WithLogging):
    """Class that encapsulates various utilities related to K8s."""

    def __init__(
        self, kube_config: None | str | dict = None
    ):
        self.kube_config = kube_config
        self.client = Client(config=K8sUtils._parsed_kube_config(kube_config))

    @staticmethod
    def _parsed_kube_config(kube_config: None | str | dict) -> KubeConfig:
        """Return the kube config file parsed as a dictionary"""
        if not kube_config:
            return KubeConfig.from_env()

        if isinstance(kube_config, str):
            return KubeConfig.from_file(kube_config)
        elif isinstance(kube_config, dict):
            return KubeConfig.from_dict(kube_config)
        else:
            raise ValueError(
                f"malformed kube_config: type {type(kube_config)}"
            )

    @property
    def default_namespace(self):
        return self.client.namespace

    def is_namespace_valid(self, namespace: str):
        """Return whether given namespace exists in K8s cluster."""
        try:
            self.client.get(Namespace, name=namespace)
        except ApiError:
            return False
        return True

    def is_service_account_valid(self, service_account: str, namespace: None | str):
        """Return whether given service account in the given namespace exists in K8s cluster."""
        try:
            self.client.get(ServiceAccount, name=service_account, namespace=namespace or self.default_namespace)
        except ApiError:
            return False
        return True


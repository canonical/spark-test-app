# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for Juju integration tests."""

from collections.abc import Iterator
from pathlib import Path
from platform import machine

import jubilant
import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--keep-models", action="store_true", help="Keep the temporary Juju model")
    parser.addoption("--model", help="Use an existing Juju Kubernetes model")


@pytest.fixture(scope="module")
def architecture() -> str:
    architectures = {"x86_64": "amd64", "aarch64": "arm64"}
    host = machine()
    if host not in architectures:
        pytest.fail(f"Unsupported test host architecture: {host}")
    return architectures[host]


@pytest.fixture(scope="module")
def packed_charm(architecture: str) -> Path:
    pattern = f"spark-test-app_*-{architecture}.charm"
    charms = sorted(Path.cwd().glob(pattern))
    if len(charms) != 1:
        pytest.fail(
            f"Expected one {pattern} artifact; found {len(charms)}. Run tox -e build-charms."
        )
    return charms[0]


@pytest.fixture(scope="module")
def juju(
    pytestconfig: pytest.Config, architecture: str, packed_charm: Path
) -> Iterator[jubilant.Juju]:
    if model_name := pytestconfig.getoption("--model"):
        model = jubilant.Juju(model=model_name, wait_timeout=600)
        model.status()
        yield model
        return

    with jubilant.temp_model(keep=pytestconfig.getoption("--keep-models")) as model:
        model.wait_timeout = 600
        model.cli("set-model-constraints", f"arch={architecture}")
        yield model

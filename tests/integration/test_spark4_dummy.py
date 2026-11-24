# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Smoke test for the dummy job with Spark 4.0 images."""

import re
from pathlib import Path

import jubilant
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

APP = "spark-test-app"
HUB = "spark-integration-hub-k8s"
WORKLOAD_IMAGE = (
    "ghcr.io/canonical/charmed-spark-kyuubi"
    "@sha256:533273d788cb9726586b5cf5297ee670b018e40e137bcf8f34f661b6fe74544a"
)
JOB_IMAGE = (
    "ghcr.io/canonical/charmed-spark"
    "@sha256:0505173879d2ed4ab103d860364aa759f13d7a29de33c89de02d47d431c54125"
)


def test_spark4_dummy_job(juju: jubilant.Juju, packed_charm: Path) -> None:
    """Run the dummy count job through Juju and verify its Spark version and output."""
    assert juju.model is not None
    juju.deploy(HUB, channel="3/stable", trust=True)

    juju.deploy(
        packed_charm,
        APP,
        trust=True,
        resources={"spark-image": WORKLOAD_IMAGE},
        config={
            "spark-image": JOB_IMAGE,
            "namespace": juju.model.rsplit(":", 1)[-1].rsplit("/", 1)[-1],
            "flavour": "dummy",
            "deploy-mode": "client",
        },
    )
    juju.integrate(f"{APP}:spark-service-account", f"{HUB}:spark-service-account")
    juju.wait(lambda status: jubilant.all_active(status, APP) and jubilant.all_agents_idle(status))

    unit = f"{APP}/0"
    juju.run(unit, "start-process", wait=120)

    for attempt in Retrying(
        retry=retry_if_exception_type(AssertionError),
        stop=stop_after_delay(300),
        wait=wait_fixed(5),
        reraise=True,
    ):
        with attempt:
            logs = juju.ssh(
                unit,
                "/charm/bin/pebble",
                "logs",
                "-n",
                "all",
                "spark-job",
                container="spark",
            )
            assert re.search(r"Total count: 100\s*$", logs, re.MULTILINE), logs
            assert re.search(r"Running Spark version 4\.0\.\d+\b", logs), logs

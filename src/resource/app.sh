#!/bin/bash
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

echo "Username: ${SPARK_USER}"
echo "Namespace: ${SPARK_NAMESPACE}"
echo "Args: ${EXTRA_ARGS}"
echo "Script: ${SCRIPT}"

spark-client.spark-submit --username ${SPARK_USER} --namespace ${SPARK_NAMESPACE} ${EXTRA_ARGS} ${SCRIPT}
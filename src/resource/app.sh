#!/bin/bash

echo "Username: ${SPARK_USER}"
echo "Namespace: ${SPARK_NAMESPACE}"
echo "Args: ${EXTRA_ARGS}"
echo "Script: ${SCRIPT}"

spark-client.spark-submit --username ${SPARK_USER} --namespace ${SPARK_NAMESPACE} ${EXTRA_ARGS} /var/lib/spark/${SCRIPT}
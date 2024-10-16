#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Spark application integrated with a Kafka topic."""

import os
import uuid
from json import loads

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, window

random_id = uuid.uuid4()

# Create a Spark Session
spark = SparkSession.builder.appName("SparkStreaming").enableHiveSupport().getOrCreate()

# Input
username = os.environ.get("KAFKA_USERNAME", "")
password = os.environ.get("KAFKA_PASSWORD", "")
endpoints = os.environ.get("KAFKA_ENDPOINTS", "")
topic_name = os.environ.get("KAFKA_TOPIC", "")

if any(item == "" for item in [username, password, endpoints, topic_name]):
    raise ValueError("Input malformed")

# Output
table_name = os.environ.get("HIVE_TABLE", "")

lines = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", endpoints)
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option(
        "kafka.sasl.jaas.config",
        f"org.apache.kafka.common.security.scram.ScramLoginModule required username={username} password={password};",
    )
    .option("subscribe", topic_name)
    .option("includeHeaders", "true")
    .load()
)

get_origin = udf(lambda x: loads(x)["origin"])

count = (
    lines.withColumn("origin", get_origin(col("value")))
    .select("origin", "timestamp")
    .withWatermark("timestamp", "10 seconds")
    .groupBy(window("timestamp", "10 seconds"), "origin")
    .count()
)

if table_name:
    confs = dict(spark.sparkContext.getConf().getAll())
    s3_bucket = confs["spark.kubernetes.file.upload.path"]

    print(f"Using bucket: {s3_bucket}")

    query = (
        count.writeStream.option("checkpointLocation", f"{s3_bucket}/checkpoints/{random_id}")
        .format("parquet")
        .outputMode("append")
        .toTable(table_name)
    )
else:
    query = count.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()

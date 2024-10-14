#!/usr/bin/env python3
#
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Simple Spark dummy application to be used for testing."""

from pyspark import SparkConf, SparkContext

# Set up Spark configuration
conf = SparkConf().setAppName("MyApp")
sc = SparkContext(conf=conf)

rdd = sc.parallelize(range(100))

print(f"Total count: {rdd.count()}")

sc.stop()

from pyspark import SparkContext, SparkConf

# Set up Spark configuration
conf = SparkConf().setAppName("MyApp")
sc = SparkContext(conf=conf)

rdd = sc.parallelize(range(100))

print(f"Total count: {rdd.count()}")

sc.stop()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OlympicsExtraction") \
    .master("local[4]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.driver.memory", "2g") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "100s") \
    .getOrCreate()
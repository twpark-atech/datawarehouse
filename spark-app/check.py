from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckGold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniostorage") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df = spark.read.parquet("s3a://bis/bus-arrival-info/bronze/")
df.show(truncate=False)

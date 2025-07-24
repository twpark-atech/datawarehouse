from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, IntegerType, StringType

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("KafkaMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniostorage") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
# 로그 레벨 설정
spark.sparkContext.setLogLevel("WARN")
# 스키마 생성
schema = StructType() \
    .add("userId", IntegerType()) \
    .add("id", IntegerType()) \
    .add("title", StringType()) \
    .add("body", StringType())
# Spark 스트림 불러와 JSON 파싱
df_kakfa = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()
df_value = df_kakfa.selectExpr("CAST(value AS STRING)")
df_parsed = df_value.select(from_json(col("value"), schema).alias("data")).select("data.*")
# query 저장
query = df_parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint_kafka_minio") \
    .option("path", "s3a://my-bucket/data/") \
    .outputMode("append") \
    .start()
# 실행
query.awaitTermination()
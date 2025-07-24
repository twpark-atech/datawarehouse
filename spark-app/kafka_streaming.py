from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("KafkaStreamingApp").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Kafka 연결
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()
# key/value를 문자열로 변환
df = df_raw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# JSON 파싱 (선택)
schema = StructType() \
    .add("userId", IntegerType()) \
    .add("id", IntegerType()) \
    .add("title", StringType()) \
    .add("body", StringType())
df_parsed = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
# 콘솔 출력
query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
query.awaitTermination()

# silver.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("TransformSilver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniostorage") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
# 로그 레벨 설정
spark.sparkContext.setLogLevel("WARN")
# Bronze 데이터 읽기
bronze_path="s3a://bis/bus-arrival-info/bronze/"
df_bronze = spark.read.parquet(bronze_path)
# 정제 (타입 캐스팅, Null 제거, 중복 제거)
df_silver = df_bronze \
    .filter(col("ARRIVALTIME").isNotNull() & col("ROUTENM").isNotNull()) \
    .withColumn("RNUM", col("RNUM").cast("int")) \
    .withColumn("PREVSTOPCNT", col("PREVSTOPCNT").cast("int")) \
    .withColumn("ARRIVALTIME", col("ARRIVALTIME").cast("int")) \
    .withColumn("ROUTEID", col("ROUTEID").cast("int")) \
    .withColumn("STOPID", col("STOPID").cast("int")) \
    .dropDuplicates()
# Silver로 저장
silver_path="s3a://bis/bus-arrival-info/silver/"
df_silver.write.mode("overwrite").parquet(silver_path)
spark.stop()
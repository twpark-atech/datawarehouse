# load_to_postgres.py
from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("GoldToPostgres") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniostorage") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
# 로그 레벨 설정
spark.sparkContext.setLogLevel("WARN")
# Gold 데이터 읽기
gold_path="s3a://bis/bus-arrival-info/gold/"
df_gold = spark.read.parquet(gold_path)
df_gold.printSchema()
df_gold.show(5)
# PostgrSQL 적재
df_gold.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/datawarehouse") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "bus_arrival_info") \
    .option("user", "admin_dw") \
    .option("password", "admin123") \
    .mode("overwrite") \
    .save()
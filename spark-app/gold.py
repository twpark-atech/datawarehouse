# gold.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("AggregateGold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniostorage") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
# 로그 레벨 설정
spark.sparkContext.setLogLevel("WARN")
# Silver 데이터 읽기
silver_path="s3a://bis/bus-arrival-info/silver/"
df_silver = spark.read.parquet(silver_path)
# 통계치 (정류장 기준으로 평균값 집계)
df_gold = df_silver.groupBy("ROUTENM", "PRESENTSTOPNM") \
    .agg(
        round(avg("ARRIVALTIME"), 1).alias("avg_arrival_time"),
        round(avg("PREVSTOPCNT"), 1).alias("avg_prev_stop_cnt"),
        count("*").alias("record_count")
    )
# Gold로 저장
gold_path="s3a://bis/bus-arrival-info/gold/"
df_gold.write.mode("overwrite").parquet(gold_path)
spark.stop()
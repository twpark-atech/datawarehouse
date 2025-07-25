# bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType

# 콘솔 출력 및 저장 함수
def process_batch(df, epoch_id):
    # 콘솔 출력
    df.show(truncate=False)
    # MinIO 저장
    df.write \
        .mode("append") \
        .format("parquet") \
        .option("path", "s3a://bis/bus-arrival-info/bronze") \
        .save()

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("IngestBronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "miniostorage") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
# 로그 레벨 설정
spark.sparkContext.setLogLevel("WARN")
# 스키마 생성
schema = StructType([
    StructField("tableInfo", StructType([
        StructField("pageNo", StringType()),
        StructField("numOfRows", StringType()),
        StructField("totalCnt", StringType()),
        StructField("resultCode", StringType()),
        StructField("resultMsg", StringType()),
        StructField("list", StructType([
            StructField("row", ArrayType(
                StructType([
                    StructField("VEHICLENO", StringType()),
                    StructField("RNUM", StringType()),
                    StructField("PREVSTOPCNT", StringType()),
                    StructField("ARRIVALTIME", StringType()),
                    StructField("ROUTEID", StringType()),
                    StructField("REMARK", StringType()),
                    StructField("STOPID", StringType()),
                    StructField("STOPNM", StringType()),
                    StructField("PRESENTSTOPNM", StringType()),
                    StructField("ROUTENM", StringType())
                ])
            ))
        ]))
    ]))
])

# Kafka readStream
df_kakfa = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'kafka:29092') \
    .option("subscribe", 'BIS-BUS-ARRIVAL-INFO') \
    .option("startingOffsets", "earliest") \
    .load()
# value: binary -> String -> JSON array 파싱 -> explode
df_value = df_kakfa.selectExpr("CAST(value AS STRING) as json_str")
# JSON list 파싱
df_parsed = df_value.withColumn("parsed", from_json("json_str", schema))
# explode 배열 -> 행별 레코드
df_row = df_parsed.selectExpr("parsed.tableInfo.list.row as rows").selectExpr("explode(rows) as row").select("row.*")
# Minio 저장
query = df_row.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()
query.awaitTermination(timeout=5)
query.stop() 
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, from_unixtime, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# --- 환경 변수 설정 ---
bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("TOPIC_RAW", "bike_raw")
# Docker Compose에서 설정한 LOCAL_BASE_PATH=/data 경로 사용
base_path = os.getenv("LOCAL_BASE_PATH", "/data") 

out_path = f"{base_path}/curated"
ckpt_path = f"{base_path}/checkpoints/curate"

# --- Spark 세션 생성 ---
spark = (
    SparkSession.builder
    .appName("seoul-bike-curator")
    .config("spark.sql.shuffle.partitions", "5")  # 로컬 테스트용 성능 최적화
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --- 스키마 정의 (API 구조 반영) ---
# 1. API 내부 리스트 요소 스키마
api_row_schema = StructType([
    StructField("stationName", StringType(), True),
    StructField("parkingBikeTotCnt", StringType(), True),
    StructField("stationLatitude", StringType(), True),
    StructField("stationLongitude", StringType(), True),
    StructField("stationId", StringType(), True)
])

# 2. Payload 스키마 (rentBikeStatus -> row 구조)
payload_schema = StructType([
    StructField("rentBikeStatus", StructType([
        StructField("row", ArrayType(api_row_schema), True)
    ]), True)
])

# 3. 전체 Kafka 메시지 스키마
kafka_schema = StructType([
    StructField("fetched_at", LongType(), True),
    StructField("source", StringType(), True),
    StructField("payload", payload_schema, True),
])

# --- 스트리밍 로직 ---
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest") # 놓친 데이터도 다 읽기
    .option("failOnDataLoss", "false")
    .load()
)

# JSON 파싱 및 데이터 평탄화 (Flatten)
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), kafka_schema).alias("data"))
    .select(
        col("data.fetched_at"),
        col("data.source"),
        explode(col("data.payload.rentBikeStatus.row")).alias("row") # 배열 폭파
    )
)

final_df = (
    parsed_df
    .withColumn("event_time", from_unixtime(col("fetched_at")).cast("timestamp"))
    .withColumn("event_date", to_date(col("event_time")))
    .select(
        "event_date",
        "event_time",
        col("row.stationId").alias("station_id"),
        col("row.stationName").alias("station_name"),
        col("row.parkingBikeTotCnt").cast("int").alias("bike_count"),
        col("row.stationLatitude").cast("double").alias("lat"),
        col("row.stationLongitude").cast("double").alias("lon")
    )
)

# --- ✅ 파일 저장 (JSON) ---
query = (
    final_df.writeStream
    .format("json")  # JSON 포맷 지정
    .option("path", out_path)
    .option("checkpointLocation", ckpt_path)
    .partitionBy("event_date")  # 날짜별 폴더 생성
    .outputMode("append")
    .start()
)

query.awaitTermination()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic_raw = os.getenv("TOPIC_RAW", "bike_raw")

base_path = os.getenv("LOCAL_BASE_PATH", "/data")
out_path = f"{base_path}/curated"
ckpt_path = f"{base_path}/checkpoints/curate"

# collector가 넣는 raw 이벤트 스키마 (payload는 일단 통째로 보관)
raw_schema = StructType([
    StructField("fetched_at", LongType(), True),
    StructField("source", StringType(), True),
    StructField("payload", StructType([]), True),
])

spark = (
    SparkSession.builder
    .appName("seoul-bike-curate-to-local")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic_raw)
    .option("startingOffsets", "latest")
    .load()
)

raw_json = raw_kafka.selectExpr("CAST(value AS STRING) AS v")

parsed = raw_json.select(from_json(col("v"), raw_schema).alias("e")).select("e.*")

# ✅ 로컬 저장을 위해 최소한의 컬럼만 정리 + 날짜 파티션 컬럼 생성
curated = (
    parsed
    .withColumn("event_time", from_unixtime(col("fetched_at")).cast("timestamp"))
    .withColumn("event_date", to_date(col("event_time")))
    .select("event_date", "event_time", "source", col("payload").alias("raw_payload"))
)

# ✅ JSON Lines로 저장(가장 단순). 나중에 Parquet로 바꾸면 됨.
query = (
    curated.writeStream
    .format("json")
    .option("path", out_path)
    .option("checkpointLocation", ckpt_path)
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

query.awaitTermination()

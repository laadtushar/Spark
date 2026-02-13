from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, window, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BooleanType,
    ArrayType,
)
import os

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "job.postings.v1"

# Schema for incoming Kafka payload
schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("source", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("job_id", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("company", StringType(), True),
                    StructField("location", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("is_remote", BooleanType(), True),
                ]
            ),
            True,
        ),
    ]
)

# Taxonomy for skill extraction (Broadcast)
SKILL_TAXONOMY = [
    "python",
    "spark",
    "kafka",
    "react",
    "aws",
    "kubernetes",
    "docker",
    "sql",
    "java",
    "golang",
]


def main():
    spark = (
        SparkSession.builder.appName("GlobalTalentPulse-Processor")
        .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints")
        .getOrCreate()
    )

    # Broadcast skill list
    broadcast_skills = spark.sparkContext.broadcast(SKILL_TAXONOMY)

    @udf(returnType=ArrayType(StringType()))
    def extract_skills(description):
        if not description:
            return []
        found = []
        desc_lower = description.lower()
        for skill in broadcast_skills.value:
            if skill in desc_lower:
                found.append(skill)
        return found

    # Read from Kafka
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON
    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # Flatten and Enrich
    enriched_df = parsed_df.select(
        col("event_id"),
        col("timestamp"),
        col("payload.title").alias("title"),
        col("payload.location").alias("location"),
        extract_skills(col("payload.description")).alias("skills"),
    )

    # Function to write windowed aggregates to Redis
    def write_to_redis(batch_df, batch_id):
        import redis

        r = redis.Redis(host="redis", port=6379, decode_responses=True)
        # Assuming the batch_df has [skill, count]
        for row in batch_df.collect():
            r.set(f"trend:{row['skill']}", row["count"])

    # 1. Hot Path: Aggregations for Redis/Dashboard
    # Explode skills to count individual occurrences
    exploded_df = enriched_df.select(
        col("timestamp"), explode(col("skills")).alias("skill")
    )

    # Simple count per skill for the dashboard
    counts_df = exploded_df.groupBy("skill").count()

    # Writing to Redis via foreachBatch
    query_redis = (
        counts_df.writeStream.outputMode("complete")
        .foreachBatch(write_to_redis)
        .start()
    )

    # 2. Cold Path: Delta Lake
    # Note: Requires Delta Spark packages
    query_delta = (
        enriched_df.writeStream.format("parquet")
        .option("path", "/data/job_postings_pq")
        .option("checkpointLocation", "/data/checkpoints_delta")
        .partitionBy("location")
        .start()
    )

    query_redis.awaitTermination()
    query_delta.awaitTermination()


if __name__ == "__main__":
    main()

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
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GlobalTalentPulse")

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
    logger.info("Starting Spark job...")

    spark = (
        SparkSession.builder.appName("GlobalTalentPulse-Processor")
        .config("spark.sql.streaming.checkpointLocation", "/data/checkpoints")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

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
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
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

        try:
            row_count = batch_df.count()
            logger.info(f"--- Processing batch {batch_id} ---")
            logger.info(f"Batch Row Count: {row_count}")

            if row_count > 0:
                r = redis.Redis(host="redis", port=6379, decode_responses=True)
                r.ping()

                rows = batch_df.collect()

                # We'll use a pipeline for better performance
                pipe = r.pipeline()

                # Track global skills in this batch to update global keys
                global_skills = {}

                for row in rows:
                    row_dict = row.asDict()
                    location = row_dict.get("location")
                    skill = row_dict.get("skill")
                    count = row_dict.get("count")

                    if skill and count:
                        # 1. Store Location-specific trend
                        if location:
                            pipe.set(f"trend:loc:{location}:{skill}", str(count))

                        # 2. Aggregate for Global trend
                        global_skills[skill] = global_skills.get(skill, 0) + count

                # 3. Store Global trends
                for skill, count in global_skills.items():
                    pipe.set(f"trend:skill:{skill}", str(count))

                # 4. Total records processed (Cumulative approx)
                # Since we are in 'complete' mode, we can't easily get the per-batch increment
                # for a simple counter without a separate 'append' stream,
                # but we can sum all current counts for a total 'skill hits' metric.
                total_hits = sum(global_skills.values())
                pipe.set("stats:total_skill_hits", str(total_hits))

                pipe.execute()
                logger.info(
                    f"Successfully updated Redis with {len(rows)} location-skill pairs."
                )
            else:
                logger.info("Batch is empty, skipping Redis write.")
        except Exception as e:
            logger.error(f"CRITICAL error in write_to_redis: {str(e)}")
            import traceback

            traceback.print_exc()

    # 1. Hot Path: Aggregations for Redis/Dashboard
    # Include location in selection
    exploded_df = enriched_df.select(
        col("location"), col("timestamp"), explode(col("skills")).alias("skill")
    )

    # Count per skill AND location
    # complete mode will keep the full history of counts in memory (state)
    counts_df = exploded_df.groupBy("location", "skill").count()

    # Writing to Redis via foreachBatch
    query_redis = (
        counts_df.writeStream.outputMode("complete")
        .foreachBatch(write_to_redis)
        .start()
    )

    # 2. Cold Path: Parquet storage
    logger.info("Initializing Cold Path (Parquet)...")
    query_delta = (
        enriched_df.writeStream.format("parquet")
        .option("path", "/data/job_postings_pq")
        .option("checkpointLocation", "/data/checkpoints_delta")
        .partitionBy("location")
        .start()
    )

    logger.info("Awaiting termination of streams...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

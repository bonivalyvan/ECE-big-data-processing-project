"""Identify IMDb-related pages in the live Wikipedia changes Kafka stream."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyspark
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


TOPIC = "wikipedia-recent-changes"
WATERMARK_DELAY = "10 minutes"
EVENT_SCHEMA = T.StructType(
    [
        T.StructField(
            "meta",
            T.StructType(
                [
                    T.StructField("id", T.StringType(), True),
                    T.StructField("dt", T.StringType(), True),
                    T.StructField("uri", T.StringType(), True),
                    T.StructField("domain", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField("id", T.LongType(), True),
        T.StructField("type", T.StringType(), True),
        T.StructField("namespace", T.IntegerType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("title_url", T.StringType(), True),
        T.StructField("timestamp", T.LongType(), True),
        T.StructField("user", T.StringType(), True),
        T.StructField("bot", T.BooleanType(), True),
        T.StructField("wiki", T.StringType(), True),
        T.StructField(
            "revision",
            T.StructType(
                [
                    T.StructField("old", T.LongType(), True),
                    T.StructField("new", T.LongType(), True),
                ]
            ),
            True,
        ),
    ]
)


def parse_kafka_events(kafka_events: DataFrame) -> DataFrame:
    parsed = kafka_events.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")
    )
    return parsed.select(
        F.col("event.meta.id").alias("event_id"),
        F.to_timestamp(F.col("event.meta.dt")).alias("event_time"),
        F.col("event.wiki").alias("wiki"),
        F.col("event.id").alias("page_id"),
        F.col("event.namespace").alias("namespace"),
        F.col("event.title").alias("title"),
        F.col("event.type").alias("change_type"),
        F.col("event.user").alias("user"),
        F.col("event.bot").alias("is_bot"),
        F.col("event.revision.old").alias("revision_old"),
        F.col("event.revision.new").alias("revision_new"),
        F.coalesce(F.col("event.title_url"), F.col("event.meta.uri")).alias(
            "source_url"
        ),
    )


def filter_candidate_changes(events: DataFrame) -> DataFrame:
    return events.where(
        (F.col("wiki") == "enwiki")
        & (F.col("namespace") == 0)
        & F.col("change_type").isin("edit", "new")
        & F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("page_id").isNotNull()
        & F.col("title").isNotNull()
    )


def build_imdb_target_index(films: DataFrame, principals: DataFrame) -> DataFrame:
    film_targets = films.select(
        F.col("wikipedia_page_id").cast("long").alias("page_id"),
        F.struct(
            F.lit("film").alias("entity_type"),
            F.col("tconst").alias("imdb_id"),
            F.col("title").alias("entity_title"),
        ).alias("imdb_target"),
    )
    principal_targets = principals.select(
        F.col("wikipedia_page_id").cast("long").alias("page_id"),
        F.struct(
            F.lit("principal").alias("entity_type"),
            F.col("nconst").alias("imdb_id"),
            F.col("principal_name").alias("entity_title"),
        ).alias("imdb_target"),
    )
    return (
        film_targets.unionByName(principal_targets)
        .where(F.col("page_id").isNotNull())
        .groupBy("page_id")
        .agg(F.collect_set("imdb_target").alias("imdb_targets"))
    )


def annotate_imdb_matches(events: DataFrame, target_index: DataFrame) -> DataFrame:
    return (
        events.join(F.broadcast(target_index), "page_id", "left")
        .withColumn("is_imdb_target", F.col("imdb_targets").isNotNull())
    )


def _read_imdb_target_index(
    spark: SparkSession, output_dir: Path
) -> DataFrame:
    film_path = output_dir / "films"
    principal_path = output_dir / "principals"
    missing = [
        str(path) for path in (film_path, principal_path) if not path.is_dir()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing finalized Wikipedia-enriched IMDb outputs:\n  "
            + "\n  ".join(missing)
            + "\nRun src.enrich_data finalize first."
        )
    films = spark.read.parquet(str(film_path))
    principals = spark.read.parquet(str(principal_path))
    return build_imdb_target_index(films, principals)


def run_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    output_dir: Path,
    checkpoint_dir: Path,
    imdb_output_dir: Path,
    starting_offsets: str = "latest",
) -> None:
    target_index = _read_imdb_target_index(spark, imdb_output_dir).persist()
    target_index.count()
    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    candidates = (
        filter_candidate_changes(parse_kafka_events(kafka_events))
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    identified = annotate_imdb_matches(candidates, target_index)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        batch.persist()
        try:
            metrics = batch.agg(
                F.count("*").alias("candidate_changes"),
                F.sum(F.col("is_imdb_target").cast("long")).alias(
                    "imdb_target_matches"
                ),
            ).first()
            candidate_count = metrics["candidate_changes"]
            imdb_match_count = metrics["imdb_target_matches"] or 0
            print(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "candidate_changes": candidate_count,
                        "imdb_target_matches": imdb_match_count,
                    }
                ),
                flush=True,
            )
            if candidate_count:
                batch.write.mode("append").parquet(str(output_dir))
        finally:
            batch.unpersist()

    query = (
        identified.writeStream.outputMode("append")
        .option("checkpointLocation", str(checkpoint_dir))
        .foreachBatch(write_batch)
        .trigger(processingTime="5 seconds")
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        target_index.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9094",
        help="Comma-separated Kafka bootstrap servers.",
    )
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/streaming/processed/changes")
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        default=Path("data/streaming/checkpoints/wikipedia-changes-v4"),
    )
    parser.add_argument(
        "--imdb-output-dir",
        type=Path,
        default=Path("data/enrichment/processed"),
    )
    parser.add_argument(
        "--starting-offsets", choices=("earliest", "latest"), default="latest"
    )
    args = parser.parse_args()

    package_version = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    spark = (
        SparkSession.builder.appName("wikipedia-imdb-change-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", package_version)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_stream(
            spark,
            args.bootstrap_servers,
            args.topic,
            args.output_dir,
            args.checkpoint_dir,
            args.imdb_output_dir,
            args.starting_offsets,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

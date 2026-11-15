"""Select IMDb enrichment targets and finalize Wikipedia records with Spark."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T


FIRST_YEAR = 1980
LAST_YEAR = 2025
TOP_FILMS_PER_GENRE_YEAR = 10
PRINCIPALS_PER_FILM = 3

SELECTION_SCHEMA = T.StructType(
    [
        T.StructField("genre", T.StringType(), True),
        T.StructField("rank", T.IntegerType(), True),
    ]
)
FILM_TARGET_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("startYear", T.IntegerType(), True),
        T.StructField("numVotes", T.LongType(), True),
        T.StructField("averageRating", T.DoubleType(), True),
        T.StructField(
            "selected_for", T.ArrayType(SELECTION_SCHEMA, containsNull=False), True
        ),
    ]
)
PRINCIPAL_TARGET_SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("film_title", T.StringType(), True),
        T.StructField("nconst", T.StringType(), True),
        T.StructField("display_name", T.StringType(), True),
        T.StructField("birthYear", T.IntegerType(), True),
        T.StructField("ordering", T.IntegerType(), True),
        T.StructField("category", T.StringType(), True),
    ]
)


def build_enrichment_targets(
    titles: DataFrame,
    ratings: DataFrame,
    principals: DataFrame,
    people: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Choose the ten most-voted feature films per genre/year and three credits."""
    genre_films = (
        titles.where(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= 60)
            & F.col("startYear").between(FIRST_YEAR, LAST_YEAR)
            & F.col("genres").isNotNull()
        )
        .join(
            ratings.select("tconst", "numVotes", "averageRating"),
            "tconst",
            "inner",
        )
        .where(F.col("numVotes").isNotNull() & (F.col("numVotes") >= 0))
        .withColumn(
            "genre",
            F.explode(
                F.array_distinct(
                    F.transform(F.split(F.col("genres"), ","), lambda value: F.trim(value))
                )
            ),
        )
        .where(F.col("genre").isNotNull() & (F.col("genre") != ""))
    )
    rank_window = Window.partitionBy("startYear", "genre").orderBy(
        F.col("numVotes").desc(),
        F.col("averageRating").desc_nulls_last(),
        F.col("tconst").asc(),
    )
    selected = (
        genre_films.withColumn("genre_rank", F.row_number().over(rank_window))
        .where(F.col("genre_rank") <= TOP_FILMS_PER_GENRE_YEAR)
    )
    film_targets = selected.groupBy(
        "tconst",
        F.col("primaryTitle").alias("title"),
        "startYear",
        "numVotes",
        "averageRating",
    ).agg(
        F.sort_array(
            F.collect_list(
                F.struct(F.col("genre").alias("genre"), F.col("genre_rank").alias("rank"))
            )
        ).alias("selected_for")
    )

    unique_credit_window = Window.partitionBy("tconst", "nconst").orderBy(
        F.col("ordering").asc_nulls_last(),
        F.col("category").asc_nulls_last(),
    )
    unique_principals = (
        principals.where(F.col("nconst").isNotNull() & (F.col("nconst") != ""))
        .withColumn("_credit_rank", F.row_number().over(unique_credit_window))
        .where(F.col("_credit_rank") == 1)
        .drop("_credit_rank")
    )
    principal_window = Window.partitionBy("tconst").orderBy(
        F.col("ordering").asc_nulls_last(),
        F.col("nconst").asc(),
    )
    principal_targets = (
        unique_principals.join(
            film_targets.select("tconst", F.col("title").alias("film_title")),
            "tconst",
            "inner",
        )
        .withColumn("principal_rank", F.row_number().over(principal_window))
        .where(F.col("principal_rank") <= PRINCIPALS_PER_FILM)
        .join(
            people.select("nconst", "primaryName", "birthYear"),
            "nconst",
            "left",
        )
        .select(
            "tconst",
            "film_title",
            "nconst",
            F.col("primaryName").alias("display_name"),
            "birthYear",
            "ordering",
            "category",
        )
    )
    return film_targets, principal_targets

def run_target_selection(
    spark: SparkSession,
    input_dir: Path,
    target_dir: Path,
    overwrite: bool = False,
) -> tuple[int, int]:
    titles, ratings, principals, people = _read_prepared_tables(spark, input_dir)
    film_targets, principal_targets = build_enrichment_targets(
        titles, ratings, principals, people
    )
    destinations = [target_dir / "films", target_dir / "principals"]
    existing = [str(path) for path in destinations if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Enrichment target path(s) already exist; pass --overwrite to replace them:\n  "
            + "\n  ".join(existing)
        )

    film_targets.persist()
    principal_targets.persist()
    try:
        film_count = film_targets.count()
        principal_count = principal_targets.count()
        unique_principal_count = principal_targets.select("nconst").distinct().count()
        mode = "overwrite" if overwrite else "error"
        film_targets.write.mode(mode).json(str(destinations[0]))
        principal_targets.write.mode(mode).json(str(destinations[1]))
    finally:
        principal_targets.unpersist()
        film_targets.unpersist()
    print(f"Selected films: {film_count}")
    print(f"Selected film-principal records: {principal_count}")
    print(f"Unique Wikipedia principal targets: {unique_principal_count}")
    print(f"Target manifests: {target_dir}")
    return film_count, principal_count


def _latest_wikipedia_records(records: DataFrame) -> DataFrame:
    latest = Window.partitionBy("entity_type", "imdb_id").orderBy(
        F.col("retrieved_at").desc_nulls_last()
    )
    return (
        records.withColumn("_record_rank", F.row_number().over(latest))
        .where(F.col("_record_rank") == 1)
        .drop("_record_rank")
    )


def _assert_collection_complete(
    film_targets: DataFrame, principal_targets: DataFrame, records: DataFrame
) -> None:
    films = records.where(F.col("entity_type") == "film").select("imdb_id").distinct()
    people = (
        records.where(F.col("entity_type") == "principal")
        .select("imdb_id")
        .distinct()
    )
    missing_films = (
        film_targets.select(F.col("tconst").alias("imdb_id"))
        .join(films, "imdb_id", "left_anti")
        .count()
    )
    missing_people = (
        principal_targets.select(F.col("nconst").alias("imdb_id"))
        .join(people, "imdb_id", "left_anti")
        .count()
    )
    invalid_lineage = records.where(
        F.col("source_url").isNull()
        | (F.trim(F.col("source_url")) == "")
        | F.col("source_type").isNull()
        | (F.trim(F.col("source_type")) == "")
        | F.col("status").isNull()
        | (F.trim(F.col("status")) == "")
    ).count()
    if missing_films or missing_people or invalid_lineage:
        raise ValueError(
            "Wikipedia collection is incomplete: "
            f"{missing_films} film and {missing_people} principal targets have no record. "
            f"{invalid_lineage} records lack source lineage or status. "
            "Run src.collect_wikipedia to resume collection or repair invalid records."
        )


def build_enriched_outputs(
    film_targets: DataFrame,
    principal_targets: DataFrame,
    records: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    latest = _latest_wikipedia_records(records)
    film_records = latest.where(F.col("entity_type") == "film").alias("wiki")
    films = (
        film_targets.alias("target")
        .join(
            film_records,
            F.col("target.tconst") == F.col("wiki.imdb_id"),
            "left",
        )
        .select(
            F.col("target.tconst").alias("tconst"),
            F.col("target.title").alias("title"),
            F.col("target.startYear").alias("startYear"),
            F.col("target.numVotes").alias("numVotes"),
            F.col("target.averageRating").alias("averageRating"),
            F.col("target.selected_for").alias("selected_for"),
            F.col("wiki.content").alias("synopsis"),
            F.col("wiki.source_url").alias("source_url"),
            F.col("wiki.source_type").alias("source_type"),
            F.col("wiki.page_title").alias("wikipedia_page_title"),
            F.col("wiki.page_id").alias("wikipedia_page_id"),
            F.col("wiki.revision_id").alias("wikipedia_revision_id"),
            F.col("wiki.retrieved_at").alias("retrieved_at"),
            F.col("wiki.status").alias("enrichment_status"),
            F.col("wiki.note").alias("enrichment_note"),
            F.coalesce(
                F.col("wiki.attribution"),
                F.when(
                    F.col("wiki.status") == "found",
                    F.lit("Wikipedia contributors"),
                ),
            ).alias("attribution"),
            F.coalesce(
                F.col("wiki.license"),
                F.when(
                    F.col("wiki.status") == "found",
                    F.lit("CC BY-SA 4.0"),
                ),
            ).alias("license"),
            F.coalesce(
                F.col("wiki.license_url"),
                F.when(
                    F.col("wiki.status") == "found",
                    F.lit("https://creativecommons.org/licenses/by-sa/4.0/"),
                ),
            ).alias("license_url"),
        )
        .withColumn(
            "synopsis",
            F.when(F.trim(F.col("synopsis")) == "", F.lit(None).cast("string"))
            .otherwise(F.trim(F.col("synopsis"))),
        )
    )
    principal_records = latest.where(F.col("entity_type") == "principal").alias("wiki")
    enriched_principals = (
        principal_targets.alias("target")
        .join(
            principal_records,
            F.col("target.nconst") == F.col("wiki.imdb_id"),
            "left",
        )
        .select(
            F.col("target.tconst").alias("tconst"),
            F.col("target.film_title").alias("film_title"),
            F.col("target.nconst").alias("nconst"),
            F.col("target.display_name").alias("principal_name"),
            F.col("target.birthYear").alias("birthYear"),
            F.col("target.ordering").alias("ordering"),
            F.col("target.category").alias("category"),
            F.col("wiki.content").alias("biography"),
            F.col("wiki.source_url").alias("source_url"),
            F.col("wiki.source_type").alias("source_type"),
            F.col("wiki.page_title").alias("wikipedia_page_title"),
            F.col("wiki.page_id").alias("wikipedia_page_id"),
            F.col("wiki.revision_id").alias("wikipedia_revision_id"),
            F.col("wiki.retrieved_at").alias("retrieved_at"),
            F.col("wiki.status").alias("enrichment_status"),
            F.col("wiki.note").alias("enrichment_note"),
            F.coalesce(
                F.col("wiki.attribution"),
                F.when(
                    F.col("wiki.status") == "found",
                    F.lit("Wikipedia contributors"),
                ),
            ).alias("attribution"),
            F.coalesce(
                F.col("wiki.license"),
                F.when(
                    F.col("wiki.status") == "found",
                    F.lit("CC BY-SA 4.0"),
                ),
            ).alias("license"),
            F.coalesce(
                F.col("wiki.license_url"),
                F.when(
                    F.col("wiki.status") == "found",
                    F.lit("https://creativecommons.org/licenses/by-sa/4.0/"),
                ),
            ).alias("license_url"),
        )
        .withColumn(
            "biography",
            F.when(F.trim(F.col("biography")) == "", F.lit(None).cast("string"))
            .otherwise(F.trim(F.col("biography"))),
        )
    )
    principal_counts = enriched_principals.groupBy("tconst").agg(
        F.countDistinct("nconst").alias("principal_target_count"),
        F.countDistinct(
            F.when(
                F.col("enrichment_status") == "found", F.col("nconst")
            )
        ).alias("biography_count"),
    )
    coverage = (
        films.select("tconst", "title", "startYear")
        .join(principal_counts, "tconst", "left")
        .fillna(
            {"principal_target_count": 0, "biography_count": 0},
            subset=["principal_target_count", "biography_count"],
        )
        .withColumn(
            "coverage_note",
            F.when(
                F.col("principal_target_count") < PRINCIPALS_PER_FILM,
                F.concat(
                    F.lit("IMDb provides only "),
                    F.col("principal_target_count").cast("string"),
                    F.lit(" principal credits; three were requested."),
                ),
            ).when(
                F.col("biography_count") < PRINCIPALS_PER_FILM,
                F.concat(
                    F.lit("Wikipedia biographies found for "),
                    F.col("biography_count").cast("string"),
                    F.lit(" of the three selected principals."),
                ),
            ),
        )
        .select(
            "tconst",
            "title",
            "startYear",
            "principal_target_count",
            "biography_count",
            "coverage_note",
        )
    )
    return films, enriched_principals, coverage


def run_finalization(
    spark: SparkSession,
    target_dir: Path,
    records_path: Path,
    output_dir: Path,
    report_dir: Path,
    overwrite: bool = False,
) -> None:
    film_path = target_dir / "films"
    principal_path = target_dir / "principals"
    if not film_path.is_dir() or not principal_path.is_dir():
        raise FileNotFoundError(
            f"Missing enrichment target manifests under {target_dir}; run target selection first."
        )
    if not records_path.is_file():
        raise FileNotFoundError(
            f"Missing Wikipedia records file: {records_path}; run src.collect_wikipedia first."
        )

    film_targets = spark.read.schema(FILM_TARGET_SCHEMA).json(str(film_path))
    principal_targets = spark.read.schema(PRINCIPAL_TARGET_SCHEMA).json(
        str(principal_path)
    )
    records = spark.read.schema(WIKIPEDIA_RECORD_SCHEMA).json(str(records_path))
    latest = _latest_wikipedia_records(records)
    _assert_collection_complete(film_targets, principal_targets, latest)
    films, principals, coverage = build_enriched_outputs(
        film_targets, principal_targets, latest
    )

    destinations = [
        output_dir / "films",
        output_dir / "principals",
        report_dir / "film_coverage",
    ]
    existing = [str(path) for path in destinations if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Enrichment output path(s) already exist; pass --overwrite to replace them:\n  "
            + "\n  ".join(existing)
        )
    mode = "overwrite" if overwrite else "error"
    films.write.mode(mode).parquet(str(destinations[0]))
    principals.write.mode(mode).parquet(str(destinations[1]))
    coverage.write.mode(mode).option("header", True).csv(str(destinations[2]))
    print(f"Enriched films: {destinations[0]}")
    print(f"Enriched principals: {destinations[1]}")
    print(f"Film coverage report: {destinations[2]}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("targets", "finalize"))
    parser.add_argument("--input-dir", type=Path, default=Path("data/processed"))
    parser.add_argument(
        "--target-dir", type=Path, default=Path("data/enrichment/targets")
    )
    parser.add_argument(
        "--records", type=Path, default=Path("data/enrichment/wikipedia_records.jsonl")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/enrichment/processed")
    )
    parser.add_argument(
        "--report-dir", type=Path, default=Path("data/reports/enrichment")
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-data-enrichment").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.stage == "targets":
            run_target_selection(
                spark, args.input_dir, args.target_dir, overwrite=args.overwrite
            )
        else:
            run_finalization(
                spark,
                args.target_dir,
                args.records,
                args.output_dir,
                args.report_dir,
                overwrite=args.overwrite,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

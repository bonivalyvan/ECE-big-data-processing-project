"""Analyze IMDb popularity and ratings by alternate-title region coverage."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


TABLES = ("title.akas", "title.basics", "title.ratings")
REPORTS = (
    "region_reach_overall",
    "region_reach_by_genre_decade",
    "region_reach_strata_comparison",
)
FIRST_YEAR = 1980
LAST_YEAR = 2025
MIN_FILMS_PER_STRATUM = 10


def build_region_reach_reports(
    akas: DataFrame, titles: DataFrame, ratings: DataFrame
) -> dict[str, DataFrame]:
    """Compare IMDb votes and ratings across title-aka region-count tiers."""
    region_counts = akas.groupBy("titleId").agg(
        F.countDistinct(
            F.when(
                F.col("region").isNotNull() & (F.trim("region") != ""),
                F.upper(F.trim("region")),
            )
        ).alias("region_count")
    )
    rated_films = (
        titles.where(
            (F.col("titleType") == "movie")
            & (F.col("runtimeMinutes") >= 60)
            & F.col("startYear").between(FIRST_YEAR, LAST_YEAR)
            & F.col("genres").isNotNull()
        )
        .join(
            ratings.select("tconst", "averageRating", "numVotes"),
            "tconst",
            "inner",
        )
        .where(
            F.col("averageRating").between(0.0, 10.0)
            & (F.col("numVotes") > 0)
        )
        .join(
            region_counts,
            F.col("tconst") == F.col("titleId"),
            "left",
        )
        .drop("titleId")
        .fillna({"region_count": 0})
        .withColumn(
            "coverage_tier",
            F.when(F.col("region_count") == 0, "no recorded region")
            .when(F.col("region_count") == 1, "1 region")
            .when(F.col("region_count") <= 4, "2-4 regions")
            .otherwise("5+ regions"),
        )
        .withColumn(
            "start_decade",
            (F.floor(F.col("startYear") / 10) * 10).cast("int"),
        )
        .withColumn(
            "genre",
            F.explode(
                F.array_distinct(
                    F.transform(
                        F.split(F.col("genres"), ","),
                        lambda value: F.trim(value),
                    )
                )
            ),
        )
        .where(F.col("genre").isNotNull() & (F.col("genre") != ""))
    )

    metrics = [
        F.countDistinct("tconst").alias("film_count"),
        F.avg("averageRating").alias("mean_rating"),
        F.expr("percentile_approx(averageRating, 0.5, 10000)").alias(
            "median_rating"
        ),
        F.expr("percentile_approx(numVotes, 0.5, 10000)").alias("median_votes"),
    ]
    overall = rated_films.groupBy("coverage_tier").agg(*metrics)
    overall = overall.withColumn(
        "tier_order",
        F.when(F.col("coverage_tier") == "no recorded region", 0)
        .when(F.col("coverage_tier") == "1 region", 1)
        .when(F.col("coverage_tier") == "2-4 regions", 2)
        .otherwise(3),
    ).orderBy("tier_order").drop("tier_order")

    stratified = rated_films.groupBy(
        "start_decade", "genre", "coverage_tier"
    ).agg(*metrics)
    comparison = (
        stratified.where(
            F.col("coverage_tier").isin("1 region", "5+ regions")
        )
        .groupBy("start_decade", "genre")
        .agg(
            F.max(
                F.when(F.col("coverage_tier") == "1 region", F.col("film_count"))
            ).alias("one_region_film_count"),
            F.max(
                F.when(F.col("coverage_tier") == "5+ regions", F.col("film_count"))
            ).alias("five_plus_region_film_count"),
            F.max(
                F.when(F.col("coverage_tier") == "1 region", F.col("median_votes"))
            ).alias("one_region_median_votes"),
            F.max(
                F.when(
                    F.col("coverage_tier") == "5+ regions",
                    F.col("median_votes"),
                )
            ).alias("five_plus_region_median_votes"),
            F.max(
                F.when(F.col("coverage_tier") == "1 region", F.col("median_rating"))
            ).alias("one_region_median_rating"),
            F.max(
                F.when(
                    F.col("coverage_tier") == "5+ regions",
                    F.col("median_rating"),
                )
            ).alias("five_plus_region_median_rating"),
        )
        .where(
            (F.col("one_region_film_count") >= MIN_FILMS_PER_STRATUM)
            & (F.col("five_plus_region_film_count") >= MIN_FILMS_PER_STRATUM)
        )
        .withColumn(
            "median_vote_ratio_5plus_vs_one",
            F.col("five_plus_region_median_votes")
            / F.col("one_region_median_votes"),
        )
        .withColumn(
            "median_rating_difference_5plus_minus_one",
            F.col("five_plus_region_median_rating")
            - F.col("one_region_median_rating"),
        )
        .orderBy("start_decade", "genre")
    )

    return {
        "region_reach_overall": overall,
        "region_reach_by_genre_decade": stratified.orderBy(
            "start_decade", "genre", "coverage_tier"
        ),
        "region_reach_strata_comparison": comparison,
    }


def run_open_question(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool = False,
) -> None:
    missing = [
        str(input_dir / table)
        for table in TABLES
        if not (input_dir / table).is_dir()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing prepared IMDb Parquet tables:\n  " + "\n  ".join(missing)
        )
    existing = [
        str(output_dir / report)
        for report in REPORTS
        if (output_dir / report).exists()
    ]
    if existing and not overwrite:
        raise FileExistsError(
            "Open-question output path(s) already exist; pass --overwrite to "
            "replace them:\n  " + "\n  ".join(existing)
        )

    reports = build_region_reach_reports(
        spark.read.parquet(str(input_dir / "title.akas")),
        spark.read.parquet(str(input_dir / "title.basics")),
        spark.read.parquet(str(input_dir / "title.ratings")),
    )
    mode = "overwrite" if overwrite else "error"
    for report, frame in reports.items():
        frame.write.mode(mode).option("header", True).csv(str(output_dir / report))
        print(f"Wrote {report}: {output_dir / report}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/processed"))
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/reports/open_question")
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing open-question report folders.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-open-question").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_open_question(spark, args.input_dir, args.output_dir, args.overwrite)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

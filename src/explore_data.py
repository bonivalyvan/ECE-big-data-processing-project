"""Run the IMDb batch exploration queries with Apache Spark."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window, functions as F


TABLES = (
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",
    "name.basics",
)

REPORTS = (
    "runtime_outliers_by_genre",
    "genre_popularity_by_year",
    "top_three_genres_by_year",
    "top_three_vs_rest_by_year",
    "directors_one_high_rating_and_low_others",
    "series_episode_extremes",
    "actors_multilingual",
    "actors_3plus_languages",
    "actors_early_career_peak",
    "multitalented_project_performance",
)

SUCCESS_RATING = 7.0
SUCCESS_VOTES = 1_000
FIRST_YEAR = 1980
LAST_YEAR = 2025


def _explode_values(frame: DataFrame, column: str, alias: str) -> DataFrame:
    return (
        frame.withColumn(
            alias,
            F.explode(
                F.array_distinct(
                    F.transform(
                        F.split(F.col(column), ","),
                        lambda value: F.trim(value),
                    )
                )
            ),
        )
        .where(F.col(alias).isNotNull() & (F.col(alias) != ""))
    )


def runtime_outliers_by_genre(titles: DataFrame) -> DataFrame:
    """Return feature films outside each genre's Tukey runtime fences."""
    films = titles.where(
        (F.col("titleType") == "movie")
        & (F.col("runtimeMinutes") >= 60)
        & F.col("genres").isNotNull()
    )
    by_genre = _explode_values(films, "genres", "genre")
    stats = by_genre.groupBy("genre").agg(
        F.countDistinct("tconst").alias("genre_film_count"),
        F.avg("runtimeMinutes").alias("genre_average_runtime"),
        F.expr("percentile_approx(runtimeMinutes, 0.25, 10000)").alias("q1_runtime"),
        F.expr("percentile_approx(runtimeMinutes, 0.75, 10000)").alias("q3_runtime"),
    )
    stats = (
        stats.withColumn("iqr_runtime", F.col("q3_runtime") - F.col("q1_runtime"))
        .withColumn("lower_fence", F.col("q1_runtime") - 1.5 * F.col("iqr_runtime"))
        .withColumn("upper_fence", F.col("q3_runtime") + 1.5 * F.col("iqr_runtime"))
    )
    return (
        by_genre.join(stats, "genre")
        .where(
            (F.col("runtimeMinutes") < F.col("lower_fence"))
            | (F.col("runtimeMinutes") > F.col("upper_fence"))
        )
        .select(
            "genre",
            "tconst",
            "primaryTitle",
            "startYear",
            "runtimeMinutes",
            "genre_film_count",
            "genre_average_runtime",
            "q1_runtime",
            "q3_runtime",
            "lower_fence",
            "upper_fence",
            (F.col("runtimeMinutes") - F.col("genre_average_runtime")).alias(
                "difference_from_genre_average"
            ),
        )
    )


def genre_popularity_by_year(
    titles: DataFrame, ratings: DataFrame
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Rank genres by yearly vote totals and compare the top three with the rest."""
    movies = (
        titles.where(
            (F.col("titleType") == "movie")
            & F.col("startYear").between(FIRST_YEAR, LAST_YEAR)
            & F.col("genres").isNotNull()
        )
        .join(ratings.select("tconst", "numVotes"), "tconst", "inner")
        .where(F.col("numVotes").isNotNull() & (F.col("numVotes") >= 0))
    )
    genre_movies = _explode_values(movies, "genres", "genre")
    popularity = genre_movies.groupBy("startYear", "genre").agg(
        F.sum("numVotes").alias("vote_count"),
        F.countDistinct("tconst").alias("film_count"),
    )
    year_totals = popularity.groupBy("startYear").agg(
        F.sum("vote_count").alias("year_vote_count"),
        F.sum("film_count").alias("year_genre_film_count"),
    )
    ranked = (
        popularity.withColumn(
            "genre_rank",
            F.row_number().over(
                Window.partitionBy("startYear").orderBy(
                    F.desc("vote_count"), F.asc("genre")
                )
            ),
        )
        .join(year_totals, "startYear")
        .withColumn(
            "vote_share",
            F.when(
                F.col("year_vote_count") > 0,
                F.col("vote_count") / F.col("year_vote_count"),
            ),
        )
        .withColumn(
            "film_share",
            F.when(
                F.col("year_genre_film_count") > 0,
                F.col("film_count") / F.col("year_genre_film_count"),
            ),
        )
    )
    top_three = ranked.where(F.col("genre_rank") <= 3)
    comparison = (
        ranked.withColumn(
            "genre_group",
            F.when(F.col("genre_rank") <= 3, F.lit("top_three")).otherwise(
                F.lit("other_genres")
            ),
        )
        .groupBy("startYear", "genre_group")
        .agg(
            F.countDistinct("genre").alias("genre_count"),
            F.sum("film_count").alias("film_count"),
            F.sum("vote_count").alias("vote_count"),
        )
        .join(year_totals, "startYear")
        .withColumn(
            "vote_share",
            F.when(
                F.col("year_vote_count") > 0,
                F.col("vote_count") / F.col("year_vote_count"),
            ),
        )
        .withColumn(
            "film_share",
            F.when(
                F.col("year_genre_film_count") > 0,
                F.col("film_count") / F.col("year_genre_film_count"),
            ),
        )
    )
    return ranked, top_three, comparison


def directors_with_one_hit_and_low_others(
    titles: DataFrame, crew: DataFrame, ratings: DataFrame, people: DataFrame
) -> DataFrame:
    """Find directors with one 8+ rated movie and at least two other weak movies."""
    directors = _explode_values(crew, "directors", "director_id").select(
        "tconst", "director_id"
    )
    rated_movies = (
        directors.join(
            titles.where(F.col("titleType") == "movie").select(
                "tconst", "primaryTitle"
            ),
            "tconst",
        )
        .join(ratings.select("tconst", "averageRating"), "tconst")
        .where(F.col("averageRating").isNotNull())
        .dropDuplicates(["director_id", "tconst"])
    )
    director_stats = rated_movies.groupBy("director_id").agg(
        F.countDistinct(
            F.when(F.col("averageRating") >= 8.0, F.col("tconst"))
        ).alias("high_rated_movie_count"),
        F.countDistinct(
            F.when(F.col("averageRating") < 8.0, F.col("tconst"))
        ).alias("other_movie_count"),
        F.avg(F.when(F.col("averageRating") < 8.0, F.col("averageRating"))).alias(
            "other_movies_average_rating"
        ),
    )
    return (
        director_stats.where(
            (F.col("high_rated_movie_count") == 1)
            & (F.col("other_movie_count") >= 2)
            & (F.col("other_movies_average_rating") <= 6.0)
        )
        .join(
            people.select(
                F.col("nconst").alias("director_id"),
                F.col("primaryName").alias("director_name"),
            ),
            "director_id",
            "left",
        )
        .select(
            "director_id",
            "director_name",
            "high_rated_movie_count",
            "other_movie_count",
            "other_movies_average_rating",
        )
    )


def series_with_episode_extremes(
    titles: DataFrame, episodes: DataFrame, ratings: DataFrame
) -> DataFrame:
    """Find long-running series with their worst late and best early episodes."""
    series = titles.where(F.col("titleType") == "tvSeries").select(
        F.col("tconst").alias("parentTconst"),
        F.col("primaryTitle").alias("series_title"),
    )
    episode_catalog = (
        episodes.where(F.col("seasonNumber") >= 1)
        .join(series, "parentTconst")
        .select("parentTconst", "series_title", "tconst", "seasonNumber")
        .dropDuplicates(["parentTconst", "tconst"])
    )
    season_counts = episode_catalog.groupBy("parentTconst", "seasonNumber").agg(
        F.countDistinct("tconst").alias("episode_count")
    )
    qualified_series = (
        season_counts.groupBy("parentTconst")
        .agg(
            F.sum(F.when(F.col("episode_count") >= 10, 1).otherwise(0)).alias(
                "qualified_season_count"
            ),
            F.max("seasonNumber").alias("last_season"),
        )
        .where(F.col("qualified_season_count") >= 3)
    )
    rated_episodes = (
        episode_catalog.join(
            ratings.select("tconst", "averageRating"), "tconst"
        )
        .where(F.col("averageRating").isNotNull())
        .join(qualified_series, "parentTconst")
    )
    series_window = Window.partitionBy("parentTconst")
    extremes = (
        rated_episodes.withColumn(
            "worst_rating", F.min("averageRating").over(series_window)
        )
        .withColumn("best_rating", F.max("averageRating").over(series_window))
        .groupBy(
            "parentTconst",
            "series_title",
            "qualified_season_count",
            "last_season",
            "worst_rating",
            "best_rating",
        )
        .agg(
            F.max(
                F.when(
                    (F.col("averageRating") == F.col("worst_rating"))
                    & (F.col("seasonNumber") == F.col("last_season")),
                    1,
                ).otherwise(0)
            ).alias("worst_in_last_season"),
            F.max(
                F.when(
                    (F.col("averageRating") == F.col("best_rating"))
                    & (F.col("seasonNumber") <= 2),
                    1,
                ).otherwise(0)
            ).alias("best_in_first_two_seasons"),
            F.concat_ws(
                ",",
                F.sort_array(
                    F.collect_set(
                        F.when(
                            F.col("averageRating") == F.col("worst_rating"),
                            F.col("tconst"),
                        )
                    )
                ),
            ).alias("worst_episode_ids"),
            F.concat_ws(
                ",",
                F.sort_array(
                    F.collect_set(
                        F.when(
                            F.col("averageRating") == F.col("best_rating"),
                            F.col("tconst"),
                        )
                    )
                ),
            ).alias("best_episode_ids"),
        )
    )
    return extremes.where(
        (F.col("worst_in_last_season") == 1)
        & (F.col("best_in_first_two_seasons") == 1)
    )


def successful_actors_by_language(
    titles: DataFrame,
    ratings: DataFrame,
    akas: DataFrame,
    principals: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Find successful feature-film actors with at least two or three languages."""
    successful_films = (
        titles.where(
            (F.col("titleType") == "movie") & (F.col("runtimeMinutes") >= 60)
        )
        .join(ratings, "tconst")
        .where(
            (F.col("averageRating") >= SUCCESS_RATING)
            & (F.col("numVotes") >= SUCCESS_VOTES)
        )
        .select("tconst")
        .distinct()
    )
    original_languages = (
        akas.where(
            (F.col("isOriginalTitle") == F.lit(True))
            & F.col("language").isNotNull()
            & (F.trim(F.col("language")) != "")
        )
        .select(
            F.col("titleId").alias("tconst"),
            F.trim(F.col("language")).alias("language"),
        )
        .distinct()
    )
    actor_films = (
        principals.where(F.col("category").isin("actor", "actress"))
        .join(successful_films, "tconst")
        .join(original_languages, "tconst")
        .select("nconst", "tconst", "language")
        .distinct()
    )
    by_actor = actor_films.groupBy("nconst").agg(
        F.countDistinct("language").alias("language_count"),
        F.concat_ws(",", F.sort_array(F.collect_set("language"))).alias("languages"),
        F.countDistinct("tconst").alias("successful_film_count"),
    )
    multilingual = by_actor.where(F.col("language_count") >= 2)
    three_plus = by_actor.where(F.col("language_count") >= 3)
    return multilingual, three_plus


def actors_with_early_career_peak(
    titles: DataFrame, ratings: DataFrame, principals: DataFrame, people: DataFrame
) -> DataFrame:
    """Find actors with 15+ films and a stronger first-third career."""
    movies = titles.where(
        (F.col("titleType") == "movie") & F.col("startYear").isNotNull()
    ).select("tconst", "startYear")
    actor_movies = (
        principals.where(F.col("category").isin("actor", "actress"))
        .select("nconst", "tconst")
        .distinct()
        .join(movies, "tconst")
    )
    career = actor_movies.groupBy("nconst").agg(
        F.countDistinct("tconst").alias("movie_count"),
        F.min("startYear").alias("first_movie_year"),
        F.max("startYear").alias("last_movie_year"),
    ).where(
        (F.col("movie_count") >= 15)
        & ((F.col("last_movie_year") - F.col("first_movie_year")) >= 20)
    )
    rated = actor_movies.join(ratings.select("tconst", "averageRating"), "tconst")
    rated = rated.join(career, "nconst")
    early = (
        (F.col("startYear") - F.col("first_movie_year")) * 3
        <= (F.col("last_movie_year") - F.col("first_movie_year"))
    )
    actor_window = Window.partitionBy("nconst")
    scored = rated.withColumn(
        "career_max_rating", F.max("averageRating").over(actor_window)
    )
    scores = scored.groupBy(
        "nconst",
        "movie_count",
        "first_movie_year",
        "last_movie_year",
        "career_max_rating",
    ).agg(
        F.avg(F.when(early, F.col("averageRating"))).alias("early_average_rating"),
        F.avg(F.when(~early, F.col("averageRating"))).alias(
            "later_average_rating"
        ),
        F.countDistinct(F.when(early, F.col("tconst"))).alias(
            "early_rated_movie_count"
        ),
        F.countDistinct(
            F.when(
                (F.col("averageRating") == F.col("career_max_rating")) & early,
                F.col("tconst"),
            )
        ).alias("top_rated_early_movie_count"),
    )
    return (
        scores.where(
            (F.col("top_rated_early_movie_count") > 0)
            & (F.col("early_average_rating") > F.col("later_average_rating"))
        )
        .join(
            people.select(
                F.col("nconst").alias("person_id"),
                F.col("primaryName").alias("person_name"),
            ),
            F.col("nconst") == F.col("person_id"),
            "left",
        )
        .select(
            "nconst",
            "person_name",
            "movie_count",
            "first_movie_year",
            "last_movie_year",
            "early_rated_movie_count",
            "career_max_rating",
            "top_rated_early_movie_count",
            "early_average_rating",
            "later_average_rating",
        )
    )


def multitalented_project_performance(
    titles: DataFrame,
    crew: DataFrame,
    ratings: DataFrame,
    principals: DataFrame,
    people: DataFrame,
) -> DataFrame:
    """Compare ratings for multi-role and single-role film projects."""
    director_roles = _explode_values(crew, "directors", "nconst").select(
        "tconst", "nconst"
    ).withColumn("role", F.lit("director"))
    writer_roles = _explode_values(crew, "writers", "nconst").select(
        "tconst", "nconst"
    ).withColumn("role", F.lit("writer"))
    actor_roles = (
        principals.where(F.col("category").isin("actor", "actress"))
        .select(
            "tconst",
            F.col("nconst"),
            F.lit("actor").alias("role"),
        )
    )
    roles = director_roles.unionByName(writer_roles).unionByName(actor_roles).distinct()
    qualified_titles = titles.where(F.col("titleType") == "movie").select("tconst")
    roles = roles.join(qualified_titles, "tconst")
    multi_talented = (
        roles.groupBy("nconst")
        .agg(F.sort_array(F.collect_set("role")).alias("all_roles"))
        .where(F.size("all_roles") >= 2)
    )
    project_roles = roles.groupBy("nconst", "tconst").agg(
        F.countDistinct("role").alias("project_role_count")
    )
    rated_projects = (
        project_roles.join(ratings.select("tconst", "averageRating"), "tconst")
        .where(F.col("averageRating").isNotNull())
        .join(multi_talented.select("nconst"), "nconst")
    )
    metrics = rated_projects.groupBy("nconst").agg(
        F.max("project_role_count").alias("max_roles_on_one_project"),
        F.sum(F.when(F.col("project_role_count") >= 2, 1).otherwise(0)).alias(
            "multi_role_project_count"
        ),
        F.avg(
            F.when(F.col("project_role_count") >= 2, F.col("averageRating"))
        ).alias("multi_role_average_rating"),
        F.sum(F.when(F.col("project_role_count") == 1, 1).otherwise(0)).alias(
            "single_role_project_count"
        ),
        F.avg(
            F.when(F.col("project_role_count") == 1, F.col("averageRating"))
        ).alias("single_role_average_rating"),
    )
    return (
        metrics.join(multi_talented, "nconst")
        .join(
            people.select(
                "nconst", F.col("primaryName").alias("person_name")
            ),
            "nconst",
            "left",
        )
        .withColumn("all_roles", F.concat_ws(",", F.col("all_roles")))
        .withColumn(
            "multi_minus_single_rating",
            F.col("multi_role_average_rating")
            - F.col("single_role_average_rating"),
        )
        .select(
            "nconst",
            "person_name",
            "all_roles",
            "max_roles_on_one_project",
            "multi_role_project_count",
            "multi_role_average_rating",
            "single_role_project_count",
            "single_role_average_rating",
            "multi_minus_single_rating",
        )
    )


def build_reports(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build every required exploration output from the prepared IMDb tables."""
    popularity, top_three, comparison = genre_popularity_by_year(
        tables["title.basics"], tables["title.ratings"]
    )
    multilingual, three_plus = successful_actors_by_language(
        tables["title.basics"],
        tables["title.ratings"],
        tables["title.akas"],
        tables["title.principals"],
    )
    return {
        "runtime_outliers_by_genre": runtime_outliers_by_genre(
            tables["title.basics"]
        ),
        "genre_popularity_by_year": popularity,
        "top_three_genres_by_year": top_three,
        "top_three_vs_rest_by_year": comparison,
        "directors_one_high_rating_and_low_others": directors_with_one_hit_and_low_others(
            tables["title.basics"],
            tables["title.crew"],
            tables["title.ratings"],
            tables["name.basics"],
        ),
        "series_episode_extremes": series_with_episode_extremes(
            tables["title.basics"],
            tables["title.episode"],
            tables["title.ratings"],
        ),
        "actors_multilingual": multilingual,
        "actors_3plus_languages": three_plus,
        "actors_early_career_peak": actors_with_early_career_peak(
            tables["title.basics"],
            tables["title.ratings"],
            tables["title.principals"],
            tables["name.basics"],
        ),
        "multitalented_project_performance": multitalented_project_performance(
            tables["title.basics"],
            tables["title.crew"],
            tables["title.ratings"],
            tables["title.principals"],
            tables["name.basics"],
        ),
    }


def run_exploration(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool = False,
) -> None:
    """Read prepared Parquet tables, run queries, and write CSV report folders."""
    missing = [str(input_dir / table) for table in TABLES if not (input_dir / table).is_dir()]
    if missing:
        raise FileNotFoundError(
            "Missing prepared IMDb Parquet tables:\n  " + "\n  ".join(missing)
        )
    existing = [str(output_dir / report) for report in REPORTS if (output_dir / report).exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Exploration output path(s) already exist; pass --overwrite to replace them:\n  "
            + "\n  ".join(existing)
        )
    tables = {table: spark.read.parquet(str(input_dir / table)) for table in TABLES}
    reports = build_reports(tables)
    mode = "overwrite" if overwrite else "error"
    for report, frame in reports.items():
        frame.write.mode(mode).option("header", True).csv(str(output_dir / report))
        print(f"Wrote {report}: {output_dir / report}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/processed"))
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/reports/exploration")
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing exploration report folders.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-data-exploration").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        run_exploration(spark, args.input_dir, args.output_dir, args.overwrite)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

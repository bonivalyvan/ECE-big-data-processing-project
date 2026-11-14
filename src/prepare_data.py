"""Profile, validate, and clean IMDb TSV datasets with Apache Spark."""

from __future__ import annotations

import argparse

from pathlib import Path
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "title.akas": (
        "titleId",
        "ordering",
        "title",
        "region",
        "language",
        "types",
        "attributes",
        "isOriginalTitle",
    ),
    "title.basics": (
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "genres",
    ),
    "title.crew": ("tconst", "directors", "writers"),
    "title.episode": (
        "tconst",
        "parentTconst",
        "seasonNumber",
        "episodeNumber",
    ),
    "title.principals": (
        "tconst",
        "ordering",
        "nconst",
        "category",
        "job",
        "characters",
    ),
    "title.ratings": ("tconst", "averageRating", "numVotes"),
    "name.basics": (
        "nconst",
        "primaryName",
        "birthYear",
        "deathYear",
        "primaryProfession",
        "knownForTitles",
    ),
}

INTEGER_COLUMNS = {
    ("title.akas", "ordering"),
    ("title.episode", "seasonNumber"),
    ("title.episode", "episodeNumber"),
    ("title.principals", "ordering"),
    ("name.basics", "birthYear"),
    ("name.basics", "deathYear"),
    ("title.basics", "startYear"),
    ("title.basics", "endYear"),
    ("title.basics", "runtimeMinutes"),
}
LONG_COLUMNS = {("title.ratings", "numVotes")}
DOUBLE_COLUMNS = {("title.ratings", "averageRating")}
BOOLEAN_COLUMNS = {
    ("title.akas", "isOriginalTitle"),
    ("title.basics", "isAdult"),
}
FOREIGN_KEYS = (
    ("title.akas", "titleId", "title.basics", "tconst", False),
    ("title.crew", "tconst", "title.basics", "tconst", False),
    ("title.crew", "directors", "name.basics", "nconst", True),
    ("title.crew", "writers", "name.basics", "nconst", True),
    ("title.episode", "tconst", "title.basics", "tconst", False),
    ("title.episode", "parentTconst", "title.basics", "tconst", False),
    ("title.principals", "tconst", "title.basics", "tconst", False),
    ("title.principals", "nconst", "name.basics", "nconst", False),
    ("title.ratings", "tconst", "title.basics", "tconst", False),
    ("name.basics", "knownForTitles", "title.basics", "tconst", True),
)

QUALITY_SCHEMA = T.StructType(
    [
        T.StructField("table", T.StringType(), False),
        T.StructField("column", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("null_count", T.LongType(), False),
        T.StructField("missing_count", T.LongType(), False),
        T.StructField("empty_count", T.LongType(), False),
        T.StructField("invalid_typed_count", T.LongType(), False),
        T.StructField("null_pct", T.DoubleType(), False),
        T.StructField("missing_pct", T.DoubleType(), False),
        T.StructField("empty_pct", T.DoubleType(), False),
        T.StructField("absent_pct", T.DoubleType(), False),
    ]
)
TOP_SCHEMA = T.StructType(
    [
        T.StructField("rank", T.IntegerType(), False),
        T.StructField("table", T.StringType(), False),
        T.StructField("column", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("null_count", T.LongType(), False),
        T.StructField("missing_count", T.LongType(), False),
        T.StructField("empty_count", T.LongType(), False),
        T.StructField("absent_pct", T.DoubleType(), False),
    ]
)
ISSUE_SCHEMA = T.StructType(
    [
        T.StructField("issue_type", T.StringType(), False),
        T.StructField("table", T.StringType(), False),
        T.StructField("column", T.StringType(), False),
        T.StructField("expected_parent_table", T.StringType(), True),
        T.StructField("affected_count", T.LongType(), False),
        T.StructField("details", T.StringType(), False),
    ]
)


def _raw_path(input_dir: Path, table: str) -> Path:
    compressed = input_dir / f"{table}.tsv.gz"
    uncompressed = input_dir / f"{table}.tsv"
    if compressed.is_file():
        return compressed
    return uncompressed


def _parse_tsv_fields(line: str) -> list[str]:
    return line.rstrip("\r\n").split("\t")


def _parse_tsv_partition(
    partition_index: int,
    lines: Iterator[str],
    table: str,
    field_count: int,
) -> Iterator[tuple[str, ...]]:
    for line_number, line in enumerate(lines):
        if partition_index == 0 and line_number == 0:
            continue
        fields = _parse_tsv_fields(line)
        if len(fields) != field_count:
            raise ValueError(
                f"Malformed {table} TSV row in partition {partition_index} "
                f"at row {line_number}: expected {field_count} fields, got {len(fields)}"
            )
        yield tuple(fields)


def read_table(spark: SparkSession, input_dir: Path, table: str) -> DataFrame:
    """Read an IMDb TSV as strings so profiling sees original source values."""
    path = _raw_path(input_dir, table)
    if not path.is_file():
        raise FileNotFoundError(f"Missing IMDb input for {table}: {path}")
    lines = spark.sparkContext.textFile(str(path))
    header = _parse_tsv_fields(lines.first())
    missing_columns = sorted(set(TABLE_COLUMNS[table]) - set(header))
    if missing_columns:
        raise ValueError(f"{path} is missing expected columns: {missing_columns}")
    schema = T.StructType(
        [T.StructField(column, T.StringType(), True) for column in header]
    )
    rows = lines.mapPartitionsWithIndex(
        lambda partition_index, records: _parse_tsv_partition(
            partition_index, records, table, len(header)
        )
    )
    return spark.createDataFrame(rows, schema=schema).select(*TABLE_COLUMNS[table])


def _typed_value(table: str, column: str, value: F.Column) -> F.Column:
    if (table, column) in INTEGER_COLUMNS:
        return F.expr(f"try_cast(`{column}` AS INT)")
    if (table, column) in LONG_COLUMNS:
        return F.expr(f"try_cast(`{column}` AS BIGINT)")
    if (table, column) in DOUBLE_COLUMNS:
        return F.expr(f"try_cast(`{column}` AS DOUBLE)")
    if (table, column) in BOOLEAN_COLUMNS:
        return F.when(value == "1", F.lit(True)).when(
            value == "0", F.lit(False)
        ).otherwise(F.lit(None).cast(T.BooleanType()))
    return value


def _is_invalid_typed(table: str, column: str, value: F.Column) -> F.Column:
    present_value = value.isNotNull() & (F.trim(value) != "") & (
        F.trim(value) != r"\N"
    )
    if (table, column) in INTEGER_COLUMNS | LONG_COLUMNS | DOUBLE_COLUMNS:
        return present_value & _typed_value(table, column, value).isNull()
    if (table, column) in BOOLEAN_COLUMNS:
        return present_value & ~F.trim(value).isin("0", "1")
    return F.lit(False)


def profile_table(frame: DataFrame, table: str) -> list[dict[str, Any]]:
    """Calculate null, IMDb missing-marker, blank, and invalid-type counts."""
    row_count = frame.count()
    expressions = []
    for index, column in enumerate(TABLE_COLUMNS[table]):
        value = F.col(column)
        expressions.extend(
            [
                F.sum(F.when(value.isNull(), 1).otherwise(0)).alias(f"null_{index}"),
                F.sum(F.when(value == r"\N", 1).otherwise(0)).alias(
                    f"missing_{index}"
                ),
                F.sum(
                    F.when(value.isNotNull() & (F.trim(value) == ""), 1).otherwise(0)
                ).alias(f"empty_{index}"),
                F.sum(
                    F.when(_is_invalid_typed(table, column, value), 1).otherwise(0)
                ).alias(f"invalid_{index}"),
            ]
        )
    counts = frame.agg(*expressions).first() if expressions else None
    results = []
    for index, column in enumerate(TABLE_COLUMNS[table]):
        null_count = int(counts[f"null_{index}"] or 0) if counts else 0
        missing_count = int(counts[f"missing_{index}"] or 0) if counts else 0
        empty_count = int(counts[f"empty_{index}"] or 0) if counts else 0
        invalid_count = int(counts[f"invalid_{index}"] or 0) if counts else 0
        denominator = row_count or 1
        absent_count = null_count + missing_count + empty_count
        results.append(
            {
                "table": table,
                "column": column,
                "row_count": row_count,
                "null_count": null_count,
                "missing_count": missing_count,
                "empty_count": empty_count,
                "invalid_typed_count": invalid_count,
                "null_pct": null_count * 100.0 / denominator,
                "missing_pct": missing_count * 100.0 / denominator,
                "empty_pct": empty_count * 100.0 / denominator,
                "absent_pct": absent_count * 100.0 / denominator,
            }
        )
    return results


def clean_table(frame: DataFrame, table: str) -> DataFrame:
    """Normalize IMDb missing values and enforce the table's column types."""
    expressions = []
    for column in TABLE_COLUMNS[table]:
        value = F.trim(F.col(column).cast(T.StringType()))
        normalized = F.when(
            value.isNull() | (value == "") | (value == r"\N"),
            F.lit(None).cast(T.StringType()),
        ).otherwise(value)
        expressions.append(_typed_value(table, column, normalized).alias(column))
    return frame.select(*expressions)


def detect_foreign_key_issues(tables: dict[str, DataFrame]) -> list[dict[str, Any]]:
    """Count distinct non-null foreign-key values without a parent record."""
    reference_frames = []
    for child_table, child_col, parent_table, _, is_multi in FOREIGN_KEYS:
        child = tables[child_table].select(child_col)
        if is_multi:
            child = child.select(
                F.explode(F.split(F.col(child_col), ",")).alias("reference")
            )
        else:
            child = child.select(F.col(child_col).alias("reference"))
        reference_frames.append(
            child.select(
                F.lit(child_table).alias("child_table"),
                F.lit(child_col).alias("child_column"),
                F.lit(parent_table).alias("parent_table"),
                F.trim(F.col("reference")).alias("reference"),
            ).where(F.col("reference").isNotNull() & (F.col("reference") != ""))
        )

    references = reference_frames[0]
    for frame in reference_frames[1:]:
        references = references.unionByName(frame)
    references = references.dropDuplicates(
        ["child_table", "child_column", "parent_table", "reference"]
    )
    parents = (
        tables["title.basics"]
        .select(
            F.lit("title.basics").alias("parent_table"),
            F.col("tconst").alias("parent_key"),
        )
        .unionByName(
            tables["name.basics"].select(
                F.lit("name.basics").alias("parent_table"),
                F.col("nconst").alias("parent_key"),
            )
        )
        .where(F.col("parent_key").isNotNull())
        .distinct()
    )
    orphan_counts = {
        (row["child_table"], row["child_column"], row["parent_table"]): row["count"]
        for row in references.join(
            parents,
            (references.parent_table == parents.parent_table)
            & (references.reference == parents.parent_key),
            "left_anti",
        )
        .groupBy("child_table", "child_column", "parent_table")
        .count()
        .collect()
    }

    issues = []
    for child_table, child_col, parent_table, parent_col, _ in FOREIGN_KEYS:
        orphan_count = int(
            orphan_counts.get((child_table, child_col, parent_table), 0)
        )
        issues.append(
            {
                "issue_type": "orphan_foreign_key",
                "table": child_table,
                "column": child_col,
                "expected_parent_table": parent_table,
                "affected_count": orphan_count,
                "details": (
                    f"{orphan_count} distinct non-empty references have no matching "
                    f"{parent_table}.{parent_col} record"
                ),
            }
        )
    return issues


def detect_chronology_issues(tables: dict[str, DataFrame]) -> list[dict[str, Any]]:
    """Find people whose recorded death year precedes their birth year."""
    invalid_count = tables["name.basics"].where(
        F.col("birthYear").isNotNull()
        & F.col("deathYear").isNotNull()
        & (F.col("deathYear") < F.col("birthYear"))
    ).count()
    return [
        {
            "issue_type": "death_before_birth",
            "table": "name.basics",
            "column": "birthYear,deathYear",
            "expected_parent_table": None,
            "affected_count": invalid_count,
            "details": f"{invalid_count} people have deathYear earlier than birthYear",
        }
    ]


def _write_csv(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    schema: T.StructType,
    path: Path,
    mode: str,
) -> None:
    spark.createDataFrame(rows, schema=schema).coalesce(1).write.mode(mode).option(
        "header", True
    ).csv(str(path))


def run_preparation(
    spark: SparkSession,
    input_dir: Path,
    output_dir: Path,
    report_dir: Path,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    """Profile source TSVs, clean and write Parquet, and write quality reports."""
    missing = [str(_raw_path(input_dir, table)) for table in TABLE_COLUMNS]
    missing = [path for path in missing if not Path(path).is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing required IMDb input files:\n  " + "\n  ".join(missing)
        )
    mode = "overwrite" if overwrite else "error"
    destinations = [output_dir / table for table in TABLE_COLUMNS]
    destinations.extend(
        report_dir / name
        for name in ("column_quality", "top_five_missing_columns", "data_quality_issues")
    )
    existing = [str(path) for path in destinations if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "Output path(s) already exist; pass --overwrite to replace them:\n  "
            + "\n  ".join(existing)
        )

    raw_tables = {
        table: read_table(spark, input_dir, table) for table in TABLE_COLUMNS
    }
    quality_rows = [
        row
        for table, frame in raw_tables.items()
        for row in profile_table(frame, table)
    ]
    cleaned_tables = {
        table: clean_table(frame, table) for table, frame in raw_tables.items()
    }
    issues = detect_foreign_key_issues(cleaned_tables)
    issues.extend(detect_chronology_issues(cleaned_tables))

    top_rows = sorted(
        quality_rows,
        key=lambda row: (-row["absent_pct"], row["table"], row["column"]),
    )[:5]
    for rank, row in enumerate(top_rows, start=1):
        row["rank"] = rank
    top_rows = [
        {
            key: row[key]
            for key in (
                "rank",
                "table",
                "column",
                "row_count",
                "null_count",
                "missing_count",
                "empty_count",
                "absent_pct",
            )
        }
        for row in top_rows
    ]

    for table, frame in cleaned_tables.items():
        frame.write.mode(mode).parquet(str(output_dir / table))
    _write_csv(
        spark, quality_rows, QUALITY_SCHEMA, report_dir / "column_quality", mode
    )
    _write_csv(
        spark,
        top_rows,
        TOP_SCHEMA,
        report_dir / "top_five_missing_columns",
        mode,
    )
    _write_csv(
        spark,
        issues,
        ISSUE_SCHEMA,
        report_dir / "data_quality_issues",
        mode,
    )
    return top_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/processed"))
    parser.add_argument(
        "--report-dir", type=Path, default=Path("data/reports/preparation")
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace generated output directories that already exist.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("imdb-data-preparation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        top_rows = run_preparation(
            spark,
            args.input_dir,
            args.output_dir,
            args.report_dir,
            overwrite=args.overwrite,
        )
        print("Top five columns by null/missing/empty percentage:")
        for row in top_rows:
            print(
                f"{row['rank']}. {row['table']}.{row['column']}: "
                f"{row['absent_pct']:.2f}%"
            )
        print(f"Cleaned Parquet tables: {args.output_dir}")
        print(f"Quality reports: {args.report_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

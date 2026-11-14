import csv
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession

from src.prepare_data import (
    TABLE_COLUMNS,
    clean_table,
    detect_chronology_issues,
    detect_foreign_key_issues,
    profile_table,
    read_table,
    run_preparation,
)


class PrepareDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("imdb-preparation-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_profile_and_clean_track_missing_blank_and_invalid_values(self):
        frame = self.spark.createDataFrame(
            [
                ("tt1", "8.2", "100"),
                ("tt2", r"\N", " "),
                ("tt3", "bad", "-1"),
                ("", None, "0"),
                (None, "7.0", "1"),
            ],
            "tconst string, averageRating string, numVotes string",
        )

        quality = {
            row["column"]: row for row in profile_table(frame, "title.ratings")
        }
        self.assertEqual(quality["tconst"]["empty_count"], 1)
        self.assertEqual(quality["tconst"]["null_count"], 1)
        self.assertEqual(quality["averageRating"]["missing_count"], 1)
        self.assertEqual(quality["averageRating"]["invalid_typed_count"], 1)

        cleaned = clean_table(frame, "title.ratings")
        self.assertEqual(cleaned.schema["averageRating"].dataType.simpleString(), "double")
        self.assertEqual(cleaned.schema["numVotes"].dataType.simpleString(), "bigint")
        values = {
            row["tconst"]: row["averageRating"]
            for row in cleaned.select("tconst", "averageRating").collect()
        }
        self.assertEqual(values["tt1"], 8.2)
        self.assertIsNone(values["tt2"])
        self.assertIsNone(values["tt3"])

    def test_tsv_reader_preserves_imdb_missing_markers_and_blank_fields(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "title.ratings.tsv"
            path.write_text(
                "tconst\taverageRating\tnumVotes\n"
                "tt1\t8.2\t100\n"
                "tt2\t\\N\t\n"
                "tt3\t\t0\n",
                encoding="utf-8",
            )
            frame = read_table(self.spark, Path(directory), "title.ratings")
            quality = {
                row["column"]: row for row in profile_table(frame, "title.ratings")
            }
            self.assertEqual(quality["averageRating"]["missing_count"], 1)
            self.assertEqual(quality["averageRating"]["empty_count"], 1)

    def test_foreign_key_audit_counts_distinct_orphan_references(self):
        tables = {
            "title.basics": self.spark.createDataFrame(
                [("tt1",)], "tconst string"
            ),
            "name.basics": self.spark.createDataFrame(
                [("nm1", None)], "nconst string, knownForTitles string"
            ),
        }
        for table, columns in TABLE_COLUMNS.items():
            if table not in tables:
                tables[table] = self.spark.createDataFrame(
                    [], ", ".join(f"{column} string" for column in columns)
                )
        tables["title.akas"] = self.spark.createDataFrame(
            [("tt1",), ("tt9",), ("tt9",)], "titleId string"
        )
        tables["title.crew"] = self.spark.createDataFrame(
            [("tt1", "nm1,nm9", "nm8")],
            "tconst string, directors string, writers string",
        )
        issues = detect_foreign_key_issues(tables)

        akas_issue = next(
            issue
            for issue in issues
            if issue["table"] == "title.akas" and issue["column"] == "titleId"
        )
        directors_issue = next(
            issue
            for issue in issues
            if issue["table"] == "title.crew" and issue["column"] == "directors"
        )
        self.assertEqual(akas_issue["affected_count"], 1)
        self.assertEqual(directors_issue["affected_count"], 1)

    def test_death_before_birth_is_reported_without_dropping_people(self):
        tables = {
            "name.basics": self.spark.createDataFrame(
                [("nm1", 1980, 1970), ("nm2", 1950, 2000)],
                "nconst string, birthYear int, deathYear int",
            )
        }
        issue = detect_chronology_issues(tables)[0]
        self.assertEqual(issue["affected_count"], 1)

    @unittest.skipIf(
        os.name == "nt" and not os.environ.get("HADOOP_HOME"),
        "Local Spark writes on Windows require a Hadoop distribution with winutils.exe.",
    )
    def test_preparation_writes_typed_tables_and_quality_reports(self):
        fixture_rows = {
            "title.akas": [["tt9", "1", "Alias", "US", "en", "movie", r"\N", "0"]],
            "title.basics": [
                ["tt1", "movie", "Example", "Example", "0", "2020", r"\N", "90", "Drama"]
            ],
            "title.crew": [],
            "title.episode": [],
            "title.principals": [["tt1", "1", "nm9", "actor", r"\N", r"\N"]],
            "title.ratings": [["tt1", "8.0", "100"]],
            "name.basics": [["nm1", "Person", "1980", "1970", "actor", r"\N"]],
        }

        with TemporaryDirectory() as directory:
            root = Path(directory)
            input_dir = root / "raw"
            input_dir.mkdir()
            for table, columns in TABLE_COLUMNS.items():
                with (input_dir / f"{table}.tsv").open(
                    "w", encoding="utf-8", newline=""
                ) as output:
                    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
                    writer.writerow(columns)
                    writer.writerows(fixture_rows[table])

            output_dir = root / "processed"
            report_dir = root / "reports"
            top_five = run_preparation(
                self.spark, input_dir, output_dir, report_dir
            )

            self.assertEqual([row["rank"] for row in top_five], [1, 2, 3, 4, 5])
            self.assertTrue((output_dir / "title.basics" / "_SUCCESS").is_file())
            issues = self.spark.read.option("header", True).csv(
                str(report_dir / "data_quality_issues")
            )
            issue_counts = {
                (row["issue_type"], row["table"], row["column"]): int(
                    row["affected_count"]
                )
                for row in issues.collect()
            }
            self.assertEqual(
                issue_counts[("orphan_foreign_key", "title.akas", "titleId")], 1
            )
            self.assertEqual(
                issue_counts[
                    ("orphan_foreign_key", "title.principals", "nconst")
                ],
                1,
            )
            self.assertEqual(
                issue_counts[("death_before_birth", "name.basics", "birthYear,deathYear")],
                1,
            )


if __name__ == "__main__":
    unittest.main()

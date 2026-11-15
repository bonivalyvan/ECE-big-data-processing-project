import unittest

from pyspark.sql import SparkSession

from src.enrich_data import (
    FILM_TARGET_SCHEMA,
    PRINCIPAL_TARGET_SCHEMA,
    WIKIPEDIA_RECORD_SCHEMA,
    build_enriched_outputs,
    build_enrichment_targets,
)


class EnrichDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("imdb-enrichment-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_selects_top_voted_feature_films_and_three_ordered_principals(self):
        titles = [
            (f"tt{index:02}", "movie", f"Film {index}", 2000, 90, "Comedy,Drama")
            for index in range(12)
        ]
        titles.extend(
            [
                ("tt_short", "movie", "Short", 2000, 59, "Comedy"),
                ("tt_old", "movie", "Old", 1979, 90, "Comedy"),
                ("tt_series", "tvSeries", "Series", 2000, 90, "Comedy"),
            ]
        )
        title_frame = self.spark.createDataFrame(
            titles,
            "tconst string, titleType string, primaryTitle string, startYear int, "
            "runtimeMinutes int, genres string",
        )
        ratings = self.spark.createDataFrame(
            [
                (f"tt{index:02}", float(5 + index / 10), index + 1)
                for index in range(12)
            ]
            + [("tt_short", 10.0, 10000), ("tt_old", 10.0, 10000), ("tt_series", 10.0, 10000)],
            "tconst string, averageRating double, numVotes long",
        )
        principals = self.spark.createDataFrame(
            [
                ("tt11", "nm2", 2, "director"),
                ("tt11", "nm1", 1, "actor"),
                ("tt11", "nm3", 3, "writer"),
                ("tt11", "nm4", 4, "actor"),
            ],
            "tconst string, nconst string, ordering int, category string",
        )
        people = self.spark.createDataFrame(
            [(f"nm{index}", f"Person {index}", 1980) for index in range(1, 5)],
            "nconst string, primaryName string, birthYear int",
        )

        films, selected_principals = build_enrichment_targets(
            title_frame, ratings, principals, people
        )

        film_rows = {row["tconst"]: row for row in films.collect()}
        self.assertEqual(len(film_rows), 10)
        self.assertNotIn("tt00", film_rows)
        self.assertNotIn("tt01", film_rows)
        self.assertNotIn("tt_short", film_rows)
        selected_for = film_rows["tt11"]["selected_for"]
        self.assertEqual(
            {(item["genre"], item["rank"]) for item in selected_for},
            {("Comedy", 1), ("Drama", 1)},
        )
        principal_rows = (
            selected_principals.where("tconst = 'tt11'")
            .orderBy("ordering")
            .select("nconst")
            .collect()
        )
        self.assertEqual([row["nconst"] for row in principal_rows], ["nm1", "nm2", "nm3"])

    def test_enriched_outputs_keep_lineage_and_report_missing_coverage(self):
        film_targets = self.spark.createDataFrame(
            [
                (
                    "tt1",
                    "Example Film",
                    2000,
                    5000,
                    8.0,
                    [("Drama", 1)],
                )
            ],
            schema=FILM_TARGET_SCHEMA,
        )
        principal_targets = self.spark.createDataFrame(
            [
                ("tt1", "Example Film", "nm1", "Example Person", 1970, 1, "actor")
            ],
            schema=PRINCIPAL_TARGET_SCHEMA,
        )
        records = self.spark.createDataFrame(
            [
                (
                    "film",
                    "tt1",
                    "Example Film",
                    "Example Film",
                    2000,
                    "A human-authored film summary.",
                    "https://en.wikipedia.org/wiki/Example_Film",
                    "wikipedia_page",
                    "Example Film",
                    1,
                    11,
                    "2026-11-14T00:00:00+00:00",
                    "found",
                    "Wikipedia lead extract saved as the film synopsis/summary.",
                ),
                (
                    "principal",
                    "nm1",
                    "Example Person",
                    None,
                    None,
                    None,
                    "https://en.wikipedia.org/w/api.php?search=Example",
                    "wikipedia_search",
                    None,
                    None,
                    None,
                    "2026-11-14T00:00:00+00:00",
                    "not_found",
                    "No exact Wikipedia page-title match was found in the search results.",
                ),
            ],
            schema=WIKIPEDIA_RECORD_SCHEMA,
        )

        films, principals, coverage = build_enriched_outputs(
            film_targets, principal_targets, records
        )

        film = films.first()
        self.assertEqual(film["synopsis"], "A human-authored film summary.")
        self.assertEqual(
            film["source_url"], "https://en.wikipedia.org/wiki/Example_Film"
        )
        self.assertEqual(film["wikipedia_revision_id"], 11)
        self.assertEqual(film["license"], "CC BY-SA 4.0")
        principal = principals.first()
        self.assertEqual(principal["enrichment_status"], "not_found")
        self.assertTrue(principal["source_url"].startswith("https://en.wikipedia.org/"))
        coverage_row = coverage.first()
        self.assertEqual(coverage_row["principal_target_count"], 1)
        self.assertEqual(coverage_row["biography_count"], 0)
        self.assertIn("only 1 principal credits", coverage_row["coverage_note"])


if __name__ == "__main__":
    unittest.main()

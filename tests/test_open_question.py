import unittest

from pyspark.sql import SparkSession

from src.open_question import build_region_reach_reports


class OpenQuestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("imdb-open-question-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_counts_distinct_regions_and_compares_genre_decade_strata(self):
        title_rows = []
        rating_rows = []
        aka_rows = []
        for index in range(10):
            one_region_id = f"one_{index}"
            broad_region_id = f"broad_{index}"
            title_rows.extend(
                [
                    (one_region_id, "movie", 1985, 90, "Comedy"),
                    (broad_region_id, "movie", 1986, 95, "Comedy"),
                ]
            )
            rating_rows.extend(
                [
                    (one_region_id, 6.0, (index + 1) * 100),
                    (broad_region_id, 8.0, (index + 10) * 100),
                ]
            )
            aka_rows.extend(
                [
                    (one_region_id, "US"),
                    (one_region_id, "US"),
                    *[
                        (broad_region_id, region)
                        for region in ("US", "GB", "FR", "DE", "JP")
                    ],
                ]
            )
        title_rows.append(("no_region", "movie", 1987, 90, "Comedy"))
        rating_rows.append(("no_region", 7.0, 500))

        titles = self.spark.createDataFrame(
            title_rows,
            "tconst string, titleType string, startYear int, "
            "runtimeMinutes int, genres string",
        )
        ratings = self.spark.createDataFrame(
            rating_rows, "tconst string, averageRating double, numVotes long"
        )
        akas = self.spark.createDataFrame(
            aka_rows, "titleId string, region string"
        )

        reports = build_region_reach_reports(akas, titles, ratings)
        overall = {
            row["coverage_tier"]: row
            for row in reports["region_reach_overall"].collect()
        }
        self.assertEqual(overall["1 region"]["film_count"], 10)
        self.assertEqual(overall["5+ regions"]["film_count"], 10)
        self.assertEqual(overall["no recorded region"]["film_count"], 1)

        comparison = reports["region_reach_strata_comparison"].first()
        self.assertEqual(comparison["start_decade"], 1980)
        self.assertEqual(comparison["genre"], "Comedy")
        self.assertEqual(comparison["one_region_film_count"], 10)
        self.assertEqual(comparison["five_plus_region_film_count"], 10)
        self.assertAlmostEqual(
            comparison["median_vote_ratio_5plus_vs_one"], 1400 / 500
        )
        self.assertAlmostEqual(
            comparison["median_rating_difference_5plus_minus_one"], 2.0
        )


if __name__ == "__main__":
    unittest.main()

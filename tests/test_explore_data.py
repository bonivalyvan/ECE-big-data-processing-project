import unittest

from pyspark.sql import SparkSession

from src.explore_data import (
    actors_with_early_career_peak,
    directors_with_one_hit_and_low_others,
    genre_popularity_by_year,
    multitalented_project_performance,
    runtime_outliers_by_genre,
    series_with_episode_extremes,
    successful_actors_by_language,
)


class ExploreDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("imdb-exploration-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_exploration_queries_cover_required_criteria(self):
        title_rows = []
        rating_rows = []
        crew_rows = []
        principal_rows = []
        aka_rows = []

        def add_movie(
            title_id,
            year,
            rating,
            votes,
            genre=None,
            runtime=90,
            title_type="movie",
        ):
            title_rows.append(
                (title_id, title_type, title_id, year, runtime, genre)
            )
            if rating is not None:
                rating_rows.append((title_id, rating, votes))

        for title_id, runtime in zip(
            ["rt1", "rt2", "rt3", "rt4", "rt5"], [90, 95, 100, 105, 300]
        ):
            add_movie(title_id, 1980, 6.0, 100, "RuntimeGenre", runtime)
        add_movie("pop_comedy", 1980, 7.0, 1200, "Comedy")
        add_movie("pop_romance", 1980, 7.0, 800, "Romance")
        add_movie("pop_scifi", 1980, 7.0, 100, "Sci-Fi")

        director_films = [
            ("director_hit", 8.5),
            ("director_low_1", 5.0),
            ("director_low_2", 6.0),
        ]
        for title_id, rating in director_films:
            add_movie(title_id, 2000, rating, 100)
            crew_rows.append((title_id, "nmDirector", None))

        for title_id, language in [
            ("lang_en", "en"),
            ("lang_fr", "fr"),
            ("lang_de", "de"),
        ]:
            add_movie(title_id, 2010, 8.0, 1500)
            aka_rows.append((title_id, language, True))
            principal_rows.append((title_id, "nmLanguage", "actor"))

        early_years = [
            2000,
            2001,
            2002,
            2003,
            2004,
            2005,
            2006,
            2007,
            2008,
            2009,
            2010,
            2012,
            2014,
            2017,
            2020,
        ]
        for index, year in enumerate(early_years):
            title_id = f"early_{index}"
            rating = 9.0 if index == 0 else (8.0 if index < 7 else 5.0)
            add_movie(title_id, year, rating, 500)
            principal_rows.append((title_id, "nmEarly", "actress"))

        add_movie("multi_dual", 2018, 9.0, 500)
        add_movie("multi_single", 2019, 6.0, 500)
        crew_rows.extend(
            [
                ("multi_dual", "nmMulti", "nmMulti"),
                ("multi_single", None, "nmMulti"),
            ]
        )
        principal_rows.append(("multi_dual", "nmMulti", "actor"))

        title_rows.append(("series1", "tvSeries", "Series One", 2010, None, None))
        episode_rows = []
        for season in (1, 2, 3):
            for episode_number in range(1, 11):
                episode_id = f"series_s{season}_e{episode_number}"
                episode_rows.append((episode_id, "series1", season, episode_number))
        rating_rows.extend(
            [
                ("series_s1_e1", 9.5, 500),
                ("series_s3_e10", 2.0, 500),
            ]
        )

        titles = self.spark.createDataFrame(
            title_rows,
            "tconst string, titleType string, primaryTitle string, "
            "startYear int, runtimeMinutes int, genres string",
        )
        ratings = self.spark.createDataFrame(
            rating_rows, "tconst string, averageRating double, numVotes long"
        )
        crew = self.spark.createDataFrame(
            crew_rows, "tconst string, directors string, writers string"
        )
        episodes = self.spark.createDataFrame(
            episode_rows,
            "tconst string, parentTconst string, seasonNumber int, episodeNumber int",
        )
        akas = self.spark.createDataFrame(
            aka_rows, "titleId string, language string, isOriginalTitle boolean"
        )
        principals = self.spark.createDataFrame(
            principal_rows, "tconst string, nconst string, category string"
        )
        people = self.spark.createDataFrame(
            [
                ("nmDirector", "Director"),
                ("nmLanguage", "Language Actor"),
                ("nmEarly", "Early Actor"),
                ("nmMulti", "Multi-talented"),
            ],
            "nconst string, primaryName string",
        )

        runtime_outliers = runtime_outliers_by_genre(titles).collect()
        self.assertEqual([row["tconst"] for row in runtime_outliers], ["rt5"])

        popularity, top_three, comparison = genre_popularity_by_year(
            titles, ratings
        )
        self.assertEqual(top_three.where("startYear = 1980").count(), 3)
        self.assertEqual(
            {row["genre_group"] for row in comparison.collect()},
            {"top_three", "other_genres"},
        )
        self.assertGreater(popularity.where("vote_share > 0").count(), 0)

        directors = directors_with_one_hit_and_low_others(
            titles, crew, ratings, people
        ).collect()
        self.assertEqual([(row["director_id"], row["other_movie_count"]) for row in directors],
                         [("nmDirector", 2)])

        series = series_with_episode_extremes(
            titles, episodes, ratings
        ).collect()
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0]["qualified_season_count"], 3)
        self.assertIn("series_s3_e10", series[0]["worst_episode_ids"])
        self.assertIn("series_s1_e1", series[0]["best_episode_ids"])

        multilingual, three_plus = successful_actors_by_language(
            titles, ratings, akas, principals
        )
        self.assertEqual(multilingual.count(), 1)
        self.assertEqual(three_plus.first()["languages"], "de,en,fr")

        early_actors = actors_with_early_career_peak(
            titles, ratings, principals, people
        ).collect()
        self.assertEqual([row["nconst"] for row in early_actors], ["nmEarly"])

        multi_role = multitalented_project_performance(
            titles, crew, ratings, principals, people
        ).collect()
        self.assertEqual(len(multi_role), 1)
        self.assertEqual(multi_role[0]["all_roles"], "actor,director,writer")
        self.assertAlmostEqual(multi_role[0]["multi_minus_single_rating"], 3.0)


if __name__ == "__main__":
    unittest.main()

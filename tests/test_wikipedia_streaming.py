import json
import unittest

from pyspark.sql import SparkSession

from src.stream_wikipedia_changes import (
    annotate_imdb_matches,
    build_imdb_target_index,
    filter_candidate_changes,
    parse_kafka_events,
)
from src.wikipedia_stream_producer import iter_sse_events, publish_event


class _FakeFuture:
    def get(self, timeout):
        return None


class _FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        return _FakeFuture()


class WikipediaStreamProducerTests(unittest.TestCase):
    def test_sse_parser_reads_json_data_events(self):
        import io

        payload = (
            b": heartbeat\n\n"
            b'data: {"wiki":"enwiki","meta":{"id":"event-1"}}\n\n'
        )
        self.assertEqual(
            list(iter_sse_events(io.BytesIO(payload))),
            [{"wiki": "enwiki", "meta": {"id": "event-1"}}],
        )

    def test_publisher_sends_only_english_wikipedia_events(self):
        producer = _FakeProducer()
        event = {"wiki": "enwiki", "meta": {"id": "event-1"}}

        self.assertTrue(publish_event(event, producer))
        self.assertFalse(
            publish_event({"wiki": "frwiki", "meta": {"id": "event-2"}}, producer)
        )
        self.assertEqual(len(producer.sent), 1)
        topic, key, value = producer.sent[0]
        self.assertEqual(topic, "wikipedia-recent-changes")
        self.assertEqual(key, b"event-1")
        self.assertEqual(value, event)


class WikipediaStreamTransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("wikipedia-streaming-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filters_broad_article_change_candidates_and_marks_exact_targets(self):
        events = [
            {
                "meta": {"id": "event-1", "dt": "2026-11-15T10:00:00Z", "uri": "url1"},
                "id": 101,
                "type": "edit",
                "namespace": 0,
                "title": "Example Film",
                "title_url": "https://en.wikipedia.org/wiki/Example_Film",
                "timestamp": 1794736800,
                "user": "Editor",
                "bot": False,
                "wiki": "enwiki",
                "revision": {"old": 1, "new": 2},
            },
            {
                "meta": {"id": "event-2", "dt": "2026-11-15T10:01:00Z", "uri": "url2"},
                "id": 202,
                "type": "new",
                "namespace": 0,
                "title": "Unmatched Article",
                "title_url": "https://en.wikipedia.org/wiki/Unmatched_Article",
                "timestamp": 1794736860,
                "user": "Editor2",
                "bot": True,
                "wiki": "enwiki",
                "revision": {"old": 0, "new": 3},
            },
            {
                "meta": {"id": "event-3", "dt": "2026-11-15T10:02:00Z", "uri": "url3"},
                "id": 303,
                "type": "edit",
                "namespace": 6,
                "title": "File:Poster.jpg",
                "wiki": "enwiki",
            },
            {
                "meta": {"id": "event-4", "dt": "2026-11-15T10:03:00Z", "uri": "url4"},
                "id": 404,
                "type": "edit",
                "namespace": 0,
                "title": "French Article",
                "wiki": "frwiki",
            },
            {
                "meta": {"id": "event-5", "dt": "2026-11-15T10:04:00Z", "uri": "url5"},
                "id": 505,
                "type": "log",
                "namespace": 0,
                "title": "Logged Article",
                "wiki": "enwiki",
            },
        ]
        kafka_rows = [
            (json.dumps(event).encode("utf-8"),)
            for event in events
        ]
        kafka_frame = self.spark.createDataFrame(kafka_rows, "value binary")
        parsed = parse_kafka_events(kafka_frame)
        candidates = filter_candidate_changes(parsed)

        film_targets = self.spark.createDataFrame(
            [(101, "tt1", "Example Film"), (None, "tt2", "Missing Page")],
            "wikipedia_page_id long, tconst string, title string",
        )
        principal_targets = self.spark.createDataFrame(
            [(101, "nm1", "Example Person")],
            "wikipedia_page_id long, nconst string, principal_name string",
        )
        target_index = build_imdb_target_index(film_targets, principal_targets)
        identified = annotate_imdb_matches(candidates, target_index)
        rows = {row["event_id"]: row for row in identified.collect()}

        self.assertEqual(set(rows), {"event-1", "event-2"})
        self.assertTrue(rows["event-1"]["is_imdb_target"])
        self.assertFalse(rows["event-2"]["is_imdb_target"])
        self.assertEqual(rows["event-1"]["page_id"], 101)
        self.assertEqual(rows["event-1"]["revision_new"], 2)


if __name__ == "__main__":
    unittest.main()

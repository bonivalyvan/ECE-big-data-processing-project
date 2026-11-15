import json
import unittest

from src.collect_wikipedia import USER_AGENT, fetch_wikipedia_record


class _FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.payload


class CollectWikipediaTests(unittest.TestCase):
    def test_fetch_uses_exact_page_and_preserves_lineage(self):
        requests = []

        def opener(request, timeout):
            requests.append(request)
            return _FakeResponse(
                {
                    "query": {
                        "pages": [
                            {
                                "title": "Example Film (2000 film)",
                                "pageid": 100,
                                "fullurl": "https://en.wikipedia.org/wiki/Example_Film",
                                "extract": "Example Film is a 2000 film.",
                                "revisions": [{"revid": 900}],
                            }
                        ]
                    }
                }
            )

        record = fetch_wikipedia_record(
            {"tconst": "tt1", "title": "Example Film", "startYear": 2000},
            "film",
            opener=opener,
        )

        self.assertEqual(record["status"], "found")
        self.assertEqual(record["content"], "Example Film is a 2000 film.")
        self.assertEqual(record["source_url"], "https://en.wikipedia.org/wiki/Example_Film")
        self.assertEqual(record["revision_id"], 900)
        self.assertEqual(requests[0].get_header("User-agent"), USER_AGENT)

    def test_unmatched_page_is_reported_with_search_uri(self):
        record = fetch_wikipedia_record(
            {"tconst": "tt2", "title": "No Matching Film", "startYear": 2000},
            "film",
            opener=lambda request, timeout: _FakeResponse({"query": {"pages": []}}),
        )

        self.assertEqual(record["status"], "not_found")
        self.assertEqual(record["source_type"], "wikipedia_search")
        self.assertIn("wikipedia.org/w/api.php?", record["source_url"])
        self.assertIsNone(record["content"])
        self.assertIsNotNone(record["note"])

    def test_missing_imdb_name_skips_wikipedia_request(self):
        record = fetch_wikipedia_record(
            {"nconst": "nm1", "display_name": None},
            "principal",
            opener=lambda request, timeout: self.fail("unexpected network request"),
        )

        self.assertEqual(record["status"], "imdb_name_missing")
        self.assertIn("wikipedia.org/w/api.php?", record["source_url"])
        self.assertIsNotNone(record["note"])


if __name__ == "__main__":
    unittest.main()

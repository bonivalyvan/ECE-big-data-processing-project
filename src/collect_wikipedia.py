"""Collect human-authored English Wikipedia lead extracts for IMDb targets."""

from __future__ import annotations

import argparse
import json
import time
import unicodedata
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = (
    "Big-Data-Test/1.0 "
    "(https://github.com/bonivalyvan/Big-Data-Test; IMDb enrichment project)"
)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_API_ERROR_CODES = {"cirrussearch-too-busy-error"}
MAX_ATTEMPTS = 5


def _normalize_title(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).casefold()
    return "".join(character for character in normalized if character.isalnum())


def _page_core_title(value: str) -> str:
    return value.split(" (", 1)[0].strip()


def _search_query(target: dict[str, Any], entity_type: str) -> str:
    if entity_type == "film":
        title = str(target["title"]).replace('"', " ")
        return f'"{title}" {target["startYear"]} film'
    name = target.get("display_name")
    if not name:
        return ""
    birth_year = target.get("birthYear")
    suffix = f" {birth_year}" if birth_year is not None else ""
    query_name = str(name).replace('"', " ")
    return f'"{query_name}"{suffix}'


def _search_url(query: str) -> str:
    parameters = {
        "action": "query",
        "generator": "search",
        "gsrsearch": query,
        "gsrnamespace": "0",
        "gsrlimit": "10",
        "prop": "extracts|info|revisions",
        "exintro": "1",
        "explaintext": "1",
        "exchars": "10000",
        "inprop": "url",
        "rvprop": "ids",
        "format": "json",
        "formatversion": "2",
    }
    return f"{API_URL}?{urlencode(parameters)}"


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _load_api_response(
    url: str,
    opener: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    request_opener = opener or urlopen
    request = Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    for attempt in range(MAX_ATTEMPTS):
        try:
            with request_opener(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if "error" in payload:
                error = payload["error"]
                if (
                    error.get("code") in RETRYABLE_API_ERROR_CODES
                    and attempt < MAX_ATTEMPTS - 1
                ):
                    sleep(min(2**attempt, 60))
                    continue
                raise RuntimeError(f"Wikipedia API error: {error}")
            return payload
        except HTTPError as error:
            if error.code not in RETRYABLE_STATUS_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            delay = _retry_after_seconds(error.headers.get("Retry-After"))
            sleep(delay if delay is not None else min(2**attempt, 60))
        except URLError:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            sleep(min(2**attempt, 60))
    raise RuntimeError("Wikipedia request retries were exhausted.")


def _search_pages(payload: dict[str, Any]) -> list[dict[str, Any]]:
    pages = payload.get("query", {}).get("pages", [])
    if isinstance(pages, dict):
        return list(pages.values())
    return pages


def _select_page(
    pages: list[dict[str, Any]], target: dict[str, Any], entity_type: str
) -> tuple[dict[str, Any] | None, str | None]:
    expected = target.get("title") if entity_type == "film" else target.get("display_name")
    if not expected:
        return None, "IMDb name record is missing; Wikipedia search was skipped."

    expected_normalized = _normalize_title(str(expected))
    exact_matches = []
    for page in pages:
        page_title = str(page.get("title", ""))
        if _normalize_title(_page_core_title(page_title)) != expected_normalized:
            continue
        score = int(_normalize_title(page_title) == expected_normalized)
        extract = str(page.get("extract", ""))
        year = target.get("startYear") if entity_type == "film" else target.get("birthYear")
        if year is not None and str(year) in page_title:
            score += 1
        if year is not None and str(year) in extract[:1000]:
            score += 1
        exact_matches.append((score, page))

    if not exact_matches:
        return None, "No exact Wikipedia page-title match was found in the search results."
    best_score = max(score for score, _ in exact_matches)
    best_matches = [page for score, page in exact_matches if score == best_score]
    if len(best_matches) != 1:
        return None, "Multiple equally ranked Wikipedia pages matched; no page was selected."
    return best_matches[0], None


def fetch_wikipedia_record(
    target: dict[str, Any],
    entity_type: str,
    opener: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if entity_type not in {"film", "principal"}:
        raise ValueError(f"Unsupported Wikipedia target type: {entity_type}")
    imdb_id = target.get("tconst") if entity_type == "film" else target.get("nconst")
    if not imdb_id:
        raise ValueError(f"Missing IMDb identifier for {entity_type} target.")

    query = _search_query(target, entity_type)
    if not query:
        query = str(imdb_id)
    search_url = _search_url(query)
    record: dict[str, Any] = {
        "entity_type": entity_type,
        "imdb_id": imdb_id,
        "display_name": target.get("display_name"),
        "title": target.get("title"),
        "start_year": target.get("startYear"),
        "content": None,
        "source_url": search_url,
        "source_type": "wikipedia_search",
        "page_title": None,
        "page_id": None,
        "revision_id": None,
        "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": "not_found",
        "note": None,
        "attribution": None,
        "license": None,
        "license_url": None,
    }
    if entity_type == "principal" and not target.get("display_name"):
        record["status"] = "imdb_name_missing"
        record["note"] = "IMDb name record is missing; Wikipedia search was skipped."
        return record
    payload = _load_api_response(search_url, opener=opener, sleep=sleep)
    page, note = _select_page(_search_pages(payload), target, entity_type)
    record["note"] = note
    if page is None:
        record["status"] = "ambiguous" if note and note.startswith("Multiple") else "not_found"
        return record

    revisions = page.get("revisions", [])
    extract = str(page.get("extract", "")).strip()
    record.update(
        {
            "content": extract or None,
            "source_url": page.get("fullurl") or search_url,
            "source_type": "wikipedia_page",
            "page_title": page.get("title"),
            "page_id": page.get("pageid"),
            "revision_id": revisions[0].get("revid") if revisions else None,
            "status": "found" if extract else "empty_extract",
            "attribution": "Wikipedia contributors" if extract else None,
            "license": "CC BY-SA 4.0" if extract else None,
            "license_url": (
                "https://creativecommons.org/licenses/by-sa/4.0/"
                if extract
                else None
            ),
            "note": (
                "Wikipedia lead extract saved as the film synopsis/summary."
                if entity_type == "film" and extract
                else (
                    "Wikipedia lead extract saved as the biographical summary."
                    if extract
                    else "Wikipedia page found, but it returned no usable lead extract."
                )
            ),
        }
    )
    return record


def _read_manifest(directory: Path) -> list[dict[str, Any]]:
    part_files = sorted(directory.glob("part-*.json"))
    if not part_files:
        raise FileNotFoundError(f"No Spark JSON manifest parts found in {directory}.")
    targets: dict[str, dict[str, Any]] = {}
    id_field = "tconst" if directory.name == "films" else "nconst"
    for part_file in part_files:
        with part_file.open("r", encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                try:
                    target = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"Invalid JSON in {part_file} at line {line_number}."
                    ) from error
                target_id = target.get(id_field)
                if not target_id:
                    raise ValueError(
                        f"Missing {id_field} in {part_file} at line {line_number}."
                    )
                targets[target_id] = target
    return [targets[target_id] for target_id in sorted(targets)]


def _load_completed_records(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    completed = set()
    with path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {path} at line {line_number}."
                ) from error
            entity_type = record.get("entity_type")
            imdb_id = record.get("imdb_id")
            if entity_type not in {"film", "principal"} or not imdb_id:
                raise ValueError(
                    f"Missing or invalid entity identity in {path} at line {line_number}."
                )
            source_url = record.get("source_url")
            if (
                not record.get("status")
                or not record.get("source_type")
                or not isinstance(source_url, str)
                or not source_url.strip()
            ):
                raise ValueError(
                    f"Missing enrichment status or source lineage in {path} "
                    f"at line {line_number}."
                )
            completed.add((entity_type, imdb_id))
    return completed


def collect_wikipedia(
    target_dir: Path,
    records_path: Path,
    request_delay: float = 1.0,
    max_entities: int | None = None,
    opener: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    if request_delay < 1.0:
        raise ValueError("request_delay must be at least one second.")
    if max_entities is not None and max_entities < 1:
        raise ValueError("max_entities must be greater than zero.")

    targets = [
        ("film", target)
        for target in _read_manifest(target_dir / "films")
    ] + [
        ("principal", target)
        for target in _read_manifest(target_dir / "principals")
    ]
    completed = _load_completed_records(records_path)
    pending = [
        (entity_type, target)
        for entity_type, target in targets
        if (
            entity_type,
            target["tconst"] if entity_type == "film" else target["nconst"],
        )
        not in completed
    ]
    if max_entities is not None:
        pending = pending[:max_entities]

    records_path.parent.mkdir(parents=True, exist_ok=True)
    last_request_started: float | None = None
    written = 0
    with records_path.open("a", encoding="utf-8", newline="\n") as destination:
        for entity_type, target in pending:
            started = monotonic()
            if last_request_started is not None:
                wait = request_delay - (started - last_request_started)
                if wait > 0:
                    sleep(wait)
            last_request_started = monotonic()
            record = fetch_wikipedia_record(
                target, entity_type, opener=opener, sleep=sleep
            )
            destination.write(json.dumps(record, ensure_ascii=False) + "\n")
            destination.flush()
            completed.add((entity_type, record["imdb_id"]))
            written += 1
            if written % 100 == 0:
                print(f"Wikipedia records collected: {written}")
    print(f"New Wikipedia records collected: {written}")
    print(f"Total completed IMDb targets: {len(completed)}")
    print(f"Records file: {records_path}")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-dir", type=Path, default=Path("data/enrichment/targets")
    )
    parser.add_argument(
        "--records", type=Path, default=Path("data/enrichment/wikipedia_records.jsonl")
    )
    parser.add_argument("--request-delay", type=float, default=1.0)
    parser.add_argument("--max-entities", type=int)
    args = parser.parse_args()
    collect_wikipedia(
        args.target_dir,
        args.records,
        request_delay=args.request_delay,
        max_entities=args.max_entities,
    )


if __name__ == "__main__":
    main()

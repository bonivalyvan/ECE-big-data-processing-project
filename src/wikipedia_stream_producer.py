"""Forward live English Wikipedia recent changes to Kafka."""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Callable, Iterator
from typing import Any
from urllib.request import Request, urlopen


EVENTSTREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = "wikipedia-recent-changes"
USER_AGENT = (
    "Big-Data-Test/1.0 "
    "(https://github.com/bonivalyvan/Big-Data-Test; IMDb streaming project)"
)
LOGGER = logging.getLogger(__name__)


def iter_sse_events(response: Any) -> Iterator[dict[str, Any]]:
    data_lines: list[str] = []
    for raw_line in response:
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if not line:
            if data_lines:
                yield json.loads("\n".join(data_lines))
                data_lines.clear()
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if data_lines:
        yield json.loads("\n".join(data_lines))


def publish_event(event: dict[str, Any], producer: Any, topic: str = TOPIC) -> bool:
    if event.get("wiki") != "enwiki":
        return False
    metadata = event.get("meta")
    event_id = metadata.get("id") if isinstance(metadata, dict) else None
    if not event_id:
        raise ValueError("English Wikipedia change event is missing meta.id.")
    producer.send(
        topic,
        key=str(event_id).encode("utf-8"),
        value=event,
    ).get(timeout=30)
    return True


def run_producer(
    bootstrap_servers: str,
    topic: str = TOPIC,
    eventstream_url: str = EVENTSTREAM_URL,
    opener: Callable[..., Any] = urlopen,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        acks="all",
        retries=10,
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8"),
    )
    retry_delay = 1
    published = 0
    request = Request(
        eventstream_url,
        headers={"Accept": "text/event-stream", "User-Agent": USER_AGENT},
    )
    try:
        while True:
            try:
                LOGGER.info("Connecting to Wikimedia EventStreams.")
                with opener(request, timeout=75) as response:
                    retry_delay = 1
                    for event in iter_sse_events(response):
                        if publish_event(event, producer, topic):
                            published += 1
                            if published % 100 == 0:
                                LOGGER.info(
                                    "English Wikipedia events published: %s",
                                    published,
                                )
            except KeyboardInterrupt:
                raise
            except Exception:
                LOGGER.exception(
                    "Wikimedia EventStreams connection failed; reconnecting in %s s.",
                    retry_delay,
                )
                sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9094",
        help="Comma-separated Kafka bootstrap servers.",
    )
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--eventstream-url", default=EVENTSTREAM_URL)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run_producer(
        args.bootstrap_servers,
        topic=args.topic,
        eventstream_url=args.eventstream_url,
    )


if __name__ == "__main__":
    main()

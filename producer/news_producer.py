
import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

API = "https://hacker-news.firebaseio.com/v0"


def get_json(url):
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def clean_text(value):
    return BeautifulSoup(
        value or "", "html.parser"
    ).get_text(" ", strip=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="data/raw/hackernews.jsonl"
    )
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--interval", type=int, default=60)
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    # Read previously saved IDs to avoid resending
    # the same story after restarting the program.
    seen = set()

    if output.exists():
        with output.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    record = json.loads(line)
                    seen.add(str(record["id"]))

    broker = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda item: json.dumps(
            item
        ).encode("utf-8")
    )

    print("Connected to Kafka!")
    print("Starting Hacker News collection...")

    try:
        while True:
            try:
                ids = get_json(
                    f"{API}/newstories.json"
                )[:args.limit]

                new_count = 0

                for story_id in reversed(ids):
                    if str(story_id) in seen:
                        continue

                    story = get_json(
                        f"{API}/item/{story_id}.json"
                    )

                    if not story:
                        continue

                    if (
                        story.get("deleted")
                        or story.get("dead")
                    ):
                        continue

                    title = clean_text(
                        story.get("title", "")
                    )
                    body = clean_text(
                        story.get("text", "")
                    )

                    text = f"{title} {body}".strip()

                    if not text:
                        continue

                    message = {
                        "id": str(story_id),
                        "text": text,
                        "source": "hackernews",
                        "timestamp": story.get("time"),
                        "collected_at": datetime.now(
                            timezone.utc
                        ).isoformat()
                    }

                    # Wait for Kafka to acknowledge the send
                    producer.send(
                        args.topic, message
                    ).get(timeout=15)

                    # Save the same message locally
                    with output.open(
                        "a", encoding="utf-8"
                    ) as file:
                        file.write(
                            json.dumps(message) + "\n"
                        )

                    seen.add(str(story_id))
                    new_count += 1

                    print("Sent:", text[:90])

                print(
                    f"New messages: {new_count} | "
                    f"Total saved: {len(seen)}"
                )

            except (requests.RequestException, Exception) as e:
                print("Error:", e)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()

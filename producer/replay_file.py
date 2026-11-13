"""
Testing helper: replay saved stories into a Kafka topic.

Reads the JSONL file saved by news_producer.py and sends each line
to Kafka with a short pause, so the Spark streaming job can be tested
without waiting for new live Hacker News stories.
"""

import argparse
import os
import time

from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True,
                        help="JSONL file saved by news_producer.py")
    parser.add_argument("--topic", default="topic1")
    parser.add_argument("--delay", type=float, default=0.2,
                        help="Seconds to wait between messages")
    parser.add_argument("--limit", type=int, default=0,
                        help="Send at most this many messages (0 = all)")
    args = parser.parse_args()

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducer(bootstrap_servers=broker)

    sent = 0
    try:
        with open(args.input, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue

                # Send the saved JSON line exactly as the producer did.
                producer.send(args.topic, line.encode("utf-8"))
                sent += 1

                if sent % 50 == 0:
                    print(f"Sent {sent} messages to {args.topic}")

                if args.limit and sent >= args.limit:
                    break

                time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        producer.flush()
        producer.close()

    print(f"Done. Sent {sent} messages to {args.topic}.")


if __name__ == "__main__":
    main()

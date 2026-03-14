import argparse
import json
from typing import Any, Dict

from kafka import KafkaConsumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Count trips with distance > threshold.")
    parser.add_argument("--topic", default="green-trips", help="Kafka topic name")
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server",
    )
    parser.add_argument(
        "--min-distance",
        type=float,
        default=5.0,
        help="Trip distance threshold",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=10000,
        help="Stop after no messages for this long",
    )
    return parser.parse_args()


def json_deserializer(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode("utf-8"))


def main() -> None:
    args = parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap_server],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="green-trips-count",
        value_deserializer=json_deserializer,
        consumer_timeout_ms=args.timeout_ms,
    )

    total = 0
    matched = 0

    for message in consumer:
        total += 1
        record = message.value
        distance = record.get("trip_distance")
        try:
            distance_value = float(distance) if distance is not None else 0.0
        except (TypeError, ValueError):
            distance_value = 0.0

        if distance_value > args.min_distance:
            matched += 1

    consumer.close()

    print(f"consumed {total} messages")
    print(f"trips with distance > {args.min_distance}: {matched}")


if __name__ == "__main__":
    main()

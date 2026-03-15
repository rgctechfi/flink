import argparse
import json
import time
from typing import Any, Dict

import pandas as pd
from kafka import KafkaProducer

DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
COLUMNS = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount",
]
DATETIME_COLUMNS = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send green taxi trips to Kafka.")
    parser.add_argument("--url", default=DATA_URL, help="Parquet URL to read")
    parser.add_argument("--topic", default="green-trips", help="Kafka topic name") #TOPIC NAME
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of rows to send",
    )
    return parser.parse_args()


def normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def load_trips(url: str, limit: int | None) -> pd.DataFrame:
    df = pd.read_parquet(url, columns=COLUMNS)

    for column in DATETIME_COLUMNS:
        df[column] = df[column].dt.strftime("%Y-%m-%d %H:%M:%S")
        df[column] = df[column].where(df[column].notna(), None)

    if limit is not None:
        df = df.head(limit)

    return df


def serialize_record(record: Dict[str, Any]) -> bytes:
    return json.dumps(record, allow_nan=False).encode("utf-8")


def main() -> None:
    args = parse_args()

    df = load_trips(args.url, args.limit)

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=serialize_record,
    )

    t0 = time.time()
    sent = 0

    for record in df.to_dict(orient="records"):
        cleaned = {key: normalize_value(value) for key, value in record.items()}
        producer.send(args.topic, value=cleaned)
        sent += 1

    producer.flush()
    t1 = time.time()

    print(f"sent {sent} messages to {args.topic}")
    print(f"took {(t1 - t0):.2f} seconds")


if __name__ == "__main__":
    main()

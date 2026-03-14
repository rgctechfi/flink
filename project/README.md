# Homework - Project Folder

This folder contains everything you need to complete the PyFlink homework using the workshop code as a base.

## Prerequisites

- Docker + Docker Compose
- `uv` (or a Python environment with `kafka-python`, `pandas`, `pyarrow`)

## Start the Services

From `project/`:

```bash
docker compose up -d --build
```

Create the topic:

```bash
docker compose exec redpanda rpk topic create green-trips
```

## Orchestration Script

You can use the helper script to run the workflow end-to-end or step by step:

```bash
./run_all.sh all
```

Available commands:

- `up` build and start services
- `topic` create the `green-trips` topic
- `tables` create PostgreSQL tables for Q4–Q6
- `jobs` submit the three Flink jobs (detached)
- `producer` send the dataset to Kafka
- `down` stop and remove containers and volumes

## Python Scripts (producer/consumer)

Run the scripts from the repo root so `uv` can find `pyproject.toml`:

```bash
uv run python project/src/producers/producer_green_trips.py
```

Or, from `project/`:

```bash
uv --project .. run python src/producers/producer_green_trips.py
```

## Question 2 — Producer (Green Trips)

```bash
uv run python project/src/producers/producer_green_trips.py
```

Useful options:

- `--limit 10000` for a quick test
- `--bootstrap-server localhost:9092` (default)

## Question 3 — Consumer (trip_distance > 5)

```bash
uv run python project/src/consumers/consumer_green_trips_count.py
```

## Create the PostgreSQL Tables (Q4–Q6)

```bash
cat sql/create_tables.sql | docker compose exec -T postgres psql -U postgres -d postgres
```

## Question 4 — 5-Minute Tumbling Window (PULocationID)

```bash
docker compose exec jobmanager ./bin/flink run \
  -py /opt/src/job/green_trips_tumbling_5m_job.py \
  --pyFiles /opt/src -d
```

SQL query:

```sql
SELECT PULocationID, num_trips
FROM green_trips_tumbling_5m
ORDER BY num_trips DESC
LIMIT 3;
```

## Question 5 — 5-Minute Session Window

```bash
docker compose exec jobmanager ./bin/flink run \
  -py /opt/src/job/green_trips_session_5m_job.py \
  --pyFiles /opt/src -d
```

SQL query:

```sql
SELECT PULocationID, num_trips
FROM green_trips_session_5m
ORDER BY num_trips DESC
LIMIT 1;
```

## Question 6 — 1-Hour Tumbling Window (tip_amount)

```bash
docker compose exec jobmanager ./bin/flink run \
  -py /opt/src/job/green_trips_tip_hourly_job.py \
  --pyFiles /opt/src -d
```

SQL query:

```sql
SELECT window_start, total_tip_amount
FROM green_trips_tip_hourly
ORDER BY total_tip_amount DESC
LIMIT 1;
```

## Notes

- Flink jobs run continuously. Let them run for 1–2 minutes, then query PostgreSQL.
- If you rerun the producer multiple times, delete and recreate the topic:

```bash
docker compose exec redpanda rpk topic delete green-trips
docker compose exec redpanda rpk topic create green-trips
```

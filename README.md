<h1 align="center">
  <span>𝘼𝙥𝙖𝙘𝙝𝙚 𝙁𝙡𝙞𝙣𝙠</span>
</h1>

<p align="center">
  <img src="./ressources/pictures/apache_flink_banner.jpg" alt="Apache Flink banner" width="720" />
</p>

<p align="center">
  <em>Stream processing with PyFlink: Redpanda/Kafka, Flink SQL/Table API, PostgreSQL sinks, and homework deliverables</em>
</p>

## Overview

This repository consolidates Module 7 (Streaming with Flink) work:

- a full workshop guide to build a streaming pipeline end to end
- a homework/project brief with the tasks and submission details
- reference images and a minimal Python project scaffold

## Environment

From `pyproject.toml`:

- Python `>= 3.13`

Workshop prerequisites (see `workshop/README.md`):

- Docker and Docker Compose
- `uv` (recommended) or `pip` for Python deps
- A SQL client (pgcli, DBeaver, pgAdmin, DataGrip)

## Quick Start

Minimal local check:

```bash
uv sync
source .venv/bin/activate
python main.py
```

For the full streaming stack, follow `workshop/README.md`.

## Data Sources Used

- Yellow Taxi November 2025 parquet (workshop examples):
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet`
- Green Taxi October 2019 CSV (homework):
  - `https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz`

## Repository Map

```text
.
├── README.md
├── README_spark.md
├── pyproject.toml
├── main.py
├── ressources/
│   └── pictures/
│       ├── apache_flink_banner.jpg
│       └── apache_flink_logo.png
├── workshop/
│   └── README.md
└── project/
    └── homework.md
```

## Workshop Track

The workshop walks through a real-time pipeline built step by step:

```
Producer (Python) -> Kafka (Redpanda) -> Flink -> PostgreSQL
```

<p align="center">
  <img src="./ressources/pictures/workshop_flow.png" alt="Apache Flink logo" width="500" />
</p>

Key topics covered:

- Redpanda (Kafka-compatible broker) setup and rationale
- Python producer and consumer with schema serialization
- PostgreSQL sink wiring and validation
- PyFlink jobs (pass-through and windowed aggregation)
- Offsets, checkpoints, and watermarking behavior
- Window types: tumbling, sliding, session
- Cleanup and operational notes

Full instructions and code walkthroughs live in `workshop/README.md`.

## Homework (Project)

The homework focuses on streaming with PyFlink using the Green Taxi dataset.

Setup requirements (same stack as the workshop):

- Redpanda
- Flink JobManager and TaskManager
- PostgreSQL

Core tasks:

- identify the Redpanda version from `rpk`
- create a `green-trips` topic
- validate Kafka connectivity from Python
- publish the trip dataset to Kafka
- implement a 5-minute session window with watermarks

Submission details and the full question text are in `project/homework.md`.

<p align="center">
  <img src="./ressources/pictures/apache_flink_logo.png" alt="Apache Flink logo" width="220" />
</p>

## Useful Operational Notes

- Flink UI: `http://localhost:8081`
- Redpanda (Kafka): `localhost:9092`
- PostgreSQL: `localhost:5432`

## Related Files

- Workshop guide: `workshop/README.md`
- Homework brief: `project/homework.md`
- Assets: `ressources/pictures/`

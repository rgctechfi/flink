#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${PROJECT_DIR}/.." && pwd)"
COMPOSE=(docker compose -f "${PROJECT_DIR}/docker-compose.yml")

usage() {
  cat <<'USAGE'
Usage: ./run_all.sh [command]

Commands:
  up        Build and start services (docker compose up -d --build)
  topic     Create the green-trips topic in Redpanda
  tables    Create PostgreSQL tables for Q4–Q6
  jobs      Submit all three Flink jobs (detached)
  producer  Send the green trips dataset to Kafka
  down      Stop and remove containers and volumes
  all       Run: up -> topic -> tables -> jobs -> producer

Examples:
  ./run_all.sh all
  ./run_all.sh up
  ./run_all.sh producer
USAGE
}

wait_for_redpanda() {
  echo "Waiting for Redpanda to be ready..."
  for _ in {1..20}; do
    if "${COMPOSE[@]}" exec -T redpanda rpk cluster info >/dev/null 2>&1; then
      echo "Redpanda is ready."
      return 0
    fi
    sleep 2
  done
  echo "Redpanda did not become ready in time."
  return 1
}

cmd_up() {
  "${COMPOSE[@]}" up -d --build
  wait_for_redpanda
}

cmd_topic() {
  "${COMPOSE[@]}" exec -T redpanda rpk topic create green-trips || true
}

cmd_tables() {
  cat "${PROJECT_DIR}/sql/create_tables.sql" | "${COMPOSE[@]}" exec -T postgres psql -U postgres -d postgres
}

cmd_jobs() {
  "${COMPOSE[@]}" exec jobmanager ./bin/flink run \
    -py /opt/src/job/green_trips_tumbling_5m_job.py \
    --pyFiles /opt/src -d

  "${COMPOSE[@]}" exec jobmanager ./bin/flink run \
    -py /opt/src/job/green_trips_session_5m_job.py \
    --pyFiles /opt/src -d

  "${COMPOSE[@]}" exec jobmanager ./bin/flink run \
    -py /opt/src/job/green_trips_tip_hourly_job.py \
    --pyFiles /opt/src -d
}

cmd_producer() {
  uv --project "${ROOT_DIR}" run python "${PROJECT_DIR}/src/producers/producer_green_trips.py"
}

cmd_down() {
  "${COMPOSE[@]}" down -v
}

cmd_all() {
  cmd_up
  cmd_topic
  cmd_tables
  cmd_jobs
  cmd_producer
}

case "${1:-}" in
  up) cmd_up ;;
  topic) cmd_topic ;;
  tables) cmd_tables ;;
  jobs) cmd_jobs ;;
  producer) cmd_producer ;;
  down) cmd_down ;;
  all) cmd_all ;;
  -h|--help|help|"") usage ;;
  *)
    echo "Unknown command: ${1}" >&2
    usage
    exit 1
    ;;
esac

#!/usr/bin/env bash
set -e

# .env 로딩(있으면)
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

TOPIC_RAW="${TOPIC_RAW:-bike_raw}"

docker compose exec -T kafka bash -lc "
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic ${TOPIC_RAW} --partitions 1 --replication-factor 1
kafka-topics --bootstrap-server kafka:9092 --list
"

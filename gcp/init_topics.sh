#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <broker_address>"
  exit 1
fi

TOPICS=("read-stock-data" "aggregated-stock-data" "anomaly-stock-data")

BROKER="$1"

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  kafka-topics.sh --bootstrap-server "$BROKER" --topic "$TOPIC" --create --partitions 1 --replication-factor 1
done

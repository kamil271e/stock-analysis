#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <broker_address>"
  exit 1
fi

BROKER_ADDRESS="$1"
TOPICS=$(kafka-topics.sh --bootstrap-server "$BROKER_ADDRESS" --list)

if [ -z "$TOPICS" ]; then
  echo "No topics found."
  exit 0
fi

for TOPIC in $TOPICS; do
  echo "Deleting topic: $TOPIC"
  kafka-topics.sh --bootstrap-server "$BROKER_ADDRESS" --delete --topic "$TOPIC"
done

echo "All topics have been deleted."

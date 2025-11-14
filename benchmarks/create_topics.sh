#!/bin/bash

# Configuration
BROKER="localhost:9092"   # Change this to your Kafka broker address
TOPIC_PREFIX="test-topic" # Base name for topics
PARTITIONS=3              # Number of partitions per topic
REPLICATION_FACTOR=1      # Replication factor

# Loop 100 times
for i in $(seq 1 100); do
    TOPIC_NAME="${TOPIC_PREFIX}-${i}"
    echo "Creating topic: $TOPIC_NAME"
    kafka-topics.sh --create \
        --topic "$TOPIC_NAME" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR" \
        --bootstrap-server "$BROKER"
done

echo "✅ Done creating 100 topics!"

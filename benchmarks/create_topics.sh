#!/bin/bash

# Configuration
BROKER="localhost:9092" # Change this to your Kafka broker address
PARTITIONS=3            # Number of partitions per topic
REPLICATION_FACTOR=1    # Replication factor
NUM_TOPICS=100          # Number of topics per format

echo "=================================================="
echo "  Creating Kafka Topics for Benchmarks"
echo "=================================================="

# Create JSON topics (test-topic-json-0 to test-topic-json-99)
echo ""
echo "📊 Creating 100 JSON topics..."
for i in $(seq 0 $((NUM_TOPICS - 1))); do
    TOPIC_NAME="test-topic-json-${i}"
    echo "  Creating topic: $TOPIC_NAME"
    kafka-topics.sh --create \
        --topic "$TOPIC_NAME" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR" \
        --bootstrap-server "$BROKER" 2>&1 | grep -v "already exists" || true
done

# Create Avro topics (test-topic-avro-0 to test-topic-avro-99)
echo ""
echo "📊 Creating 100 Avro topics..."
for i in $(seq 0 $((NUM_TOPICS - 1))); do
    TOPIC_NAME="test-topic-avro-${i}"
    echo "  Creating topic: $TOPIC_NAME"
    kafka-topics.sh --create \
        --topic "$TOPIC_NAME" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION_FACTOR" \
        --bootstrap-server "$BROKER" 2>&1 | grep -v "already exists" || true
done

echo ""
echo "✅ Done creating 200 topics (100 JSON + 100 Avro)!"
echo "=================================================="

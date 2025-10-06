#!/bin/bash
set -e

# Step 1: List topics first (run synchronously)
python3 benchmark_listtopics.py

# Step 2: Run producer and consumer simultaneously
python3 benchmark_produce.py &
python3 benchmark_consume.py &

# Step 3: Wait for both background processes to finish
wait

echo "✅ List topics, Produce and consume benchmarks completed!"

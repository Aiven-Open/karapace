#!/bin/bash
set -e

echo "=================================================="
echo "  Karapace REST Proxy Benchmarks"
echo "=================================================="

# Step 1: List topics first (run synchronously)
python3 benchmark_listtopics.py

echo ""
echo "📊 JSON Format Benchmarks"
echo "--------------------------------------------------"

# Step 2: JSON - Produce then consume
python3 benchmark_produce.py
python3 benchmark_consume.py

echo ""
echo "📊 Avro Format Benchmarks"
echo "--------------------------------------------------"

# Step 3: Avro - Produce then consume
python3 benchmark_produce_avro.py
python3 benchmark_consume_avro.py

echo ""
echo "✅ All benchmarks completed (JSON + Avro)!"
echo "=================================================="

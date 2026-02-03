# Karapace REST Proxy Benchmarks

Benchmarks for measuring producer and consumer performance with different serialization formats.

## Files

### JSON Format (Simple Serialization)
- `benchmark_produce.py` - Produce JSON messages to Kafka
- `benchmark_consume.py` - Consume JSON messages from Kafka

### Avro Format (Schema Registry)
- `benchmark_produce_avro.py` - Produce Avro messages with schema validation
- `benchmark_consume_avro.py` - Consume Avro messages with parallel deserialization

### Utilities
- `benchmark_listtopics.py` - List available topics
- `run_benchmarks.sh` - Run all benchmarks (JSON + Avro)
- `create_topics.sh` - Create 200 topics (100 JSON + 100 Avro) in Kafka

## Setup

### Create Topics (First Time Only)

Before running benchmarks, create the required 200 topics:

```bash
# Edit create_topics.sh to set your Kafka broker address if needed
# Default: localhost:9092

./create_topics.sh
```

This creates:
- 100 JSON topics: `test-topic-json-0` to `test-topic-json-99`
- 100 Avro topics: `test-topic-avro-0` to `test-topic-avro-99`

## Running Benchmarks

### Run all benchmarks:
```bash
./run_benchmarks.sh
```

### Run individual benchmarks:

**JSON Format:**
```bash
python3 benchmark_produce.py
python3 benchmark_consume.py
```

**Avro Format:**
```bash
python3 benchmark_produce_avro.py
python3 benchmark_consume_avro.py
```

## Configuration

Default settings (edit the files to change):
- **Messages**: 50,000 per benchmark (distributed across all topics)
- **Batch size**: 1,000 messages per request
- **Topics**:
  - JSON: 100 topics (`test-topic-json-0` to `test-topic-json-99`)
  - Avro: 100 topics (`test-topic-avro-0` to `test-topic-avro-99`)
- **Distribution**: Round-robin across all topics

## Multi-Topic Testing

The benchmarks use **100 topics per format** to simulate realistic production workloads:

- **Producer**: Distributes messages round-robin across all 100 topics
- **Consumer**: Subscribes to all 100 topics simultaneously
- **Schema Registry**: Registers schemas for all topics

This approach tests:
- Metadata management across many topics
- Consumer rebalancing and partition assignment
- Schema caching performance

## Expected Performance

### Consumer Optimizations

**JSON Format** (synchronous fast path):
- ~35% improvement from baseline
- No async overhead
- Tested across 100 topics

**Avro Format** (parallel deserialization):
- **2-5x faster** than serial deserialization
- Parallel schema lookups and validation across topics
- Benefits increase with batch size and topic count

### Producer Performance

Both formats use the same producer optimization (simplified gather pattern) across 100 topics.

## Output

Results are saved to `karapace5_benchmarks.csv` with metrics:
- Average latency per batch
- P95 latency
- Total messages produced/consumed
- Format type (JSON vs Avro)

## Requirements

- Karapace REST Proxy running on `localhost:8082`
- Karapace Schema Registry running on `localhost:8081`
- Python 3.10+ with `httpx` library

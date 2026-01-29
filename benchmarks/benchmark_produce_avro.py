"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details

Benchmark for producing Avro-formatted messages to Kafka REST Proxy
"""

import asyncio
import httpx
import time
import csv
from statistics import mean

VERSION = "karapace5"
FILE_NAME = "karapace5_benchmarks.csv"
# Topics to produce to - 100 Avro topics
NUM_TOPICS = 100
TOPIC_PREFIX = "test-topic-avro"
TOPICS = [f"{TOPIC_PREFIX}-{i}" for i in range(NUM_TOPICS)]
BASE_URL = "http://localhost:8082"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
N_MESSAGES = 50000  # Total messages to produce (distributed across all topics)
BATCH_SIZE = 1000  # Number of messages per request
TIMEOUT = 10.0  # HTTP timeout (seconds)


# Generate Avro payload batch with schema_id
def make_avro_payload(start_index: int, batch_size: int, schema_id: int):
    return {
        "value_schema_id": schema_id,
        "records": [{"value": {"name": f"user_{start_index + i}", "age": start_index + i}} for i in range(batch_size)],
    }


async def measure_produce_latency(
    base_url: str, topics: list, schema_id: int, n_messages: int = N_MESSAGES, batch_size: int = BATCH_SIZE
):
    latencies = []
    responses = []

    headers = {"Content-Type": "application/vnd.kafka.avro.v2+json"}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        total_batches = (n_messages + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            # Round-robin across topics
            topic_idx = batch_num % len(topics)
            topic = topics[topic_idx]
            path = f"/topics/{topic}"

            start_index = batch_num * batch_size
            current_batch_size = min(batch_size, n_messages - start_index)
            payload = make_avro_payload(start_index, current_batch_size, schema_id)

            t0 = time.perf_counter()
            r = await client.post(f"{base_url}{path}", json=payload, headers=headers)
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)
            responses.append(r.text)

            assert r.status_code == 200, f"{base_url}{path} returned {r.status_code}: {r.text}"
            if batch_num % 10 == 0:  # Print every 10th batch to reduce noise
                print(
                    f"[Batch {batch_num+1}/{total_batches}] Sent {current_batch_size} Avro messages to {topic} in {elapsed:.2f} ms"
                )

    return latencies, responses


async def run_benchmark(schema_id: int):
    results = []
    print(f"\n🔹 Benchmarking Avro produce to {VERSION} at {BASE_URL}")
    print(f"   Using {len(TOPICS)} topics: {TOPIC_PREFIX}-0 to {TOPIC_PREFIX}-{len(TOPICS)-1}")
    latencies, responses = await measure_produce_latency(BASE_URL, TOPICS, schema_id)
    avg = mean(latencies)
    p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]
    total_messages = N_MESSAGES

    results.append((VERSION, f"{TOPIC_PREFIX} (100 Avro topics)", avg, p95, total_messages))
    print(f"  ✅ Avg latency per batch: {avg:.2f} ms, P95: {p95:.2f} ms, total messages produced: {total_messages}")

    # Write CSV results
    with open(FILE_NAME, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Version", "Endpoint", "Avg latency per batch (ms)", "P95 latency per batch (ms)", "Total messages produced"]
        )
        writer.writerows(results)
        writer.writerow([])

    print("\n✅ Results saved to", FILE_NAME)


async def register_avro_schemas():
    """Register Avro schemas for all topics and return a schema ID."""
    schema_def = {
        "schema": '{"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}, {"name": "age", "type": "int"}]}'
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        print(f"📝 Registering Avro schemas for {len(TOPICS)} topics...")
        registered = 0
        schema_id = None

        for topic in TOPICS:
            subject = f"{topic}-value"
            post_url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"

            try:
                r = await client.post(post_url, json=schema_def)
                if r.status_code in (200, 409):
                    registered += 1
                    # Get schema ID from first topic (all use same schema)
                    if schema_id is None:
                        get_url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
                        r_get = await client.get(get_url)
                        if r_get.status_code == 200:
                            schema_id = r_get.json()["id"]
            except Exception as e:
                print(f"⚠️ Error registering schema for {subject}: {e}")

        print(f"✅ Registered/verified Avro schemas for {registered}/{len(TOPICS)} topics, schema_id: {schema_id}")
        if schema_id is None:
            raise RuntimeError("Failed to get schema ID")
        return schema_id


if __name__ == "__main__":
    schema_id = asyncio.run(register_avro_schemas())
    asyncio.run(run_benchmark(schema_id))

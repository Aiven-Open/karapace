"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details
"""

import asyncio
import httpx
import time
import csv
from statistics import mean

VERSION = "karapace5"
FILE_NAME = "karapace5_benchmarks.csv"
# Topics to produce to - 100 JSON topics
NUM_TOPICS = 100
TOPIC_PREFIX = "test-topic-json"
TOPICS = [f"{TOPIC_PREFIX}-{i}" for i in range(NUM_TOPICS)]
BASE_URL = "http://localhost:8082"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
N_MESSAGES = 50000  # Total messages to produce (distributed across all topics)
BATCH_SIZE = 1000  # Number of messages per request
TIMEOUT = 10.0  # HTTP timeout (seconds)


# Generate a payload batch
def make_payload(start_index: int, batch_size: int = BATCH_SIZE):
    return {"records": [{"value": {"age": start_index + i}} for i in range(batch_size)]}


async def measure_produce_latency(base_url: str, topics: list, n_messages: int = N_MESSAGES, batch_size: int = BATCH_SIZE):
    latencies = []
    responses = []

    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        total_batches = (n_messages + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            # Round-robin across topics
            topic_idx = batch_num % len(topics)
            topic = topics[topic_idx]
            path = f"/topics/{topic}"

            start_index = batch_num * batch_size
            current_batch_size = min(batch_size, n_messages - start_index)
            payload = make_payload(start_index, current_batch_size)

            t0 = time.perf_counter()
            r = await client.post(f"{base_url}{path}", json=payload, headers=headers)
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)
            responses.append(r.text)

            assert r.status_code == 200, f"{base_url}{path} returned {r.status_code}"
            if batch_num % 10 == 0:  # Print every 10th batch to reduce noise
                print(
                    f"[Batch {batch_num+1}/{total_batches}] Sent {current_batch_size} messages to {topic} in {elapsed:.2f} ms"
                )

    return latencies, responses


async def run_benchmark():
    results = []
    print(f"\n🔹 Benchmarking JSON produce to {VERSION} at {BASE_URL}")
    print(f"   Using {len(TOPICS)} topics: {TOPIC_PREFIX}-0 to {TOPIC_PREFIX}-{len(TOPICS)-1}")
    latencies, responses = await measure_produce_latency(BASE_URL, TOPICS)
    avg = mean(latencies)
    p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]
    total_messages = N_MESSAGES

    results.append((VERSION, f"{TOPIC_PREFIX} (100 topics)", avg, p95, total_messages))
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


async def register_schemas():
    """Register JSON schema for all topics."""
    schema_def = {
        "schemaType": "JSON",
        "schema": '{"type": "object", "properties": {"age": {"type": "integer"}}, "required": ["age"]}',
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        print(f"📝 Registering JSON schemas for {len(TOPICS)} topics...")
        registered = 0
        for topic in TOPICS:
            subject = f"{topic}-value"
            post_url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"

            try:
                r = await client.post(post_url, json=schema_def)
                if r.status_code in (200, 409):
                    registered += 1
            except Exception as e:
                print(f"⚠️ Error registering schema for {subject}: {e}")

        print(f"✅ Registered/verified schemas for {registered}/{len(TOPICS)} topics")


if __name__ == "__main__":
    asyncio.run(register_schemas())
    asyncio.run(run_benchmark())

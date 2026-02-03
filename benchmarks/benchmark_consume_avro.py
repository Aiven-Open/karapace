"""
Copyright (c) 2026 Aiven Ltd
See LICENSE for details

Benchmark for consuming Avro-formatted messages from Kafka REST Proxy
"""

import asyncio
import httpx
import time
import csv
from statistics import mean

VERSION = "karapace5"
FILE_NAME = "karapace5_benchmarks.csv"

# Consumer setup - 100 Avro topics
NUM_TOPICS = 100
TOPIC_PREFIX = "test-topic-avro"
TOPICS = [f"{TOPIC_PREFIX}-{i}" for i in range(NUM_TOPICS)]
CONSUMER_GROUP = "avro_consumer_group"
CONSUMER_INSTANCE = "avro_consumer_instance"
BASE_URL = "http://localhost:8082"
BASE_CONSUMER_URL = f"/consumers/{CONSUMER_GROUP}/instances/{CONSUMER_INSTANCE}"

TIMEOUT = 60.0
TOTAL_MESSAGES = 50000


async def create_consumer_instance(base_url: str):
    """Create an Avro consumer instance in the specified consumer group."""
    url = f"{base_url}/consumers/{CONSUMER_GROUP}"
    payload = {
        "name": CONSUMER_INSTANCE,
        "format": "avro",  # ← Avro format
        "auto.offset.reset": "earliest",
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(
            url,
            json=payload,
            headers={
                "Content-Type": "application/vnd.kafka.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            },
        )

        if r.status_code in (200, 409):
            print(f"✅ Avro consumer instance '{CONSUMER_INSTANCE}' ready in group '{CONSUMER_GROUP}'")
        else:
            raise RuntimeError(f"❌ Failed to create consumer: {r.status_code} - {r.text}")


async def subscribe_to_topics(base_url: str):
    """Subscribe the consumer instance to all Avro topics."""
    url = f"{base_url}{BASE_CONSUMER_URL}/subscription"
    payload = {"topics": TOPICS}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(
            url,
            json=payload,
            headers={
                "Content-Type": "application/vnd.kafka.v2+json",
            },
        )

        if r.status_code == 204:
            print(f"✅ Subscribed to {len(TOPICS)} Avro topics: {TOPIC_PREFIX}-0 to {TOPIC_PREFIX}-{len(TOPICS)-1}")
        else:
            raise RuntimeError(f"❌ Subscription failed: {r.status_code} - {r.text}")


async def measure_consume_latency(base_url: str, total_messages: int):
    """Measure consumption latency for Avro messages."""
    latencies = []
    responses = []

    headers = {"Accept": "application/vnd.kafka.avro.v2+json"}  # ← Avro accept header
    # Add timeout parameter and max_bytes for better performance
    consume_url = f"{base_url}{BASE_CONSUMER_URL}/records?timeout=1000&max_bytes=10485760"  # 1s timeout, 10MB max

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        consumed_messages = []
        while len(consumed_messages) < total_messages:
            t0 = time.perf_counter()
            r = await client.get(consume_url, headers=headers)
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)

            if r.status_code != 200:
                print(f"⚠️ Error {r.status_code}: {r.text}")
                await asyncio.sleep(1)
                continue

            messages = r.json()
            if messages:
                consumed_messages.extend(messages)
                responses.append(messages)
                if len(consumed_messages) % 5000 < len(messages):  # Print every ~5000 messages
                    print(f"Fetched {len(messages)} Avro messages, total {len(consumed_messages)}")
            elif len(consumed_messages) >= total_messages:
                # All messages consumed
                break
            # No sleep between fetches - fetch as fast as possible

    return latencies, responses


async def delete_consumer_instance(base_url: str):
    """Clean up consumer instance."""
    url = f"{base_url}{BASE_CONSUMER_URL}"

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.delete(
            url,
            headers={
                "Accept": "application/vnd.kafka.v2+json",
            },
        )

        if r.status_code == 204:
            print(f"🧹 Deleted consumer instance '{CONSUMER_INSTANCE}'")
        else:
            print(f"⚠️ Failed to delete consumer instance: {r.status_code} - {r.text}")


async def run_benchmark():
    results = []
    print(f"\n🔹 Benchmarking Avro consume from {VERSION} at {BASE_URL}{BASE_CONSUMER_URL}/records")
    latencies, responses = await measure_consume_latency(BASE_URL, total_messages=TOTAL_MESSAGES)
    avg = mean(latencies)
    p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]

    results.append(
        (VERSION, f"{TOPIC_PREFIX} (100 Avro topics)", BASE_CONSUMER_URL, avg, p95, sum(len(r) for r in responses))
    )
    print(f"✅ Avg: {avg:.2f} ms, P95: {p95:.2f} ms, total consumed: {sum(len(r) for r in responses)}")

    # Cleanup
    await delete_consumer_instance(BASE_URL)

    # Save results
    with open(FILE_NAME, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Version", "Topic", "Endpoint", "Avg latency (ms)", "P95 latency (ms)", "Total messages consumed"])
        writer.writerows(results)
        writer.writerow([])

    print(f"\n✅ Results saved to {FILE_NAME}")


async def main():
    """Main entrypoint for setup + benchmark."""
    print(f"\n🔹 Setting up Avro consumer for {VERSION} at {BASE_URL}")
    await create_consumer_instance(BASE_URL)
    await subscribe_to_topics(BASE_URL)
    await run_benchmark()


if __name__ == "__main__":
    asyncio.run(main())

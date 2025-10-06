import asyncio

import httpx
import time
import csv
from statistics import mean

# create consumer instance
#
# curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
#     --data '{"name": "json_consumer_instance", "format": "json", "auto.offset.reset": "earliest"}' \
#     http://localhost:8082/consumers/json_consumer_group
#
# subscribe to topic
#
# curl -X POST http://localhost:8082/consumers/json_consumer_group/instances/json_consumer_instance/subscription \
#   -H "Content-Type: application/vnd.kafka.v2+json" \
#   --data '{ "topics": ["test-topic-2"] }'

# Karapace instances
VERSIONS = {
    "karapace5": "http://localhost:8082",
}

FILE_NAME = "karapace5_benchmarks.csv"

# Consumer endpoint
TOPIC = "test-topic-2"
ENDPOINT = "/consumers/json_consumer_group/instances/json_consumer_instance/records"

N_REQUESTS = 1  # number of consume requests
TIMEOUT = 60.0
total_messages = 50000

async def measure_consume_latency(base_url: str, path: str, total_messages: int):
    latencies = []
    responses = []

    headers = {"Accept": "application/vnd.kafka.json.v2+json"}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        consumed_messages = []
        while len(consumed_messages) < total_messages:
            t0 = time.perf_counter()
            r = await client.get(f"{base_url}{path}", headers=headers)
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)

            if r.status_code != 200:
                print(f"Error {r.status_code}: {r.text}")
                continue

            messages = r.json()
            consumed_messages.extend(messages)
            responses.append(messages)
            print(f"Fetched {len(messages)} messages, total {len(consumed_messages)}")

    return latencies, responses


async def run_benchmark():
    results = []
    for name, base_url in VERSIONS.items():
        print(f"\n🔹 Benchmarking consume from {name} at {base_url}{ENDPOINT}")
        latencies, responses = await measure_consume_latency(base_url, ENDPOINT, total_messages=total_messages)
        avg = mean(latencies)
        p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]

        results.append((name, ENDPOINT, avg, p95, sum(len(r) for r in responses)))
        print(f"  Avg: {avg:.2f} ms, P95: {p95:.2f} ms, total messages consumed: {sum(len(r) for r in responses)}")


# Write CSV results
    with open(FILE_NAME, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Version", "Endpoint", "Avg latency per batch (ms)", "P95 latency per batch (ms)", "Total messages consumed"])
        writer.writerows(results)
        writer.writerow([])

    print("\n✅ Results saved to", FILE_NAME)

if __name__ == "__main__":
    asyncio.run(run_benchmark())

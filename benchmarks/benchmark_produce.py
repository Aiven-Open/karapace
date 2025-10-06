import asyncio

import httpx
import time
import csv
from statistics import mean

# Create a schema on the topic before running this script
#
# curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#                 --data '{
# "schemaType": "JSON",
# "schema": "{\"type\": \"object\", \"properties\": {\"age\": {\"type\": \"integer\"}}, \"required\": [\"age\"]}"
# }' \
#   http://localhost:8081/subjects/test-topic-2-value/versions


# Karapace instances
VERSIONS = {
    "karapace5": "http://localhost:8082",
}

FILE_NAME = "karapace5_benchmarks.csv"

# Topic to produce to
TOPIC = "test-topic-2"
ENDPOINT = f"/topics/{TOPIC}"

N_MESSAGES = 50000        # Total messages to produce
BATCH_SIZE = 1000         # Number of messages per request
TIMEOUT = 10.0           # HTTP timeout (seconds)

# Generate a payload batch
def make_payload(start_index: int, batch_size: int = BATCH_SIZE):
    return {
        "records": [{"value": {"age": start_index + i}} for i in range(batch_size)]
    }

async def measure_produce_latency(base_url: str, path: str, n_messages: int = N_MESSAGES, batch_size: int = BATCH_SIZE):
    latencies = []
    responses = []

    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        total_batches = (n_messages + batch_size - 1) // batch_size
        for batch_num in range(total_batches):
            start_index = batch_num * batch_size
            current_batch_size = min(batch_size, n_messages - start_index)
            payload = make_payload(start_index, current_batch_size)

            t0 = time.perf_counter()
            r = await client.post(f"{base_url}{path}", json=payload, headers=headers)
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)
            responses.append(r.text)

            assert r.status_code == 200, f"{base_url}{path} returned {r.status_code}"
            print(f"[Batch {batch_num+1}/{total_batches}] Sent {current_batch_size} messages in {elapsed:.2f} ms")

    return latencies, responses


async def run_benchmark():
    results = []
    for name, base_url in VERSIONS.items():
        print(f"\n🔹 Benchmarking produce to {name} at {base_url}{ENDPOINT}")
        latencies, responses = await measure_produce_latency(base_url, ENDPOINT)
        avg = mean(latencies)
        p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]
        total_messages = N_MESSAGES

        results.append((name, ENDPOINT, avg, p95, total_messages))
        print(f"  ✅ Avg latency per batch: {avg:.2f} ms, P95: {p95:.2f} ms, total messages produced: {total_messages}")

    # Write CSV results
    with open(FILE_NAME, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Version", "Endpoint", "Avg latency per batch (ms)", "P95 latency per batch (ms)", "Total messages produced"])
        writer.writerows(results)
        writer.writerow([])

    print("\n✅ Results saved to", FILE_NAME)


if __name__ == "__main__":
    asyncio.run(run_benchmark())

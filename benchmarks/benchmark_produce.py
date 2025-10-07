import asyncio
import httpx
import time
import csv
from statistics import mean

VERSION = "karapace5"
FILE_NAME = "karapace5_benchmarks.csv"
# Topic to produce to
TOPIC = "test-topic-2"
SUBJECT = f"{TOPIC}-value"
ENDPOINT = f"/topics/{TOPIC}"
BASE_URL = "http://localhost:8082"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
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
    print(f"\n🔹 Benchmarking produce to {VERSION} at {BASE_URL}{ENDPOINT}")
    latencies, responses = await measure_produce_latency(BASE_URL, ENDPOINT)
    avg = mean(latencies)
    p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]
    total_messages = N_MESSAGES

    results.append((VERSION, ENDPOINT, avg, p95, total_messages))
    print(f"  ✅ Avg latency per batch: {avg:.2f} ms, P95: {p95:.2f} ms, total messages produced: {total_messages}")

    # Write CSV results
    with open(FILE_NAME, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Version", "Endpoint", "Avg latency per batch (ms)", "P95 latency per batch (ms)", "Total messages produced"])
        writer.writerows(results)
        writer.writerow([])

    print("\n✅ Results saved to", FILE_NAME)


async def register_schema():
    """Try registering the schema; ignore if it already exists."""
    schema_def = {
        "schemaType": "JSON",
        "schema": '{"type": "object", "properties": {"age": {"type": "integer"}}, "required": ["age"]}',
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        post_url = f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions"

        try:
            r = await client.post(post_url, json=schema_def)
            if r.status_code == 200:
                print(f"✅ Schema registered successfully for {SUBJECT}")
            elif r.status_code == 409:
                print(f"ℹ️ Schema already exists for {SUBJECT}, continuing...")
            else:
                print(f"⚠️ Unexpected response ({r.status_code}): {r.text}")
        except Exception as e:
            print(f"❌ Error during schema registration: {e}")
            raise

if __name__ == "__main__":
    asyncio.run(register_schema())
    asyncio.run(run_benchmark())

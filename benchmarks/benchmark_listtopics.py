"""
Copyright (c) 2025 Aiven Ltd
See LICENSE for details
"""

import asyncio
import json

import httpx
import time
import csv
from statistics import mean


# Make sure topics exist (with create_topics.sh) before this script is run

# Karapace instances
VERSIONS = {
    "karapace5": "http://localhost:8082",
}

FILE_NAME = "karapace5_benchmarks.csv"

# Endpoints to benchmark
ENDPOINTS = [
    "/topics",
]

NUMBER_OF_TOPICS = 102
N_REQUESTS = 100  # use smaller number for testing responses
TIMEOUT = 10.0


async def measure_latency(base_url: str, path: str, n: int = N_REQUESTS):
    latencies = []
    responses = []
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        for i in range(n):
            t0 = time.perf_counter()
            r = await client.get(f"{base_url}{path}")
            elapsed = (time.perf_counter() - t0) * 1000
            latencies.append(elapsed)
            text = r.text
            responses.append(text)
            # print(f"[{i+1}/{n}] {base_url}{path} -> {r.status_code}, response: {text[:200]}")  # print first 200 chars
            assert r.status_code == 200, f"{base_url}{path} returned {r.status_code}"

            # Assert at least 100 topics
            try:
                topics = json.loads(r.text)
                assert isinstance(topics, list), f"Expected list of topics, got {type(topics)}"
                assert len(topics) == NUMBER_OF_TOPICS, f"Expected at least 100 topics, got {len(topics)}"
            except json.JSONDecodeError:
                raise AssertionError(f"Response is not valid JSON: {r.text}")

    return latencies, responses


async def run_benchmark():
    results = []
    for name, base_url in VERSIONS.items():
        for endpoint in ENDPOINTS:
            print(f"\n🔹 Benchmarking {name} at {base_url}{endpoint}")
            latencies, responses = await measure_latency(base_url, endpoint)
            avg = mean(latencies)
            p95 = sorted(latencies)[int(0.95 * len(latencies)) - 1]

            # Save average, p95, and first response sample to CSV
            # results.append((name, endpoint, avg, p95, responses[0][:200]))
            results.append((name, endpoint, avg, p95, N_REQUESTS))

            print(f"  Avg: {avg:.2f} ms, P95: {p95:.2f} ms, first response: {responses[0][:200]}")

    # Write CSV results
    with open(FILE_NAME, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Version", "Endpoint", "Avg (ms)", "P95 (ms)", "Number of reqs"])
        writer.writerows(results)
        writer.writerow([])

    print("\n✅ Results saved to ", FILE_NAME)


if __name__ == "__main__":
    asyncio.run(run_benchmark())


import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import httpx

BASE_URL = "http://localhost:8100/v4"


def configure_bucket(client, bucket, method):
    policy = {
        "method": method,
        "n": 3,
        "w": 2,
        "r": 2,
        "k": 4,
        "m": 2
    }

    response = client.put(
        f"{BASE_URL}/buckets/{bucket}/policy",
        json=policy
    )
    response.raise_for_status()


def get_stats(client):
    response = client.get(f"{BASE_URL}/stats")
    response.raise_for_status()
    return response.json()


def create_file(path, size):
    with open(path, "wb") as output:
        remaining = size

        while remaining:
            block = os.urandom(
                min(1024 * 1024, remaining)
            )
            output.write(block)
            remaining -= len(block)


def benchmark(client, files, method, mode):
    bucket = f"bench-{method}-{int(time.time())}"

    configure_bucket(client, bucket, method)

    before = get_stats(client)
    before_bytes = before["physical_by_method"].get(
        method, 0
    )

    start = time.perf_counter()
    logical_bytes = 0
    successful = 0

    for index, path in enumerate(files):
        size = path.stat().st_size

        with path.open("rb") as source:
            response = client.post(
                f"{BASE_URL}/objects/{bucket}/file-{index}",
                files={
                    "file": (
                        path.name,
                        source,
                        "application/octet-stream"
                    )
                },
                data={"mode": mode}
            )

        response.raise_for_status()

        logical_bytes += size
        successful += 1

        if successful % 100 == 0:
            print(
                f"{method}: {successful}/{len(files)}",
                flush=True
            )

    elapsed = time.perf_counter() - start

    after = get_stats(client)
    after_bytes = after["physical_by_method"].get(
        method, 0
    )

    physical_bytes = after_bytes - before_bytes

    return {
        "method": method,
        "bucket": bucket,
        "files": successful,
        "logical_bytes": logical_bytes,
        "additional_physical_bytes": physical_bytes,
        "storage_overhead": round(
            physical_bytes / logical_bytes, 3
        ) if logical_bytes else 0,
        "upload_seconds": round(elapsed, 2),
        "throughput_MB_per_second": round(
            logical_bytes / (1024 * 1024) / elapsed, 2
        ) if elapsed else 0
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--workload",
        choices=["small", "large"],
        required=True
    )

    parser.add_argument(
        "--count",
        type=int,
        default=None
    )

    parser.add_argument(
        "--large-mb",
        type=int,
        default=100
    )

    parser.add_argument(
        "--mode",
        choices=["fixed", "cdc"],
        default="fixed"
    )

    args = parser.parse_args()

    count = args.count or (
        5000 if args.workload == "small" else 10
    )

    files = []

    with tempfile.TemporaryDirectory() as folder:
        directory = Path(folder)

        for index in range(count):
            if args.workload == "small":
                size = (1 + index % 50) * 1024
            else:
                size = args.large_mb * 1024 * 1024

            path = directory / f"file-{index}.bin"
            create_file(path, size)
            files.append(path)

        with httpx.Client(timeout=300) as client:
            replication = benchmark(
                client, files, "replication", args.mode
            )

            erasure = benchmark(
                client, files, "erasure", args.mode
            )

    result = {
        "workload": args.workload,
        "chunking_mode": args.mode,
        "replication": replication,
        "erasure_coding": erasure
    }

    output = Path(
        f"benchmark_{args.workload}_{args.mode}.json"
    )

    output.write_text(
        json.dumps(result, indent=2)
    )

    print(json.dumps(result, indent=2))
    print(f"Results saved to {output}")


if __name__ == "__main__":
    main()

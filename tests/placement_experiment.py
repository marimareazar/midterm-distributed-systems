
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0,
    str(ROOT / "metadata_service")
)

from ring import placement_movement


nodes = [
    "node-1",
    "node-2",
    "node-3",
    "node-4"
]

hashes = [
    hashlib.sha256(
        f"test-chunk-{i}".encode()
    ).hexdigest()
    for i in range(5000)
]

add_result = placement_movement(
    hashes,
    nodes,
    nodes + ["node-5"]
)

remove_result = placement_movement(
    hashes,
    nodes,
    nodes[:3]
)

print("=" * 50)
print("CONSISTENT HASHING PLACEMENT EXPERIMENT")
print("=" * 50)

print(f"Total chunk hashes: {len(hashes)}")
print("Virtual nodes per physical node: 128")

print("\nAdding Node 5:")
print(f"Moved chunks: {add_result['moved_chunks']}")
print(
    f"Movement percentage: "
    f"{add_result['movement_percentage']}%"
)

print("\nRemoving Node 4:")
print(f"Moved chunks: {remove_result['moved_chunks']}")
print(
    f"Movement percentage: "
    f"{remove_result['movement_percentage']}%"
)

print("=" * 50)

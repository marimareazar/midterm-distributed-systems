
import hashlib
import io
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0,
    str(ROOT / "metadata_service")
)

from chunking import (
    MB,
    MIN_SIZE,
    MAX_SIZE,
    chunk_hash,
    fixed_chunks,
    content_defined_chunks
)
from ring import (
    ConsistentHashRing,
    placement_movement
)


class TestChunking(unittest.TestCase):

    def test_fixed_chunking(self):
        data = os.urandom(10 * MB)

        chunks = list(
            fixed_chunks(io.BytesIO(data))
        )

        self.assertEqual(len(chunks), 3)

        self.assertEqual(
            [len(chunk) for chunk in chunks],
            [4 * MB, 4 * MB, 2 * MB]
        )

        self.assertEqual(b"".join(chunks), data)

    def test_content_defined_chunking(self):
        data = os.urandom(18 * MB)

        chunks = list(
            content_defined_chunks(io.BytesIO(data))
        )

        self.assertEqual(
            b"".join(chunks),
            data
        )

        self.assertTrue(
            all(
                len(chunk) <= MAX_SIZE
                for chunk in chunks
            )
        )

        self.assertTrue(
            all(
                len(chunk) >= MIN_SIZE
                for chunk in chunks[:-1]
            )
        )

    def test_sha256(self):
        data = b"Distributed Storage"

        expected = hashlib.sha256(
            data
        ).hexdigest()

        self.assertEqual(
            chunk_hash(data),
            expected
        )

    def test_empty_file(self):
        self.assertEqual(
            list(fixed_chunks(io.BytesIO(b""))),
            []
        )


class TestConsistentHashing(unittest.TestCase):

    def setUp(self):
        self.nodes = [
            "node-1",
            "node-2",
            "node-3",
            "node-4"
        ]

        self.hashes = [
            hashlib.sha256(
                f"chunk-{i}".encode()
            ).hexdigest()
            for i in range(5000)
        ]

    def test_deterministic_placement(self):
        ring1 = ConsistentHashRing(self.nodes)
        ring2 = ConsistentHashRing(self.nodes)

        for digest in self.hashes[:100]:
            self.assertEqual(
                ring1.get_node(digest),
                ring2.get_node(digest)
            )

    def test_virtual_nodes(self):
        ring = ConsistentHashRing(
            self.nodes,
            virtual_nodes=128
        )

        self.assertEqual(
            len(ring.positions),
            4 * 128
        )

    def test_add_node(self):
        result = placement_movement(
            self.hashes,
            self.nodes,
            self.nodes + ["node-5"]
        )

        self.assertGreater(
            result["movement_percentage"],
            0
        )

        self.assertLess(
            result["movement_percentage"],
            40
        )

    def test_remove_node(self):
        result = placement_movement(
            self.hashes,
            self.nodes,
            self.nodes[:3]
        )

        self.assertGreater(
            result["movement_percentage"],
            0
        )

        self.assertLess(
            result["movement_percentage"],
            50
        )

    def test_replica_candidates(self):
        ring = ConsistentHashRing(self.nodes)

        selected = ring.get_nodes(
            self.hashes[0],
            3
        )

        self.assertEqual(len(selected), 3)
        self.assertEqual(len(set(selected)), 3)


if __name__ == "__main__":
    unittest.main()

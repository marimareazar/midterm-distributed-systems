
import hashlib
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "metadata_service"))

from erasure_coding import encode_chunk, decode_chunk
from ring import ConsistentHashRing


class TestPhase4(unittest.TestCase):

    def setUp(self):
        self.nodes = [
            f"node-{i}" for i in range(1, 7)
        ]

    def test_six_distinct_nodes(self):
        ring = ConsistentHashRing(self.nodes)

        digest = hashlib.sha256(
            b"test-data"
        ).hexdigest()

        selected = ring.get_nodes(digest, 6)

        self.assertEqual(len(selected), 6)
        self.assertEqual(len(set(selected)), 6)

    def test_replication_quorum(self):
        n, w, r = 3, 2, 2
        self.assertGreater(w + r, n)

    def test_erasure_encoding(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        self.assertEqual(len(fragments), 6)

    def test_recovery_from_four_fragments(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        recovered = decode_chunk(
            fragments[2:6],
            len(data),
            4,
            2
        )

        self.assertEqual(recovered, data)

    def test_cannot_recover_three_fragments(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        with self.assertRaises(ValueError):
            decode_chunk(
                fragments[:3],
                len(data),
                4,
                2
            )


if __name__ == "__main__":
    unittest.main()

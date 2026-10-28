
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "metadata_service"))

from erasure_coding import encode_chunk, decode_chunk


class TestErasureCoding(unittest.TestCase):

    def test_encode_six_fragments(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        self.assertEqual(len(fragments), 6)
        self.assertEqual(len({
            f["index"] for f in fragments
        }), 6)

    def test_recover_after_two_missing_fragments(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        available = fragments[1:5]

        recovered = decode_chunk(
            available,
            len(data),
            4,
            2
        )

        self.assertEqual(recovered, data)

    def test_reject_three_missing_fragments(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        with self.assertRaises(ValueError):
            decode_chunk(
                fragments[:3],
                len(data),
                4,
                2
            )

    def test_detect_corruption(self):
        data = os.urandom(1024 * 1024)
        fragments = encode_chunk(data, 4, 2)

        fragments[0]["data"] = b"corrupted"

        with self.assertRaises(ValueError):
            decode_chunk(
                fragments[:4],
                len(data),
                4,
                2
            )


if __name__ == "__main__":
    unittest.main()


import bisect
import hashlib


def hash_value(value: str) -> int:
    return int(
        hashlib.sha256(value.encode()).hexdigest(),
        16
    )


class ConsistentHashRing:

    def __init__(
        self,
        nodes: list[str],
        virtual_nodes: int = 128
    ):
        self.virtual_nodes = virtual_nodes
        self.ring = []
        self.positions = []

        for node in sorted(set(nodes)):
            self.add_node(node)

    def add_node(self, node_id: str):
        for i in range(self.virtual_nodes):
            position = hash_value(f"{node_id}:{i}")

            index = bisect.bisect_left(
                self.positions,
                position
            )

            self.positions.insert(index, position)
            self.ring.insert(index, node_id)

    def remove_node(self, node_id: str):
        filtered = [
            (position, node)
            for position, node in zip(
                self.positions,
                self.ring
            )
            if node != node_id
        ]

        self.positions = [
            position for position, _ in filtered
        ]

        self.ring = [
            node for _, node in filtered
        ]

    def get_node(self, chunk_hash: str) -> str:
        if not self.ring:
            raise ValueError("No storage nodes available")

        position = int(chunk_hash, 16)

        index = bisect.bisect_left(
            self.positions,
            position
        )

        if index == len(self.ring):
            index = 0

        return self.ring[index]

    def get_nodes(
        self,
        chunk_hash: str,
        count: int
    ) -> list[str]:

        if not self.ring or count <= 0:
            return []

        position = int(chunk_hash, 16)

        index = bisect.bisect_left(
            self.positions,
            position
        )

        result = []
        seen = set()

        for offset in range(len(self.ring)):
            node = self.ring[
                (index + offset) % len(self.ring)
            ]

            if node not in seen:
                seen.add(node)
                result.append(node)

            if len(result) == count:
                break

        return result


def placement_movement(
    hashes: list[str],
    before: list[str],
    after: list[str]
) -> dict:

    old_ring = ConsistentHashRing(before)
    new_ring = ConsistentHashRing(after)

    moved = sum(
        old_ring.get_node(h) != new_ring.get_node(h)
        for h in hashes
    )

    total = len(hashes)

    return {
        "total_chunks": total,
        "moved_chunks": moved,
        "movement_percentage": round(
            100 * moved / total, 2
        ) if total else 0
    }

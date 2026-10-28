
import hashlib

import httpx

from ring import ConsistentHashRing


class ReplicationManager:

    def __init__(self, nodes, n=3, w=2, r=2):
        if not 1 <= w <= n:
            raise ValueError("Invalid write quorum")

        if not 1 <= r <= n:
            raise ValueError("Invalid read quorum")

        if w + r <= n:
            raise ValueError("W + R must be greater than N")

        if len(set(nodes)) < n:
            raise ValueError("Not enough distinct storage nodes")

        self.n = n
        self.w = w
        self.r = r
        self.ring = ConsistentHashRing(nodes)

    def select_nodes(self, chunk_hash):
        return self.ring.get_nodes(chunk_hash, self.n)

    def write_chunk(self, data):
        chunk_hash = hashlib.sha256(data).hexdigest()
        target_nodes = self.select_nodes(chunk_hash)

        successful_nodes = []
        failed_nodes = []

        with httpx.Client(timeout=30) as client:
            for node in target_nodes:
                host = node.replace("-", "")
                url = f"http://{host}:8000/chunks/{chunk_hash}"

                try:
                    response = client.put(
                        url,
                        content=data,
                        headers={
                            "Content-Type": "application/octet-stream"
                        }
                    )
                    response.raise_for_status()
                    successful_nodes.append(node)

                except httpx.HTTPError:
                    failed_nodes.append(node)

        return {
            "chunk_hash": chunk_hash,
            "success": len(successful_nodes) >= self.w,
            "quorum_reached": len(successful_nodes) >= self.w,
            "fully_replicated": len(successful_nodes) == self.n,
            "successful_nodes": successful_nodes,
            "failed_nodes": failed_nodes
        }

    def read_chunk(self, chunk_hash):
        target_nodes = self.select_nodes(chunk_hash)
        valid_responses = []

        with httpx.Client(timeout=30) as client:
            for node in target_nodes:
                host = node.replace("-", "")
                url = f"http://{host}:8000/chunks/{chunk_hash}"

                try:
                    response = client.get(url)
                    response.raise_for_status()

                    data = response.content

                    if hashlib.sha256(data).hexdigest() != chunk_hash:
                        continue

                    valid_responses.append((node, data))

                except httpx.HTTPError:
                    continue

        if len(valid_responses) < self.r:
            raise RuntimeError("Read quorum not reached")

        return valid_responses[0][1]

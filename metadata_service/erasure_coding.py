
import hashlib
from zfec.easyfec import Encoder, Decoder


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encode_chunk(
    data: bytes,
    k: int = 4,
    m: int = 2
) -> list[dict]:
    if k < 1 or m < 1 or k + m > 256:
        raise ValueError("Invalid erasure coding parameters")

    total = k + m
    encoder = Encoder(k, total)

    encoded = encoder.encode(data)

    return [
        {
            "index": index,
            "data": fragment,
            "hash": sha256(fragment),
            "size": len(fragment)
        }
        for index, fragment in enumerate(encoded)
    ]


def decode_chunk(
    fragments: list[dict],
    original_size: int,
    k: int = 4,
    m: int = 2
) -> bytes:
    if original_size < 0:
        raise ValueError("Invalid original size")

    unique = {}

    for fragment in fragments:
        index = fragment["index"]

        if not 0 <= index < k + m:
            raise ValueError("Invalid fragment index")

        if sha256(fragment["data"]) != fragment["hash"]:
            raise ValueError("Corrupted fragment")

        unique[index] = fragment["data"]

    if len(unique) < k:
        raise ValueError("Not enough fragments to recover chunk")

    selected = sorted(unique)[:k]

    block_size = len(unique[selected[0]])
    padlen = block_size * k - original_size

    if padlen < 0:
        raise ValueError("Invalid padding information")

    decoder = Decoder(k, k + m)

    return decoder.decode(
        [unique[index] for index in selected],
        selected,
        padlen
    )

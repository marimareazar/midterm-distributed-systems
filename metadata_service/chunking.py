
import hashlib
from typing import BinaryIO, Iterator

MB = 1024 * 1024

FIXED_SIZE = 4 * MB
MIN_SIZE = 1 * MB
AVG_SIZE = 4 * MB
MAX_SIZE = 8 * MB

MASK = AVG_SIZE - 1
GEAR = [
    int.from_bytes(
        hashlib.sha256(bytes([i])).digest()[:8],
        "big"
    )
    for i in range(256)
]


def chunk_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixed_chunks(
    file: BinaryIO,
    size: int = FIXED_SIZE
) -> Iterator[bytes]:

    while True:
        data = file.read(size)

        if not data:
            break

        yield data


def content_defined_chunks(
    file: BinaryIO
) -> Iterator[bytes]:

    buffer = bytearray()
    rolling_hash = 0

    while True:
        block = file.read(64 * 1024)

        if not block:
            break

        for byte in block:
            buffer.append(byte)

            rolling_hash = (
                (rolling_hash << 1) + GEAR[byte]
            ) & 0xFFFFFFFFFFFFFFFF

            size = len(buffer)

            if size < MIN_SIZE:
                continue

            boundary_found = (rolling_hash & MASK) == 0

            if boundary_found or size >= MAX_SIZE:
                yield bytes(buffer)

                buffer.clear()
                rolling_hash = 0

    if buffer:
        yield bytes(buffer)


def split_file(
    file: BinaryIO,
    mode: str = "fixed"
) -> Iterator[bytes]:

    if mode == "fixed":
        yield from fixed_chunks(file)

    elif mode == "cdc":
        yield from content_defined_chunks(file)

    else:
        raise ValueError(
            "Chunking mode must be 'fixed' or 'cdc'"
        )

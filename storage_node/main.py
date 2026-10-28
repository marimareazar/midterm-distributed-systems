
import asyncio
import hashlib
import os
import re
import shutil
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response

NODE_ID = os.getenv("NODE_ID", "node-1")
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", "./data"))
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:8000")
HEARTBEAT_INTERVAL = 5

HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def get_chunk_path(chunk_hash: str) -> Path:
    if not HASH_PATTERN.fullmatch(chunk_hash):
        raise HTTPException(400, "Invalid SHA-256 hash")

    return STORAGE_DIR / chunk_hash[:2] / chunk_hash


def calculate_hash(path: Path) -> str:
    sha256 = hashlib.sha256()

    with path.open("rb") as file:
        while data := file.read(1024 * 1024):
            sha256.update(data)

    return sha256.hexdigest()


def verify_chunk(path: Path, chunk_hash: str):
    if not path.is_file():
        raise HTTPException(404, "Chunk not found")

    if calculate_hash(path) != chunk_hash:
        raise HTTPException(409, "Chunk corrupted")


def count_chunks() -> int:
    return sum(
        1
        for path in STORAGE_DIR.rglob("*")
        if path.is_file()
        and HASH_PATTERN.fullmatch(path.name)
    )


def get_node_status() -> dict:
    disk = shutil.disk_usage(STORAGE_DIR)

    return {
        "node_id": NODE_ID,
        "free_space": disk.free,
        "chunk_count": count_chunks()
    }


async def send_heartbeats():
    async with httpx.AsyncClient(timeout=3) as client:
        while True:
            try:
                response = await client.post(
                    f"{METADATA_URL}/heartbeat",
                    json=get_node_status()
                )
                response.raise_for_status()
            except (httpx.HTTPError, OSError) as error:
                print(
                    f"Heartbeat failed: {error}",
                    flush=True
                )

            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(send_heartbeats())
    yield

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title=f"Storage Node - {NODE_ID}",
    lifespan=lifespan
)


@app.get("/health")
def health():
    return get_node_status()


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request):
    path = get_chunk_path(chunk_hash)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_name = tempfile.mkstemp(dir=path.parent)
    temp_path = Path(temp_name)

    sha256 = hashlib.sha256()
    size = 0

    try:
        with os.fdopen(fd, "wb") as file:
            async for data in request.stream():
                sha256.update(data)
                file.write(data)
                size += len(data)

        if sha256.hexdigest() != chunk_hash:
            raise HTTPException(
                400,
                "Chunk hash mismatch"
            )

        if path.exists():
            verify_chunk(path, chunk_hash)
            return {
                "message": "Chunk already exists",
                "hash": chunk_hash,
                "size": path.stat().st_size
            }

        os.replace(temp_path, path)

        return {
            "message": "Chunk stored successfully",
            "hash": chunk_hash,
            "size": size,
            "node_id": NODE_ID
        }

    finally:
        temp_path.unlink(missing_ok=True)


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    path = get_chunk_path(chunk_hash)
    verify_chunk(path, chunk_hash)

    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=chunk_hash,
        content_disposition_type="attachment"
    )


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    path = get_chunk_path(chunk_hash)
    verify_chunk(path, chunk_hash)

    return Response(
        headers={
            "Content-Length": str(path.stat().st_size),
            "ETag": f'"{chunk_hash}"',
            "X-Node-ID": NODE_ID
        }
    )


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    path = get_chunk_path(chunk_hash)

    if not path.is_file():
        raise HTTPException(404, "Chunk not found")

    path.unlink()

    return {
        "message": "Chunk deleted successfully",
        "hash": chunk_hash,
        "node_id": NODE_ID
    }

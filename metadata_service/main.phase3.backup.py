
import hashlib
import os
import re
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import (
    FastAPI, File, Form, HTTPException,
    Query, UploadFile
)
from fastapi.background import BackgroundTasks
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from chunking import split_file, chunk_hash
from database import get_connection, initialize_database
from ring import ConsistentHashRing, placement_movement


NODE_TIMEOUT = 15
VIRTUAL_NODES = 128
STORAGE_PORT = 8000

NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")


class Heartbeat(BaseModel):
    node_id: str
    free_space: int
    chunk_count: int


class PlacementRequest(BaseModel):
    before: list[str]
    after: list[str]


@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    yield


app = FastAPI(
    title="Distributed Storage Metadata Service",
    lifespan=lifespan
)


def validate_name(value: str):
    if not NAME_PATTERN.fullmatch(value):
        raise HTTPException(
            400,
            "Use letters, numbers, dots, hyphens or underscores"
        )


def get_storage_url(node_id: str) -> str:
    validate_name(node_id)

    host = node_id.replace("-", "")

    return f"http://{host}:{STORAGE_PORT}"


def get_active_nodes() -> list[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT node_id
                FROM nodes
                WHERE last_seen > NOW() - INTERVAL '15 seconds'
                ORDER BY node_id
            """)

            return [row["node_id"] for row in cur.fetchall()]


def get_ring() -> ConsistentHashRing:
    nodes = get_active_nodes()

    if not nodes:
        raise HTTPException(
            503,
            "No active storage nodes available"
        )

    return ConsistentHashRing(
        nodes,
        virtual_nodes=VIRTUAL_NODES
    )


def require_object(
    conn,
    bucket: str,
    key: str,
    version: int | None = None
):
    with conn.cursor() as cur:
        if version is None:
            cur.execute("""
                SELECT *
                FROM objects
                WHERE bucket = %s AND object_key = %s
                ORDER BY version DESC
                LIMIT 1
            """, (bucket, key))
        else:
            cur.execute("""
                SELECT *
                FROM objects
                WHERE bucket = %s
                  AND object_key = %s
                  AND version = %s
            """, (bucket, key, version))

        obj = cur.fetchone()

    if obj is None:
        raise HTTPException(404, "Object not found")

    return obj


def get_object_chunks(conn, object_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                oc.position,
                oc.chunk_hash,
                c.node_id,
                c.size,
                c.ref_count
            FROM object_chunks oc
            JOIN chunks c
              ON c.chunk_hash = oc.chunk_hash
            WHERE oc.object_id = %s
            ORDER BY oc.position
        """, (object_id,))

        return cur.fetchall()


def serialize_object(obj, chunks):
    return {
        "bucket": obj["bucket"],
        "key": obj["object_key"],
        "version": obj["version"],
        "size": obj["size"],
        "chunking_mode": obj["chunking_mode"],
        "created_at": obj["created_at"].isoformat(),
        "chunk_count": len(chunks),
        "chunks": [
            {
                "position": chunk["position"],
                "hash": chunk["chunk_hash"],
                "node_id": chunk["node_id"],
                "size": chunk["size"],
                "ref_count": chunk["ref_count"]
            }
            for chunk in chunks
        ]
    }


@app.get("/health")
def health():
    return {"status": "ok", "service": "metadata"}


# -------------------------------------------------
# Phase 2: Heartbeats and node monitoring
# -------------------------------------------------

@app.post("/heartbeat")
def receive_heartbeat(heartbeat: Heartbeat):
    validate_name(heartbeat.node_id)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nodes
                    (node_id, free_space, chunk_count, last_seen)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (node_id)
                DO UPDATE SET
                    free_space = EXCLUDED.free_space,
                    chunk_count = EXCLUDED.chunk_count,
                    last_seen = NOW()
            """, (
                heartbeat.node_id,
                heartbeat.free_space,
                heartbeat.chunk_count
            ))

    return {"message": "Heartbeat received"}


@app.get("/nodes")
def list_nodes():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    node_id,
                    free_space,
                    chunk_count,
                    last_seen,
                    last_seen >
                        NOW() - INTERVAL '15 seconds'
                        AS online
                FROM nodes
                ORDER BY node_id
            """)

            rows = cur.fetchall()

    return {
        row["node_id"]: {
            "free_space": row["free_space"],
            "chunk_count": row["chunk_count"],
            "last_seen": row["last_seen"].isoformat(),
            "status": "online" if row["online"] else "offline"
        }
        for row in rows
    }


# -------------------------------------------------
# Phase 3: Consistent hashing
# -------------------------------------------------

@app.get("/placement/ring")
def placement_ring():
    nodes = get_active_nodes()

    return {
        "active_nodes": nodes,
        "virtual_nodes_per_node": VIRTUAL_NODES,
        "total_virtual_nodes": len(nodes) * VIRTUAL_NODES
    }


@app.post("/placement/simulate")
def simulate_placement(request: PlacementRequest):
    if not request.before or not request.after:
        raise HTTPException(
            400,
            "Both node lists must be non-empty"
        )

    if len(set(request.before)) != len(request.before):
        raise HTTPException(400, "Duplicate node IDs")

    if len(set(request.after)) != len(request.after):
        raise HTTPException(400, "Duplicate node IDs")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chunk_hash FROM chunks")
            hashes = [
                row["chunk_hash"]
                for row in cur.fetchall()
            ]

    result = placement_movement(
        hashes,
        request.before,
        request.after
    )

    return {
        "before": request.before,
        "after": request.after,
        **result
    }


# -------------------------------------------------
# Phase 3: Chunk statistics
# -------------------------------------------------

@app.get("/chunks/stats")
def chunk_statistics():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS unique_chunks,
                    COALESCE(SUM(size), 0) AS physical_bytes,
                    COALESCE(SUM(ref_count), 0) AS references,
                    COALESCE(
                        SUM(size * ref_count), 0
                    ) AS referenced_bytes
                FROM chunks
            """)

            result = cur.fetchone()

    return {
        "unique_chunks": result["unique_chunks"],
        "physical_bytes": result["physical_bytes"],
        "references": result["references"],
        "referenced_bytes": result["referenced_bytes"],
        "estimated_saved_bytes": (
            result["referenced_bytes"]
            - result["physical_bytes"]
        )
    }


# -------------------------------------------------
# Phase 3: Upload an object
# -------------------------------------------------

@app.post("/objects/{bucket}/{key}")
def upload_object(
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    mode: str = Form("fixed")
):
    validate_name(bucket)
    validate_name(key)

    if mode not in ("fixed", "cdc"):
        raise HTTPException(
            400,
            "Mode must be fixed or cdc"
        )

    ring = get_ring()

    entries = []
    known_chunks = {}
    total_size = 0
    new_unique_chunks = 0

    with httpx.Client(timeout=120) as client:
        for data in split_file(file.file, mode):
            digest = chunk_hash(data)
            size = len(data)
            total_size += size

            if digest not in known_chunks:
                with get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT node_id, size
                            FROM chunks
                            WHERE chunk_hash = %s
                        """, (digest,))

                        existing = cur.fetchone()

                if existing:
                    if existing["size"] != size:
                        raise HTTPException(
                            409, "Chunk metadata mismatch"
                        )

                    node_id = existing["node_id"]

                    try:
                        response = client.head(
                            f"{get_storage_url(node_id)}/chunks/{digest}"
                        )
                        response.raise_for_status()
                    except httpx.HTTPError:
                        raise HTTPException(
                            503,
                            f"Existing chunk unavailable: {digest}"
                        )

                else:
                    node_id = ring.get_node(digest)

                    try:
                        response = client.put(
                            f"{get_storage_url(node_id)}/chunks/{digest}",
                            content=data,
                            headers={
                                "Content-Type":
                                    "application/octet-stream"
                            }
                        )
                        response.raise_for_status()
                    except httpx.HTTPError as error:
                        raise HTTPException(
                            503,
                            f"Chunk storage failed: {error}"
                        )

                    new_unique_chunks += 1

                known_chunks[digest] = (node_id, size)

            entries.append(digest)

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Serialize metadata writes for this prototype.
            cur.execute(
                "SELECT pg_advisory_xact_lock(3001)"
            )

            cur.execute("""
                SELECT COALESCE(MAX(version), 0) + 1
                    AS next_version
                FROM objects
                WHERE bucket = %s AND object_key = %s
            """, (bucket, key))

            version = cur.fetchone()["next_version"]

            cur.execute("""
                INSERT INTO objects (
                    bucket, object_key, version,
                    size, chunking_mode
                )
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (
                bucket, key, version, total_size, mode
            ))

            object_id = cur.fetchone()["id"]

            for position, digest in enumerate(entries):
                node_id, size = known_chunks[digest]

                cur.execute("""
                    INSERT INTO chunks (
                        chunk_hash, node_id,
                        size, ref_count
                    )
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (chunk_hash)
                    DO UPDATE SET
                        ref_count = chunks.ref_count + 1
                """, (digest, node_id, size))

                cur.execute("""
                    INSERT INTO object_chunks (
                        object_id, position, chunk_hash
                    )
                    VALUES (%s, %s, %s)
                """, (object_id, position, digest))

    return {
        "message": "Object stored successfully",
        "bucket": bucket,
        "key": key,
        "version": version,
        "size": total_size,
        "chunking_mode": mode,
        "chunk_count": len(entries),
        "new_unique_chunks": new_unique_chunks
    }


# -------------------------------------------------
# Phase 3: List and search objects
# -------------------------------------------------

@app.get("/objects")
def list_objects(
    bucket: str | None = None,
    search: str | None = None
):
    query = """
        SELECT bucket, object_key, version,
               size, chunking_mode, created_at
        FROM objects
        WHERE (%s IS NULL OR bucket = %s)
          AND (%s IS NULL OR object_key ILIKE %s)
        ORDER BY created_at DESC
        LIMIT 1000
    """

    pattern = f"%{search}%" if search else None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                (bucket, bucket, search, pattern)
            )
            rows = cur.fetchall()

    return [
        {
            **row,
            "created_at": row["created_at"].isoformat()
        }
        for row in rows
    ]


# -------------------------------------------------
# Phase 3: Object manifest
# -------------------------------------------------

@app.get("/objects/{bucket}/{key}/manifest")
def object_manifest(
    bucket: str,
    key: str,
    version: int | None = Query(None)
):
    with get_connection() as conn:
        obj = require_object(conn, bucket, key, version)
        chunks = get_object_chunks(conn, obj["id"])

    return serialize_object(obj, chunks)


# -------------------------------------------------
# Phase 3: Download and reconstruct an object
# -------------------------------------------------

@app.get("/objects/{bucket}/{key}")
def download_object(
    bucket: str,
    key: str,
    background_tasks: BackgroundTasks,
    version: int | None = Query(None)
):
    with get_connection() as conn:
        obj = require_object(conn, bucket, key, version)
        chunks = get_object_chunks(conn, obj["id"])

    fd, temp_name = tempfile.mkstemp()
    temp_path = Path(temp_name)

    try:
        with os.fdopen(fd, "wb") as output:
            with httpx.Client(timeout=120) as client:
                for chunk in chunks:
                    digest = chunk["chunk_hash"]
                    node_id = chunk["node_id"]

                    response = client.get(
                        f"{get_storage_url(node_id)}/chunks/{digest}"
                    )

                    if response.status_code != 200:
                        raise HTTPException(
                            503,
                            f"Chunk unavailable: {digest}"
                        )

                    data = response.content

                    if hashlib.sha256(data).hexdigest() != digest:
                        raise HTTPException(
                            409,
                            f"Corrupted chunk: {digest}"
                        )

                    output.write(data)

        if temp_path.stat().st_size != obj["size"]:
            raise HTTPException(
                409,
                "Reconstructed file size mismatch"
            )

        background_tasks.add_task(
            temp_path.unlink,
            missing_ok=True
        )

        return FileResponse(
            temp_path,
            filename=key,
            media_type="application/octet-stream",
            background=background_tasks
        )

    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


@app.head("/objects/{bucket}/{key}")
def head_object(
    bucket: str,
    key: str,
    version: int | None = Query(None)
):
    with get_connection() as conn:
        obj = require_object(conn, bucket, key, version)

    return Response(
        headers={
            "Content-Length": str(obj["size"]),
            "X-Object-Version": str(obj["version"]),
            "X-Chunking-Mode": obj["chunking_mode"]
        }
    )


# -------------------------------------------------
# Phase 3: Delete object and update references
# -------------------------------------------------

@app.delete("/objects/{bucket}/{key}")
def delete_object(
    bucket: str,
    key: str,
    version: int | None = Query(None)
):
    chunks_to_delete = []

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(3001)"
            )

            obj = require_object(
                conn, bucket, key, version
            )

            chunks = get_object_chunks(
                conn, obj["id"]
            )

            for chunk in chunks:
                digest = chunk["chunk_hash"]

                cur.execute("""
                    UPDATE chunks
                    SET ref_count = ref_count - 1
                    WHERE chunk_hash = %s
                    RETURNING ref_count, node_id
                """, (digest,))

                result = cur.fetchone()

                if result["ref_count"] == 0:
                    chunks_to_delete.append(
                        (digest, result["node_id"])
                    )

            cur.execute(
                "DELETE FROM objects WHERE id = %s",
                (obj["id"],)
            )

            for digest, _ in chunks_to_delete:
                cur.execute("""
                    DELETE FROM chunks
                    WHERE chunk_hash = %s
                      AND ref_count = 0
                """, (digest,))

    # Physical cleanup after metadata commit.
    # A production version requires retryable garbage collection.
    failed_deletions = []

    with httpx.Client(timeout=30) as client:
        for digest, node_id in set(chunks_to_delete):
            try:
                response = client.delete(
                    f"{get_storage_url(node_id)}/chunks/{digest}"
                )

                if response.status_code not in (200, 404):
                    failed_deletions.append(digest)

            except httpx.HTTPError:
                failed_deletions.append(digest)

    return {
        "message": "Object metadata deleted",
        "bucket": bucket,
        "key": key,
        "version": obj["version"],
        "unreferenced_chunks": len(
            set(chunks_to_delete)
        ),
        "failed_physical_deletions": failed_deletions
    }

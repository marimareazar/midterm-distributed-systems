
import hashlib
import io
import os
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

import httpx
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.background import BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from database import get_connection
from chunking import split_file
from ring import ConsistentHashRing
from erasure_coding import encode_chunk, decode_chunk


router = APIRouter()
operation_lock = threading.RLock()
stop_worker = threading.Event()

NODE_TIMEOUT = 15
SCRUB_INTERVAL = 60
REPAIR_INTERVAL = 10
HTTP_TIMEOUT = 60

repair_metrics = {
    "bytes_read": 0,
    "bytes_written": 0,
    "repairs_completed": 0,
    "repairs_failed": 0
}


class BucketPolicy(BaseModel):
    method: str = "replication"
    n: int = Field(default=3, ge=1, le=10)
    w: int = Field(default=2, ge=1)
    r: int = Field(default=2, ge=1)
    k: int = Field(default=4, ge=1)
    m: int = Field(default=2, ge=1)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def node_url(node_id: str, digest: str) -> str:
    host = node_id.replace("-", "")
    return f"http://{host}:8000/chunks/{digest}"


def initialize_fault_tolerance():
    statements = [
        """
        CREATE TABLE IF NOT EXISTS ft_buckets (
            bucket TEXT PRIMARY KEY,
            method TEXT NOT NULL,
            n INTEGER NOT NULL,
            w INTEGER NOT NULL,
            r INTEGER NOT NULL,
            k INTEGER NOT NULL,
            m INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ft_objects (
            id BIGSERIAL PRIMARY KEY,
            bucket TEXT NOT NULL,
            object_key TEXT NOT NULL,
            version INTEGER NOT NULL,
            size BIGINT NOT NULL,
            chunking_mode TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(bucket, object_key, version)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ft_chunks (
            id TEXT PRIMARY KEY,
            digest CHAR(64) NOT NULL,
            size BIGINT NOT NULL,
            method TEXT NOT NULL,
            n INTEGER NOT NULL,
            w INTEGER NOT NULL,
            r INTEGER NOT NULL,
            k INTEGER NOT NULL,
            m INTEGER NOT NULL,
            ref_count BIGINT NOT NULL DEFAULT 0
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ft_object_chunks (
            object_id BIGINT NOT NULL
                REFERENCES ft_objects(id) ON DELETE CASCADE,
            position INTEGER NOT NULL,
            chunk_id TEXT NOT NULL REFERENCES ft_chunks(id),
            PRIMARY KEY(object_id, position)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ft_fragments (
            chunk_id TEXT NOT NULL
                REFERENCES ft_chunks(id) ON DELETE CASCADE,
            shard_index INTEGER NOT NULL,
            node_id TEXT NOT NULL,
            digest CHAR(64) NOT NULL,
            size BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'healthy',
            PRIMARY KEY(chunk_id, shard_index)
        )
        """
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


def active_nodes():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT node_id FROM nodes
                WHERE last_seen >
                    NOW() - INTERVAL '15 seconds'
                ORDER BY node_id
            """)
            return [row["node_id"] for row in cur.fetchall()]


def policy_for(bucket):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM ft_buckets WHERE bucket=%s",
                (bucket,)
            )
            row = cur.fetchone()

    if row is None:
        raise HTTPException(404, "Bucket policy not found")

    return dict(row)


def validate_policy(policy: BucketPolicy):
    if policy.method not in ("replication", "erasure"):
        raise HTTPException(400, "Invalid protection method")

    if policy.method == "replication":
        if policy.w > policy.n or policy.r > policy.n:
            raise HTTPException(400, "Invalid quorum")

        if policy.w + policy.r <= policy.n:
            raise HTTPException(400, "W + R must exceed N")

    else:
        if policy.k + policy.m > 256:
            raise HTTPException(400, "Too many fragments")

        if policy.k + policy.m > len(active_nodes()):
            raise HTTPException(
                503,
                "Not enough active nodes for erasure coding"
            )


def chunk_id_for(digest, policy):
    if policy["method"] == "replication":
        tag = (
            f"replication-{policy['n']}-"
            f"{policy['w']}-{policy['r']}"
        )
    else:
        tag = f"erasure-{policy['k']}-{policy['m']}"

    return f"{digest}:{tag}"


def get_chunk(conn, chunk_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM ft_chunks WHERE id=%s",
            (chunk_id,)
        )
        return cur.fetchone()


def get_fragments(conn, chunk_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * FROM ft_fragments
            WHERE chunk_id=%s
            ORDER BY shard_index
        """, (chunk_id,))
        return cur.fetchall()


def put_bytes(client, node, data):
    digest = sha(data)

    try:
        response = client.put(
            node_url(node, digest),
            content=data,
            headers={
                "Content-Type": "application/octet-stream"
            }
        )
        return response.status_code == 200
    except httpx.HTTPError:
        return False


def get_bytes(client, node, digest):
    try:
        response = client.get(node_url(node, digest))

        if response.status_code != 200:
            return None

        data = response.content

        if sha(data) != digest:
            return None

        return data

    except httpx.HTTPError:
        return None


def head_valid(client, fragment):
    try:
        response = client.head(
            node_url(
                fragment["node_id"],
                fragment["digest"]
            )
        )
        return response.status_code == 200
    except httpx.HTTPError:
        return False


def persist_fragment(
    conn, chunk_id, index, node, digest, size
):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ft_fragments
                (chunk_id, shard_index, node_id,
                 digest, size, status)
            VALUES (%s, %s, %s, %s, %s, 'healthy')
            ON CONFLICT (chunk_id, shard_index)
            DO UPDATE SET
                node_id=EXCLUDED.node_id,
                digest=EXCLUDED.digest,
                size=EXCLUDED.size,
                status='healthy'
        """, (chunk_id, index, node, digest, size))


def store_protected_chunk(client, data, policy):
    digest = sha(data)
    cid = chunk_id_for(digest, policy)

    with get_connection() as conn:
        existing = get_chunk(conn, cid)

    if existing:
        return cid, False

    nodes = active_nodes()
    ring = ConsistentHashRing(nodes)
    candidates = ring.get_nodes(digest, len(nodes))

    method = policy["method"]
    stored = []

    if method == "replication":
        target = policy["n"]
        required = policy["w"]

        for node in candidates:
            if len(stored) >= target:
                break

            if put_bytes(client, node, data):
                stored.append(
                    (len(stored), node, digest, len(data))
                )

        if len(stored) < required:
            raise HTTPException(
                503,
                "Replication write quorum not reached"
            )

    else:
        k, m = policy["k"], policy["m"]
        target = k + m

        if len(candidates) < target:
            raise HTTPException(503, "Not enough nodes")

        pieces = encode_chunk(data, k, m)

        for piece, node in zip(pieces, candidates):
            if put_bytes(client, node, piece["data"]):
                stored.append((
                    piece["index"],
                    node,
                    piece["hash"],
                    piece["size"]
                ))

        # A normal erasure-coded write must establish
        # the entire 4+2 protection set.
        if len(stored) != target:
            raise HTTPException(
                503,
                "Not all erasure-coded fragments were stored"
            )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ft_chunks
                    (id, digest, size, method,
                     n, w, r, k, m, ref_count)
                VALUES
                    (%s, %s, %s, %s,
                     %s, %s, %s, %s, %s, 0)
                ON CONFLICT (id) DO NOTHING
            """, (
                cid, digest, len(data), method,
                policy["n"], policy["w"], policy["r"],
                policy["k"], policy["m"]
            ))

        for index, node, fdigest, size in stored:
            persist_fragment(
                conn, cid, index, node, fdigest, size
            )

    return cid, True


def read_protected_chunk(client, chunk, fragments):
    online = set(active_nodes())

    if chunk["method"] == "replication":
        valid = []

        for fragment in fragments:
            if fragment["node_id"] not in online:
                continue

            data = get_bytes(
                client,
                fragment["node_id"],
                fragment["digest"]
            )

            if data is not None and sha(data) == chunk["digest"]:
                valid.append(data)

            if len(valid) >= chunk["r"]:
                return valid[0]

        raise HTTPException(
            503, "Replication read quorum not reached"
        )

    available = []

    for fragment in fragments:
        if fragment["node_id"] not in online:
            continue

        data = get_bytes(
            client,
            fragment["node_id"],
            fragment["digest"]
        )

        if data is not None:
            available.append({
                "index": fragment["shard_index"],
                "data": data,
                "hash": fragment["digest"]
            })

        if len(available) >= chunk["k"]:
            break

    if len(available) < chunk["k"]:
        raise HTTPException(
            503,
            "Not enough erasure-coded fragments"
        )

    original = decode_chunk(
        available,
        chunk["size"],
        chunk["k"],
        chunk["m"]
    )

    if sha(original) != chunk["digest"]:
        raise HTTPException(
            409,
            "Reconstructed chunk hash mismatch"
        )

    return original


@router.put("/buckets/{bucket}/policy")
def configure_bucket(bucket: str, policy: BucketPolicy):
    if not bucket or "/" in bucket:
        raise HTTPException(400, "Invalid bucket")

    validate_policy(policy)

    with operation_lock:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM ft_objects
                    WHERE bucket=%s LIMIT 1
                """, (bucket,))

                if cur.fetchone():
                    raise HTTPException(
                        409,
                        "Delete existing bucket objects before "
                        "changing its protection policy"
                    )

                cur.execute("""
                    INSERT INTO ft_buckets
                        (bucket, method, n, w, r, k, m)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (bucket)
                    DO UPDATE SET
                        method=EXCLUDED.method,
                        n=EXCLUDED.n,
                        w=EXCLUDED.w,
                        r=EXCLUDED.r,
                        k=EXCLUDED.k,
                        m=EXCLUDED.m
                """, (
                    bucket, policy.method,
                    policy.n, policy.w, policy.r,
                    policy.k, policy.m
                ))

    return {"bucket": bucket, **policy.model_dump()}


@router.get("/buckets/{bucket}/policy")
def get_bucket_policy(bucket: str):
    return policy_for(bucket)


@router.post("/objects/{bucket}/{key}")
def upload_object(
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    mode: str = Form("fixed")
):
    if mode not in ("fixed", "cdc"):
        raise HTTPException(400, "Invalid chunking mode")

    with operation_lock:
        policy = policy_for(bucket)

        ordered = []
        newly_created = 0
        total_size = 0

        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for data in split_file(file.file, mode):
                total_size += len(data)

                cid, created = store_protected_chunk(
                    client, data, policy
                )

                ordered.append(cid)
                newly_created += int(created)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(MAX(version), 0) + 1
                        AS version
                    FROM ft_objects
                    WHERE bucket=%s AND object_key=%s
                """, (bucket, key))
                version = cur.fetchone()["version"]

                cur.execute("""
                    INSERT INTO ft_objects
                        (bucket, object_key, version,
                         size, chunking_mode)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    bucket, key, version,
                    total_size, mode
                ))
                object_id = cur.fetchone()["id"]

                for position, cid in enumerate(ordered):
                    cur.execute("""
                        INSERT INTO ft_object_chunks
                            (object_id, position, chunk_id)
                        VALUES (%s, %s, %s)
                    """, (object_id, position, cid))

                for cid, count in Counter(ordered).items():
                    cur.execute("""
                        UPDATE ft_chunks
                        SET ref_count=ref_count + %s
                        WHERE id=%s
                    """, (count, cid))

        return {
            "message": "Protected object stored",
            "bucket": bucket,
            "key": key,
            "version": version,
            "size": total_size,
            "chunk_count": len(ordered),
            "new_unique_chunks": newly_created,
            "protection": policy["method"]
        }


def object_record(conn, bucket, key, version=None):
    with conn.cursor() as cur:
        if version is None:
            cur.execute("""
                SELECT * FROM ft_objects
                WHERE bucket=%s AND object_key=%s
                ORDER BY version DESC LIMIT 1
            """, (bucket, key))
        else:
            cur.execute("""
                SELECT * FROM ft_objects
                WHERE bucket=%s AND object_key=%s
                    AND version=%s
            """, (bucket, key, version))

        record = cur.fetchone()

    if not record:
        raise HTTPException(404, "Object not found")

    return record


def object_chunks(conn, object_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.*, oc.position
            FROM ft_object_chunks oc
            JOIN ft_chunks c ON c.id=oc.chunk_id
            WHERE oc.object_id=%s
            ORDER BY oc.position
        """, (object_id,))
        return cur.fetchall()


@router.get("/objects/{bucket}/{key}/manifest")
def manifest(bucket: str, key: str, version: int | None = None):
    with get_connection() as conn:
        obj = object_record(conn, bucket, key, version)
        chunks = object_chunks(conn, obj["id"])

        entries = []

        for chunk in chunks:
            fragments = get_fragments(conn, chunk["id"])

            entries.append({
                "position": chunk["position"],
                "hash": chunk["digest"],
                "size": chunk["size"],
                "method": chunk["method"],
                "ref_count": chunk["ref_count"],
                "fragments": [
                    {
                        "index": f["shard_index"],
                        "node": f["node_id"],
                        "hash": f["digest"],
                        "status": f["status"]
                    }
                    for f in fragments
                ]
            })

    return {
        "bucket": bucket,
        "key": key,
        "version": obj["version"],
        "size": obj["size"],
        "chunking_mode": obj["chunking_mode"],
        "created_at": obj["created_at"].isoformat(),
        "chunks": entries
    }


@router.get("/objects/{bucket}/{key}")
def download_object(
    bucket: str,
    key: str,
    background_tasks: BackgroundTasks,
    version: int | None = None
):
    with operation_lock:
        with get_connection() as conn:
            obj = object_record(conn, bucket, key, version)
            chunks = object_chunks(conn, obj["id"])

            fd, filename = tempfile.mkstemp()
            temp = Path(filename)

            try:
                with os.fdopen(fd, "wb") as output:
                    with httpx.Client(
                        timeout=HTTP_TIMEOUT
                    ) as client:
                        for chunk in chunks:
                            fragments = get_fragments(
                                conn, chunk["id"]
                            )

                            data = read_protected_chunk(
                                client, chunk, fragments
                            )

                            output.write(data)

                if temp.stat().st_size != obj["size"]:
                    raise HTTPException(
                        409, "Object size mismatch"
                    )

            except Exception:
                temp.unlink(missing_ok=True)
                raise

    background_tasks.add_task(
        temp.unlink, missing_ok=True
    )

    return FileResponse(
        temp,
        filename=key,
        media_type="application/octet-stream",
        background=background_tasks
    )


@router.delete("/objects/{bucket}/{key}")
def delete_object(
    bucket: str,
    key: str,
    version: int | None = None
):
    deletions = []

    with operation_lock:
        with get_connection() as conn:
            obj = object_record(conn, bucket, key, version)
            chunks = object_chunks(conn, obj["id"])

            counts = Counter(
                chunk["id"] for chunk in chunks
            )

            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM ft_objects WHERE id=%s",
                    (obj["id"],)
                )

                for cid, count in counts.items():
                    cur.execute("""
                        UPDATE ft_chunks
                        SET ref_count=ref_count - %s
                        WHERE id=%s
                        RETURNING ref_count
                    """, (count, cid))

                    remaining = cur.fetchone()["ref_count"]

                    if remaining == 0:
                        deletions.extend(
                            get_fragments(conn, cid)
                        )
                        cur.execute(
                            "DELETE FROM ft_chunks WHERE id=%s",
                            (cid,)
                        )

        failed = []

        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for fragment in deletions:
                try:
                    response = client.delete(
                        node_url(
                            fragment["node_id"],
                            fragment["digest"]
                        )
                    )

                    if response.status_code not in (200, 404):
                        failed.append(fragment["digest"])

                except httpx.HTTPError:
                    failed.append(fragment["digest"])

    return {
        "deleted": True,
        "unreferenced_fragments": len(deletions),
        "failed_cleanup": failed
    }


@router.get("/stats")
def storage_stats():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS chunks,
                       COALESCE(SUM(ref_count),0)
                           AS references,
                       COALESCE(SUM(size),0)
                           AS unique_bytes
                FROM ft_chunks
            """)
            chunks = cur.fetchone()

            cur.execute("""
                SELECT c.method,
                       COALESCE(SUM(f.size),0)
                           AS physical_bytes
                FROM ft_fragments f
                JOIN ft_chunks c ON c.id=f.chunk_id
                GROUP BY c.method
            """)
            storage = cur.fetchall()

    return {
        "unique_chunks": chunks["chunks"],
        "references": chunks["references"],
        "unique_bytes": chunks["unique_bytes"],
        "physical_by_method": {
            row["method"]: row["physical_bytes"]
            for row in storage
        },
        "repair_metrics": dict(repair_metrics)
    }

def repair_once():
    repaired = 0
    degraded = 0
    online = set(active_nodes())

    with operation_lock:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM ft_chunks
                    WHERE ref_count > 0
                    ORDER BY id
                """)
                chunks = cur.fetchall()

        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for chunk in chunks:
                with get_connection() as conn:
                    fragments = get_fragments(
                        conn, chunk["id"]
                    )

                healthy = []

                for fragment in fragments:
                    if (
                        fragment["node_id"] in online
                        and head_valid(client, fragment)
                    ):
                        healthy.append(fragment)

                target = (
                    chunk["n"]
                    if chunk["method"] == "replication"
                    else chunk["k"] + chunk["m"]
                )

                if len(healthy) >= target:
                    continue

                try:
                    if chunk["method"] == "replication":
                        original = None

                        for fragment in healthy:
                            data = get_bytes(
                                client,
                                fragment["node_id"],
                                fragment["digest"]
                            )

                            if data is not None:
                                original = data
                                repair_metrics["bytes_read"] += len(data)
                                break

                        if original is None:
                            degraded += 1
                            repair_metrics["repairs_failed"] += 1
                            continue

                        candidates = ConsistentHashRing(
                            sorted(online)
                        ).get_nodes(
                            chunk["digest"],
                            len(online)
                        )

                        used = {
                            f["node_id"] for f in healthy
                        }

                        for node in candidates:
                            if len(healthy) >= target:
                                break

                            if node in used:
                                continue

                            if not put_bytes(client, node, original):
                                continue

                            existing_indices = {
                                f["shard_index"]
                                for f in healthy
                            }

                            index = next(
                                i for i in range(target)
                                if i not in existing_indices
                            )

                            with get_connection() as conn:
                                persist_fragment(
                                    conn, chunk["id"], index,
                                    node, chunk["digest"],
                                    len(original)
                                )

                            healthy.append({
                                "shard_index": index,
                                "node_id": node,
                                "digest": chunk["digest"]
                            })

                            used.add(node)
                            repaired += 1
                            repair_metrics["bytes_written"] += len(original)
                            repair_metrics["repairs_completed"] += 1

                    else:
                        if len(healthy) < chunk["k"]:
                            degraded += 1
                            repair_metrics["repairs_failed"] += 1
                            continue

                        available = []

                        for fragment in healthy:
                            data = get_bytes(
                                client,
                                fragment["node_id"],
                                fragment["digest"]
                            )

                            if data is not None:
                                available.append({
                                    "index": fragment["shard_index"],
                                    "data": data,
                                    "hash": fragment["digest"]
                                })

                                repair_metrics["bytes_read"] += len(data)

                            if len(available) == chunk["k"]:
                                break

                        if len(available) < chunk["k"]:
                            degraded += 1
                            continue

                        original = decode_chunk(
                            available,
                            chunk["size"],
                            chunk["k"],
                            chunk["m"]
                        )

                        if sha(original) != chunk["digest"]:
                            degraded += 1
                            continue

                        pieces = encode_chunk(
                            original,
                            chunk["k"],
                            chunk["m"]
                        )

                        candidates = ConsistentHashRing(
                            sorted(online)
                        ).get_nodes(
                            chunk["digest"],
                            len(online)
                        )

                        used = {
                            f["node_id"] for f in healthy
                        }

                        healthy_indices = {
                            f["shard_index"]
                            for f in healthy
                        }

                        for piece in pieces:
                            if piece["index"] in healthy_indices:
                                continue

                            node = next(
                                (n for n in candidates if n not in used),
                                None
                            )

                            if node is None:
                                break

                            if not put_bytes(
                                client, node, piece["data"]
                            ):
                                continue

                            with get_connection() as conn:
                                persist_fragment(
                                    conn,
                                    chunk["id"],
                                    piece["index"],
                                    node,
                                    piece["hash"],
                                    piece["size"]
                                )

                            used.add(node)
                            repaired += 1
                            repair_metrics["bytes_written"] += piece["size"]
                            repair_metrics["repairs_completed"] += 1

                except (ValueError, httpx.HTTPError):
                    degraded += 1
                    repair_metrics["repairs_failed"] += 1

    return {
        "repaired_fragments": repaired,
        "unrecoverable_or_degraded_chunks": degraded,
        "metrics": dict(repair_metrics)
    }


def scrub_once():
    checked = 0
    corrupted_or_missing = 0

    with operation_lock:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT f.*
                    FROM ft_fragments f
                    JOIN ft_chunks c ON c.id=f.chunk_id
                    WHERE c.ref_count > 0
                """)
                fragments = cur.fetchall()

        online = set(active_nodes())

        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            for fragment in fragments:
                checked += 1

                valid = (
                    fragment["node_id"] in online
                    and head_valid(client, fragment)
                )

                if not valid:
                    corrupted_or_missing += 1

                with get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE ft_fragments
                            SET status=%s
                            WHERE chunk_id=%s AND shard_index=%s
                        """, (
                            "healthy" if valid else "unavailable",
                            fragment["chunk_id"],
                            fragment["shard_index"]
                        ))

    repair_result = repair_once()

    return {
        "checked_fragments": checked,
        "corrupted_or_missing": corrupted_or_missing,
        "repair": repair_result
    }


def background_worker():
    last_scrub = time.monotonic()

    while not stop_worker.wait(REPAIR_INTERVAL):
        try:
            repair_once()

            now = time.monotonic()

            if now - last_scrub >= SCRUB_INTERVAL:
                scrub_once()
                last_scrub = now

        except Exception as error:
            print(
                f"Background maintenance error: {error}",
                flush=True
            )


@router.post("/repair/run")
def run_repair():
    return repair_once()


@router.post("/scrub/run")
def run_scrub():
    return scrub_once()


@router.get("/repair/metrics")
def get_repair_metrics():
    return dict(repair_metrics)


@router.post("/repair/metrics/reset")
def reset_repair_metrics():
    with operation_lock:
        for key in repair_metrics:
            repair_metrics[key] = 0

    return dict(repair_metrics)

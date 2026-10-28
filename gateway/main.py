
import json
import os
import re
import tempfile

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import httpx

from fastapi import (
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    UploadFile
)

from fastapi.concurrency import run_in_threadpool

from fastapi.middleware.cors import CORSMiddleware

from fastapi.responses import (
    Response,
    StreamingResponse
)

from pydantic import BaseModel, Field

from database import (
    initialize_database,
    get_connection,
    save_bucket,
    bucket_exists,
    list_buckets,
    save_object,
    get_object,
    delete_object_record,
    search_objects,
    serialize
)

from security import require_api_key

from search import (
    index_object,
    remove_object,
    rebuild_index,
    search_objects_es
)


# ============================================================
# CONFIGURATION
# ============================================================

METADATA_URL = os.getenv(
    "METADATA_URL",
    "http://metadata:8000"
).rstrip("/")

NAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")

TIMEOUT = httpx.Timeout(
    connect=10,
    read=300,
    write=300,
    pool=30
)


# ============================================================
# MODELS
# ============================================================

class BucketCreate(BaseModel):
    name: str
    method: str = "replication"
    n: int = Field(default=3, ge=1, le=10)
    w: int = Field(default=2, ge=1)
    r: int = Field(default=2, ge=1)
    k: int = Field(default=4, ge=1)
    m: int = Field(default=2, ge=1)


# ============================================================
# VALIDATION
# ============================================================

def validate_name(value: str):
    if not NAME_PATTERN.fullmatch(value):
        raise HTTPException(
            status_code=400,
            detail=(
                "Name may contain letters, numbers, "
                "dots, underscores and hyphens"
            )
        )


def parse_tags(raw: str):
    try:
        value = json.loads(raw or "{}")

    except (json.JSONDecodeError, TypeError):
        raise HTTPException(
            status_code=400,
            detail="Tags must be valid JSON"
        )

    if not isinstance(value, dict):
        raise HTTPException(
            status_code=400,
            detail="Tags must be a JSON object"
        )

    if len(value) > 20:
        raise HTTPException(
            status_code=400,
            detail="Maximum 20 tags allowed"
        )

    for key, item in value.items():
        if not isinstance(key, str):
            raise HTTPException(
                status_code=400,
                detail="Tag keys must be strings"
            )

        if not isinstance(item, str):
            raise HTTPException(
                status_code=400,
                detail="Tag values must be strings"
            )

        if len(key) > 100 or len(item) > 500:
            raise HTTPException(
                status_code=400,
                detail="Tag key or value is too long"
            )

    return value


def parse_range(header: str | None, size: int):
    if header is None:
        return 0, size - 1, False

    if (
        size <= 0
        or not header.startswith("bytes=")
        or "," in header
    ):
        raise HTTPException(
            status_code=416,
            detail="Invalid byte range",
            headers={
                "Content-Range": f"bytes */{size}"
            }
        )

    value = header[6:]

    if value.count("-") != 1:
        raise HTTPException(
            status_code=416,
            detail="Invalid byte range",
            headers={
                "Content-Range": f"bytes */{size}"
            }
        )

    start_text, end_text = value.split("-", 1)

    try:
        if start_text == "":
            length = int(end_text)

            if length <= 0:
                raise ValueError

            start = max(0, size - length)
            end = size - 1

        else:
            start = int(start_text)

            end = (
                int(end_text)
                if end_text
                else size - 1
            )

            if start < 0 or start >= size:
                raise ValueError

            if end < start:
                raise ValueError

            end = min(end, size - 1)

    except (ValueError, TypeError):
        raise HTTPException(
            status_code=416,
            detail="Unsatisfiable byte range",
            headers={
                "Content-Range": f"bytes */{size}"
            }
        )

    return start, end, True


# ============================================================
# METADATA SERVICE COMMUNICATION
# ============================================================

def metadata_request(method, path, **kwargs):
    try:
        with httpx.Client(timeout=TIMEOUT) as client:
            response = client.request(
                method,
                f"{METADATA_URL}{path}",
                **kwargs
            )

    except httpx.HTTPError as error:
        raise HTTPException(
            status_code=502,
            detail=f"Metadata service unavailable: {error}"
        )

    if response.status_code >= 400:
        try:
            detail = response.json()

        except ValueError:
            detail = response.text[:300]

        raise HTTPException(
            status_code=response.status_code,
            detail=detail
        )

    return response


def metadata_json(method, path, **kwargs):
    return metadata_request(
        method,
        path,
        **kwargs
    ).json()


def backend_object_path(bucket, key):
    validate_name(bucket)
    validate_name(key)

    return f"/v4/objects/{bucket}/{key}"


def read_manifest(bucket, key, version=None):
    path = backend_object_path(bucket, key) + "/manifest"

    params = {}

    if version is not None:
        params["version"] = version

    return metadata_json(
        "GET",
        path,
        params=params
    )


# ============================================================
# OBJECT UPLOAD
# ============================================================

def perform_upload(
    bucket,
    key,
    file_obj,
    filename,
    mode,
    tags
):
    validate_name(bucket)
    validate_name(key)

    if not bucket_exists(bucket):
        raise HTTPException(
            status_code=404,
            detail="Bucket not found"
        )

    if mode not in ("fixed", "cdc"):
        raise HTTPException(
            status_code=400,
            detail="Invalid chunking mode"
        )

    response = metadata_json(
        "POST",
        backend_object_path(bucket, key),
        files={
            "file": (
                filename,
                file_obj,
                "application/octet-stream"
            )
        },
        data={
            "mode": mode
        }
    )

    version = response["version"]

    save_object(
        bucket=bucket,
        key=key,
        version=version,
        size=response["size"],
        chunk_count=response["chunk_count"],
        mode=mode,
        tags=tags
    )

    # Synchronize searchable metadata to Elasticsearch
    try:
        index_object(
            bucket,
            key,
            version
        )

    except Exception as error:
        print(
            f"Elasticsearch indexing failed: {error}",
            flush=True
        )

    return {
        **response,
        "tags": tags
    }


# ============================================================
# FASTAPI LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    initialize_database()
    yield


# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="Distributed Storage Gateway",
    description=(
        "Object storage API with replication, "
        "erasure coding, Elasticsearch search, "
        "cluster monitoring and repair metrics."
    ),
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(require_api_key)]
)


# ============================================================
# CORS FOR REACT FRONTEND
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=False,
    allow_methods=[
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "HEAD",
        "OPTIONS"
    ],
    allow_headers=[
        "X-API-Key",
        "Content-Type",
        "Range",
        "X-Object-Tags"
    ],
    expose_headers=[
        "Content-Range",
        "Content-Length",
        "Accept-Ranges",
        "X-Object-Version",
        "X-Chunk-Count"
    ]
)


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "gateway"
    }


# ============================================================
# CREATE BUCKET
# ============================================================

@app.post("/buckets", status_code=201)
def create_bucket(config: BucketCreate):
    validate_name(config.name)

    if config.method not in ("replication", "erasure"):
        raise HTTPException(
            status_code=400,
            detail="Method must be replication or erasure"
        )

    if config.method == "replication":
        if (
            config.w > config.n
            or config.r > config.n
            or config.w + config.r <= config.n
        ):
            raise HTTPException(
                status_code=400,
                detail="Invalid replication quorum configuration"
            )

    policy = config.model_dump(
        exclude={"name"}
    )

    result = metadata_json(
        "PUT",
        f"/v4/buckets/{config.name}/policy",
        json=policy
    )

    save_bucket(
        config.name,
        policy
    )

    return {
        "message": "Bucket configured successfully",
        "bucket": config.name,
        "policy": result
    }


# ============================================================
# LIST BUCKETS
# ============================================================

@app.get("/buckets")
def get_buckets():
    return [
        serialize(row)
        for row in list_buckets()
    ]


# ============================================================
# GET BUCKET
# ============================================================

@app.get("/buckets/{bucket}")
def get_bucket(bucket: str):
    validate_name(bucket)

    for row in list_buckets():
        if row["name"] == bucket:
            return serialize(row)

    raise HTTPException(
        status_code=404,
        detail="Bucket not found"
    )


# ============================================================
# MULTIPART FORM UPLOAD
# ============================================================

@app.post("/objects/{bucket}/{key}")
def multipart_upload(
    bucket: str,
    key: str,
    file: UploadFile = File(...),
    mode: str = Form("fixed"),
    tags: str = Form("{}")
):
    return perform_upload(
        bucket=bucket,
        key=key,
        file_obj=file.file,
        filename=file.filename or key,
        mode=mode,
        tags=parse_tags(tags)
    )


# ============================================================
# STREAMING UPLOAD
# ============================================================

@app.put("/objects/{bucket}/{key}")
async def stream_upload(
    bucket: str,
    key: str,
    request: Request,
    mode: str = Query("fixed"),
    x_object_tags: str = Header(
        default="{}",
        alias="X-Object-Tags"
    )
):
    validate_name(bucket)
    validate_name(key)

    parsed_tags = parse_tags(x_object_tags)

    if mode not in ("fixed", "cdc"):
        raise HTTPException(
            status_code=400,
            detail="Invalid chunking mode"
        )

    fd, filename = tempfile.mkstemp()

    try:
        with os.fdopen(fd, "wb") as output:
            async for block in request.stream():
                if block:
                    output.write(block)

        def forward():
            with open(filename, "rb") as source:
                return perform_upload(
                    bucket=bucket,
                    key=key,
                    file_obj=source,
                    filename=key,
                    mode=mode,
                    tags=parsed_tags
                )

        return await run_in_threadpool(forward)

    finally:
        Path(filename).unlink(missing_ok=True)


# ============================================================
# LIST AND FILTER OBJECTS - POSTGRESQL
# ============================================================

@app.get("/objects")
def list_and_search_objects(
    bucket: str | None = None,
    prefix: str | None = None,
    min_size: int | None = Query(None, ge=0),
    max_size: int | None = Query(None, ge=0),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    tags: str = "{}",
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    if bucket is not None:
        validate_name(bucket)

    if (
        min_size is not None
        and max_size is not None
        and min_size > max_size
    ):
        raise HTTPException(
            status_code=400,
            detail="Invalid size range"
        )

    if (
        date_from is not None
        and date_to is not None
        and date_from > date_to
    ):
        raise HTTPException(
            status_code=400,
            detail="Invalid date range"
        )

    rows = search_objects(
        bucket=bucket,
        prefix=prefix,
        min_size=min_size,
        max_size=max_size,
        date_from=date_from,
        date_to=date_to,
        tags=parse_tags(tags),
        limit=limit,
        offset=offset
    )

    return {
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "objects": [
            serialize(row)
            for row in rows
        ]
    }


# ============================================================
# OBJECT METADATA
# ============================================================

@app.get("/objects/{bucket}/{key}/metadata")
def object_metadata(
    bucket: str,
    key: str,
    version: int | None = None
):
    validate_name(bucket)
    validate_name(key)

    obj = get_object(
        bucket,
        key,
        version
    )

    if obj is None:
        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    manifest = read_manifest(
        bucket,
        key,
        obj["version"]
    )

    return {
        "object": serialize(obj),
        "manifest": manifest
    }


# ============================================================
# HEAD OBJECT
# ============================================================

@app.head("/objects/{bucket}/{key}")
def head_object(
    bucket: str,
    key: str,
    version: int | None = None
):
    validate_name(bucket)
    validate_name(key)

    obj = get_object(
        bucket,
        key,
        version
    )

    if obj is None:
        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    read_manifest(
        bucket,
        key,
        obj["version"]
    )

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(obj["size"]),
            "X-Object-Version": str(obj["version"]),
            "X-Chunk-Count": str(obj["chunk_count"]),
            "Accept-Ranges": "bytes"
        }
    )


# ============================================================
# DOWNLOAD OBJECT WITH BYTE RANGE SUPPORT
# ============================================================

@app.get("/objects/{bucket}/{key}")
def download_object(
    bucket: str,
    key: str,
    version: int | None = None,
    range_header: str | None = Header(
        default=None,
        alias="Range"
    )
):
    validate_name(bucket)
    validate_name(key)

    obj = get_object(
        bucket,
        key,
        version
    )

    if obj is None:
        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    read_manifest(
        bucket,
        key,
        obj["version"]
    )

    size = obj["size"]

    if size == 0:
        if range_header is not None:
            raise HTTPException(
                status_code=416,
                detail="Empty object",
                headers={
                    "Content-Range": "bytes */0"
                }
            )

        return Response(
            content=b"",
            media_type="application/octet-stream",
            headers={
                "Content-Length": "0",
                "Accept-Ranges": "bytes",
                "X-Object-Version": str(obj["version"])
            }
        )

    start, end, partial = parse_range(
        range_header,
        size
    )

    client = httpx.Client(timeout=TIMEOUT)

    try:
        upstream_request = client.build_request(
            "GET",
            METADATA_URL + backend_object_path(
                bucket,
                key
            ),
            params={
                "version": obj["version"]
            }
        )

        upstream = client.send(
            upstream_request,
            stream=True
        )

    except httpx.HTTPError as error:
        client.close()

        raise HTTPException(
            status_code=502,
            detail=f"Storage backend unavailable: {error}"
        )

    if upstream.status_code != 200:
        status = upstream.status_code
        upstream.close()
        client.close()

        raise HTTPException(
            status_code=503,
            detail=(
                "Object unavailable in storage backend "
                f"(upstream status {status})"
            )
        )

    def stream_body():
        position = 0

        try:
            for block in upstream.iter_bytes(
                chunk_size=1024 * 1024
            ):
                if not block:
                    continue

                block_end = position + len(block) - 1

                if block_end < start:
                    position += len(block)
                    continue

                if position > end:
                    break

                first = max(
                    0,
                    start - position
                )

                last = min(
                    len(block),
                    end - position + 1
                )

                if first < last:
                    yield block[first:last]

                position += len(block)

                if position > end:
                    break

        finally:
            upstream.close()
            client.close()

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
        "X-Object-Version": str(obj["version"])
    }

    if partial:
        headers["Content-Range"] = (
            f"bytes {start}-{end}/{size}"
        )

    return StreamingResponse(
        stream_body(),
        status_code=206 if partial else 200,
        media_type="application/octet-stream",
        headers=headers
    )


# ============================================================
# DELETE OBJECT
# ============================================================

@app.delete("/objects/{bucket}/{key}")
def delete_object(
    bucket: str,
    key: str,
    version: int | None = None
):
    validate_name(bucket)
    validate_name(key)

    obj = get_object(
        bucket,
        key,
        version
    )

    if obj is None:
        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    result = metadata_json(
        "DELETE",
        backend_object_path(bucket, key),
        params={
            "version": obj["version"]
        }
    )

    delete_object_record(
        bucket,
        key,
        obj["version"]
    )

    try:
        remove_object(
            bucket,
            key,
            obj["version"]
        )

    except Exception as error:
        print(
            f"Elasticsearch deletion failed: {error}",
            flush=True
        )

    return result


# ============================================================
# ELASTICSEARCH OBJECT SEARCH
# ============================================================

@app.get("/search")
def elasticsearch_objects(
    bucket: str | None = None,
    prefix: str | None = None,
    min_size: int | None = Query(None, ge=0),
    max_size: int | None = Query(None, ge=0),
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    tag_key: str | None = None,
    tag_value: str | None = None,
    limit: int = Query(100, ge=1, le=1000)
):
    if bucket is not None:
        validate_name(bucket)

    if (tag_key is None) != (tag_value is None):
        raise HTTPException(
            status_code=400,
            detail="Provide both tag_key and tag_value"
        )

    if tag_key is not None:
        if not re.fullmatch(
            r"[A-Za-z0-9_-]{1,100}",
            tag_key
        ):
            raise HTTPException(
                status_code=400,
                detail="Invalid tag key"
            )

    if (
        min_size is not None
        and max_size is not None
        and min_size > max_size
    ):
        raise HTTPException(
            status_code=400,
            detail="Invalid size range"
        )

    if (
        date_from is not None
        and date_to is not None
        and date_from > date_to
    ):
        raise HTTPException(
            status_code=400,
            detail="Invalid date range"
        )

    try:
        return search_objects_es(
            bucket=bucket,
            prefix=prefix,
            min_size=min_size,
            max_size=max_size,
            date_from=(
                date_from.isoformat()
                if date_from
                else None
            ),
            date_to=(
                date_to.isoformat()
                if date_to
                else None
            ),
            tag_key=tag_key,
            tag_value=tag_value,
            limit=limit
        )

    except httpx.HTTPError as error:
        raise HTTPException(
            status_code=503,
            detail=f"Elasticsearch unavailable: {error}"
        )


# ============================================================
# REBUILD ELASTICSEARCH INDEX
# ============================================================

@app.post("/search/reindex")
def reindex_search():
    try:
        return rebuild_index()

    except httpx.HTTPError as error:
        raise HTTPException(
            status_code=503,
            detail=f"Elasticsearch unavailable: {error}"
        )


# ============================================================
# CLUSTER STATUS
# ============================================================

@app.get("/cluster/status")
def cluster_status():
    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    node_id,
                    free_space,
                    chunk_count,
                    last_seen,
                    (
                        last_seen >
                        NOW() - INTERVAL '15 seconds'
                    ) AS online

                FROM nodes

                ORDER BY node_id
            """)

            nodes = cur.fetchall()

            cur.execute("""
                SELECT
                    c.id,
                    c.method,
                    c.n,
                    c.k,
                    c.m,
                    c.r,

                    COUNT(f.shard_index) FILTER (
                        WHERE
                            n.last_seen >
                                NOW() - INTERVAL '15 seconds'
                            AND f.status = 'healthy'
                    ) AS healthy_fragments

                FROM ft_chunks c

                LEFT JOIN ft_fragments f
                    ON f.chunk_id = c.id

                LEFT JOIN nodes n
                    ON n.node_id = f.node_id

                WHERE c.ref_count > 0

                GROUP BY
                    c.id,
                    c.method,
                    c.n,
                    c.k,
                    c.m,
                    c.r
            """)

            chunk_rows = cur.fetchall()

            cur.execute("""
                SELECT
                    object_id,
                    chunk_id

                FROM ft_object_chunks
            """)

            relations = cur.fetchall()

    chunk_states = {}

    for chunk in chunk_rows:
        available = chunk["healthy_fragments"]

        if chunk["method"] == "replication":
            target = chunk["n"]
            required = chunk["r"]

        else:
            target = chunk["k"] + chunk["m"]
            required = chunk["k"]

        chunk_states[chunk["id"]] = {
            "degraded": available < target,
            "unavailable": available < required,
            "healthy_fragments": available,
            "required_fragments": required,
            "target_fragments": target
        }

    degraded_objects = set()
    unavailable_objects = set()

    for relation in relations:
        state = chunk_states.get(
            relation["chunk_id"]
        )

        if state is None:
            continue

        if state["degraded"]:
            degraded_objects.add(
                relation["object_id"]
            )

        if state["unavailable"]:
            unavailable_objects.add(
                relation["object_id"]
            )

    online_nodes = [
        node for node in nodes
        if node["online"]
    ]

    total_free_space = sum(
        node["free_space"]
        for node in online_nodes
    )

    try:
        storage_stats = metadata_json(
            "GET",
            "/v4/stats"
        )

    except HTTPException:
        storage_stats = {}

    return {
        "total_nodes": len(nodes),
        "online_nodes": len(online_nodes),
        "offline_nodes": len(nodes) - len(online_nodes),
        "total_free_space_bytes": total_free_space,

        "nodes": [
            {
                "node_id": node["node_id"],
                "status": (
                    "online"
                    if node["online"]
                    else "offline"
                ),
                "free_space": node["free_space"],
                "chunk_count": node["chunk_count"],
                "last_seen": (
                    node["last_seen"].isoformat()
                    if node["last_seen"]
                    else None
                )
            }
            for node in nodes
        ],

        "unique_protected_chunks": len(chunk_rows),

        "degraded_chunks": sum(
            1
            for state in chunk_states.values()
            if state["degraded"]
        ),

        "unavailable_chunks": sum(
            1
            for state in chunk_states.values()
            if state["unavailable"]
        ),

        "degraded_objects": len(degraded_objects),
        "unavailable_objects": len(unavailable_objects),

        "storage_stats": storage_stats
    }


# ============================================================
# REPAIR MONITORING
# ============================================================

@app.get("/repair/progress")
def repair_progress():
    metrics = metadata_json(
        "GET",
        "/v4/repair/metrics"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT
                    COUNT(*) AS degraded_fragments

                FROM ft_fragments f

                LEFT JOIN nodes n
                    ON n.node_id = f.node_id

                WHERE
                    f.status != 'healthy'

                    OR n.last_seen IS NULL

                    OR n.last_seen <
                        NOW() - INTERVAL '15 seconds'
            """)

            row = cur.fetchone()

    return {
        "metrics": metrics,
        "degraded_fragments": row["degraded_fragments"],
        "worker": "periodic background repair"
    }

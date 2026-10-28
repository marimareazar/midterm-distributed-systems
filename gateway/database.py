
import os

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://storage:storagepass@db:5432/storage"
)


def get_connection():
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row
    )


def initialize_database():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS gw_buckets (
                    name TEXT PRIMARY KEY,
                    method TEXT NOT NULL,
                    n INTEGER NOT NULL,
                    w INTEGER NOT NULL,
                    r INTEGER NOT NULL,
                    k INTEGER NOT NULL,
                    m INTEGER NOT NULL,
                    created_at TIMESTAMPTZ
                        NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS gw_objects (
                    id BIGSERIAL PRIMARY KEY,
                    bucket TEXT NOT NULL
                        REFERENCES gw_buckets(name),
                    object_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    size BIGINT NOT NULL,
                    chunk_count INTEGER NOT NULL,
                    chunking_mode TEXT NOT NULL,
                    tags JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ
                        NOT NULL DEFAULT NOW(),
                    UNIQUE(bucket, object_key, version)
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_gw_objects_key
                ON gw_objects(bucket, object_key)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_gw_objects_size
                ON gw_objects(size)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_gw_objects_created
                ON gw_objects(created_at)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                    idx_gw_objects_tags
                ON gw_objects USING GIN(tags)
            """)


def save_bucket(name, policy):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gw_buckets
                    (name, method, n, w, r, k, m)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name)
                DO UPDATE SET
                    method=EXCLUDED.method,
                    n=EXCLUDED.n,
                    w=EXCLUDED.w,
                    r=EXCLUDED.r,
                    k=EXCLUDED.k,
                    m=EXCLUDED.m
            """, (
                name,
                policy["method"],
                policy["n"],
                policy["w"],
                policy["r"],
                policy["k"],
                policy["m"]
            ))


def bucket_exists(name):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM gw_buckets WHERE name=%s",
                (name,)
            )
            return cur.fetchone() is not None


def list_buckets():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM gw_buckets
                ORDER BY name
            """)
            return cur.fetchall()


def save_object(
    bucket, key, version, size,
    chunk_count, mode, tags
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gw_objects (
                    bucket, object_key, version,
                    size, chunk_count, chunking_mode,
                    tags
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bucket, object_key, version)
                DO UPDATE SET
                    size=EXCLUDED.size,
                    chunk_count=EXCLUDED.chunk_count,
                    chunking_mode=EXCLUDED.chunking_mode,
                    tags=EXCLUDED.tags
            """, (
                bucket, key, version, size,
                chunk_count, mode, Jsonb(tags)
            ))


def get_object(bucket, key, version=None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            if version is None:
                cur.execute("""
                    SELECT * FROM gw_objects
                    WHERE bucket=%s AND object_key=%s
                    ORDER BY version DESC LIMIT 1
                """, (bucket, key))
            else:
                cur.execute("""
                    SELECT * FROM gw_objects
                    WHERE bucket=%s AND object_key=%s
                      AND version=%s
                """, (bucket, key, version))

            return cur.fetchone()


def delete_object_record(bucket, key, version):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM gw_objects
                WHERE bucket=%s AND object_key=%s
                  AND version=%s
            """, (bucket, key, version))


def search_objects(
    bucket=None,
    prefix=None,
    min_size=None,
    max_size=None,
    date_from=None,
    date_to=None,
    tags=None,
    limit=100,
    offset=0
):
    tags = tags or {}

    query = """
        SELECT *
        FROM gw_objects
        WHERE (%s IS NULL OR bucket=%s)
          AND (%s IS NULL OR object_key LIKE %s)
          AND (%s IS NULL OR size >= %s)
          AND (%s IS NULL OR size <= %s)
          AND (%s IS NULL OR created_at >= %s)
          AND (%s IS NULL OR created_at <= %s)
          AND tags @> %s
        ORDER BY created_at DESC, id DESC
        LIMIT %s OFFSET %s
    """

    prefix_pattern = (
        prefix.replace("\\", "\\\\")
              .replace("%", "\\%")
              .replace("_", "\\_") + "%"
        if prefix is not None else None
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (
                bucket, bucket,
                prefix, prefix_pattern,
                min_size, min_size,
                max_size, max_size,
                date_from, date_from,
                date_to, date_to,
                Jsonb(tags),
                limit, offset
            ))
            return cur.fetchall()


def serialize(row):
    if row is None:
        return None

    result = dict(row)

    if result.get("created_at"):
        result["created_at"] = (
            result["created_at"].isoformat()
        )

    return result

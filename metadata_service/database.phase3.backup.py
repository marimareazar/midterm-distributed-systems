
import os

import psycopg
from psycopg.rows import dict_row

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
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    free_space BIGINT NOT NULL DEFAULT 0,
                    chunk_count INTEGER NOT NULL DEFAULT 0,
                    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    id BIGSERIAL PRIMARY KEY,
                    bucket TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    size BIGINT NOT NULL,
                    chunking_mode TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                        DEFAULT NOW(),
                    UNIQUE(bucket, object_key, version)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_hash CHAR(64) PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    size BIGINT NOT NULL,
                    ref_count BIGINT NOT NULL
                        CHECK (ref_count >= 0)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS object_chunks (
                    object_id BIGINT NOT NULL
                        REFERENCES objects(id)
                        ON DELETE CASCADE,
                    position INTEGER NOT NULL,
                    chunk_hash CHAR(64) NOT NULL
                        REFERENCES chunks(chunk_hash),
                    PRIMARY KEY(object_id, position)
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_objects_bucket_key
                ON objects(bucket, object_key)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS
                idx_object_chunks_hash
                ON object_chunks(chunk_hash)
            """)

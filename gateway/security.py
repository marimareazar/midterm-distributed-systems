
import hmac
import os
import threading
import time
from collections import deque

from fastapi import Header, HTTPException

API_KEY = os.getenv("GATEWAY_API_KEY", "")
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "6000"))
RATE_WINDOW = 60


class RateLimiter:
    def __init__(self, limit: int, window: int = 60):
        self.limit = limit
        self.window = window
        self.requests = {}
        self.lock = threading.Lock()

    def check(self, identity: str):
        now = time.monotonic()

        with self.lock:
            records = self.requests.setdefault(
                identity, deque()
            )

            while records and now - records[0] >= self.window:
                records.popleft()

            if len(records) >= self.limit:
                retry_after = max(
                    1,
                    int(self.window - (now - records[0])) + 1
                )

                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded",
                    headers={"Retry-After": str(retry_after)}
                )

            records.append(now)


limiter = RateLimiter(RATE_LIMIT, RATE_WINDOW)


def require_api_key(
    x_api_key: str | None = Header(
        default=None,
        alias="X-API-Key"
    )
):
    if not API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Gateway API key is not configured"
        )

    if not x_api_key or not hmac.compare_digest(
        x_api_key, API_KEY
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"}
        )

    limiter.check(x_api_key)
    return x_api_key

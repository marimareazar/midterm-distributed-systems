import os
import sys
import uuid
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(ROOT / "gateway"))

from gateway.main import parse_range
from gateway.security import RateLimiter
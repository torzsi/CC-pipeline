from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse


LAZY_IMAGE_ATTRS = (
    "src",
    "data-src",
    "data-original",
    "data-lazy-src",
    "data-url",
)


def resolve_image_url(page_url: str, raw_url: str) -> str:
    return urljoin(page_url, raw_url.strip())


def infer_storage_path(source_url: str, *, prefix: str = "images") -> str:
    parsed = urlparse(source_url)
    suffix = Path(parsed.path).suffix or ".bin"
    digest = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    return f"s3://local-bucket/{prefix}/{digest[:2]}/{digest}{suffix}"

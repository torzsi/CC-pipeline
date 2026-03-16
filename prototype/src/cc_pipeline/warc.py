from __future__ import annotations

from dataclasses import dataclass
import gzip
from pathlib import Path
from typing import Iterator
from urllib.request import Request, urlopen


@dataclass
class WarcRecord:
    headers: dict[str, str]
    http_headers: dict[str, str]
    body: bytes

    @property
    def warc_type(self) -> str | None:
        return self.headers.get("WARC-Type")

    @property
    def target_uri(self) -> str | None:
        return self.headers.get("WARC-Target-URI")

    @property
    def content_type(self) -> str | None:
        raw = (
            self.http_headers.get("Content-Type")
            or self.http_headers.get("content-type")
            or self.headers.get("WARC-Identified-Payload-Type")
            or self.headers.get("Content-Type")
        )
        if raw is None:
            return None
        return raw.split(";", 1)[0].strip().lower()

    def text(self, *, default_encoding: str = "utf-8") -> str:
        return self.body.decode(default_encoding, errors="replace")


class LocalWarcReader:
    def read_range(self, path: str | Path, *, offset: int, length: int) -> bytes:
        with Path(path).open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)

    def read_record(self, path: str | Path, *, offset: int, length: int) -> WarcRecord:
        payload = self.read_range(path, offset=offset, length=length)
        if str(path).endswith(".gz"):
            payload = gzip.decompress(payload)
        return parse_warc_record(payload)


class RemoteWarcReader:
    def __init__(self, *, http_prefix: str = "https://data.commoncrawl.org/") -> None:
        self.http_prefix = http_prefix.rstrip("/") + "/"

    def read_range(self, path: str, *, offset: int, length: int) -> bytes:
        target = self._resolve_url(path)
        headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
        request = Request(target, headers=headers)
        with urlopen(request) as response:
            return response.read()

    def read_record(self, path: str, *, offset: int, length: int) -> WarcRecord:
        payload = self.read_range(path, offset=offset, length=length)
        if path.endswith(".gz"):
            payload = gzip.decompress(payload)
        return parse_warc_record(payload)

    def _resolve_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self.http_prefix + path.lstrip("/")


def parse_warc_record(payload: bytes) -> WarcRecord:
    warc_header_blob, remainder = _split_once(payload, b"\r\n\r\n")
    warc_headers = _parse_header_lines(warc_header_blob)

    if remainder.startswith(b"HTTP/"):
        _, http_remainder = _split_once(remainder, b"\r\n")
        http_header_blob, body = _split_once(http_remainder, b"\r\n\r\n")
        http_headers = _parse_header_lines(http_header_blob)
    else:
        http_headers = {}
        body = remainder

    return WarcRecord(headers=warc_headers, http_headers=http_headers, body=body.rstrip(b"\r\n"))


def iter_warc_records(payload: bytes) -> Iterator[WarcRecord]:
    cursor = payload
    marker = b"WARC/"
    while cursor:
        start = cursor.find(marker)
        if start == -1:
            return
        cursor = cursor[start:]
        next_start = cursor.find(b"\r\nWARC/", 1)
        if next_start == -1:
            yield parse_warc_record(cursor)
            return
        yield parse_warc_record(cursor[: next_start + 2].rstrip(b"\r\n"))
        cursor = cursor[next_start + 2 :]


def _parse_header_lines(blob: bytes) -> dict[str, str]:
    headers: dict[str, str] = {}
    for raw_line in blob.decode("utf-8", errors="replace").splitlines():
        if ":" not in raw_line:
            continue
        key, value = raw_line.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


def _split_once(payload: bytes, delimiter: bytes) -> tuple[bytes, bytes]:
    parts = payload.split(delimiter, 1)
    if len(parts) != 2:
        raise ValueError("invalid WARC payload structure")
    return parts[0], parts[1]

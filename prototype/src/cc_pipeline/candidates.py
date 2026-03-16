from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse


SUPPORTED_CONTENT_TYPES = ("text/html", "application/pdf")
BLOCKED_EXTENSIONS = (".css", ".js", ".json", ".xml", ".txt")


@dataclass
class CCIndexEntry:
    url: str
    mime: str | None = None
    status: str | int | None = None
    length: int | None = None
    filename: str | None = None
    offset: int | None = None
    languages: str | None = None
    timestamp: str | None = None
    digest: str | None = None
    charset: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "mime": self.mime,
            "status": self.status,
            "length": self.length,
            "filename": self.filename,
            "offset": self.offset,
            "languages": self.languages,
            "timestamp": self.timestamp,
            "digest": self.digest,
            "charset": self.charset,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CCIndexEntry":
        return cls(
            url=str(payload.get("url") or ""),
            mime=payload.get("mime"),
            status=payload.get("status"),
            length=_coerce_int(payload.get("length")),
            filename=payload.get("filename"),
            offset=_coerce_int(payload.get("offset")),
            languages=payload.get("languages"),
            timestamp=payload.get("timestamp"),
            digest=payload.get("digest"),
            charset=payload.get("charset"),
        )


@dataclass
class CandidateSelection:
    keep: bool
    score: float
    reasons: list[str]


class CandidateSelector:
    def __init__(
        self,
        *,
        min_length: int = 1024,
        supported_content_types: tuple[str, ...] = SUPPORTED_CONTENT_TYPES,
    ) -> None:
        self.min_length = min_length
        self.supported_content_types = tuple(value.lower() for value in supported_content_types)

    def evaluate(self, entry: CCIndexEntry) -> CandidateSelection:
        reasons: list[str] = []
        score = 0.0

        if not entry.url:
            return CandidateSelection(False, score, ["missing_url"])

        status = str(entry.status or "")
        if status and status != "200":
            reasons.append("http_status_not_200")

        mime = (entry.mime or "").lower()
        if not any(mime.startswith(prefix) for prefix in self.supported_content_types):
            reasons.append("unsupported_mime")
        elif mime.startswith("text/html"):
            score += 3.0
        elif mime.startswith("application/pdf"):
            score += 2.0

        if entry.length is None or entry.length < self.min_length:
            reasons.append("record_too_small")
        else:
            score += min(entry.length / 100_000, 2.0)

        if entry.offset is None or not entry.filename:
            reasons.append("missing_warc_pointer")
        else:
            score += 1.0

        parsed = urlparse(entry.url)
        if any(parsed.path.lower().endswith(ext) for ext in BLOCKED_EXTENSIONS):
            reasons.append("blocked_extension")

        if "image" in parsed.path.lower() or "gallery" in parsed.path.lower():
            score += 0.5
        if "article" in parsed.path.lower() or "blog" in parsed.path.lower():
            score += 0.5

        if entry.languages:
            score += 0.25

        return CandidateSelection(not reasons, round(score, 4), reasons)

    def select(self, entries: Iterable[CCIndexEntry]) -> list[tuple[CCIndexEntry, CandidateSelection]]:
        selected: list[tuple[CCIndexEntry, CandidateSelection]] = []
        for entry in entries:
            decision = self.evaluate(entry)
            if decision.keep:
                selected.append((entry, decision))
        return selected


def load_index_entries(path: str | Path) -> list[CCIndexEntry]:
    entries: list[CCIndexEntry] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        entries.append(CCIndexEntry.from_dict(json.loads(line)))
    return entries


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)

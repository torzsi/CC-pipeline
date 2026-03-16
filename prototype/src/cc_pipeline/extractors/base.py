from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class ExtractedText:
    text: str


@dataclass
class ExtractedImage:
    source_url: str
    width: int | None = None
    height: int | None = None
    alt_text: str | None = None


@dataclass
class ExtractionResult:
    title: str | None
    language: str | None
    slots: list[ExtractedText | ExtractedImage] = field(default_factory=list)


class Extractor(Protocol):
    def extract(self, content: str, *, page_url: str) -> ExtractionResult:
        """Extract interleaved multimodal slots from a source document."""

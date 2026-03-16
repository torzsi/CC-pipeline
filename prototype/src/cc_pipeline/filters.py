from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse

from .schema import InterleavedRecord
from .text import normalize_text


@dataclass
class FilterDecision:
    keep: bool
    reasons: list[str]


@dataclass
class TextFilter:
    min_chars: int = 50
    max_repetition_ratio: float = 0.3

    def evaluate(self, text: str) -> FilterDecision:
        normalized = normalize_text(text)
        if len(normalized) < self.min_chars:
            return FilterDecision(False, ["text_too_short"])
        tokens = normalized.lower().split()
        if not tokens:
            return FilterDecision(False, ["text_empty"])
        unique_ratio = len(set(tokens)) / len(tokens)
        if unique_ratio < self.max_repetition_ratio:
            return FilterDecision(False, ["text_repetitive"])
        return FilterDecision(True, [])


@dataclass
class ImageFilter:
    min_width: int = 128
    min_height: int = 128
    blocked_extensions: tuple[str, ...] = (".svg", ".ico")

    def evaluate(self, *, source_url: str, width: int | None, height: int | None) -> FilterDecision:
        parsed = urlparse(source_url)
        if any(parsed.path.lower().endswith(ext) for ext in self.blocked_extensions):
            return FilterDecision(False, ["blocked_extension"])
        if width is not None and width < self.min_width:
            return FilterDecision(False, ["image_width_too_small"])
        if height is not None and height < self.min_height:
            return FilterDecision(False, ["image_height_too_small"])
        return FilterDecision(True, [])


@dataclass
class RecordFilter:
    text_filter: TextFilter = field(default_factory=TextFilter)
    image_filter: ImageFilter = field(default_factory=ImageFilter)

    def evaluate(self, record: InterleavedRecord) -> FilterDecision:
        reasons: list[str] = []
        text_payload = " ".join(text for text in record.texts if text)
        text_decision = self.text_filter.evaluate(text_payload)
        if not text_decision.keep:
            reasons.extend(text_decision.reasons)

        image_slots = 0
        for idx, image_path in enumerate(record.image):
            if image_path is None:
                continue
            image_slots += 1
            decision = self.image_filter.evaluate(
                source_url=record.url[idx] or "",
                width=record.width[idx],
                height=record.height[idx],
            )
            if not decision.keep:
                reasons.extend(decision.reasons)

        if image_slots == 0:
            reasons.append("no_images")

        return FilterDecision(not reasons, reasons)

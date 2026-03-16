from __future__ import annotations

from dataclasses import dataclass, field

from .schema import InterleavedRecord
from .text import jaccard_similarity, stable_text_hash


@dataclass
class DedupDecision:
    is_duplicate: bool
    reason: str | None = None
    existing_id: str | None = None


@dataclass
class Deduplicator:
    near_text_threshold: float = 0.9
    _exact_text: dict[str, str] = field(default_factory=dict)
    _near_text: dict[str, str] = field(default_factory=dict)

    def check(self, record: InterleavedRecord, *, record_id: str) -> DedupDecision:
        text_payload = " ".join(text for text in record.texts if text)
        text_hash = stable_text_hash(text_payload)
        existing = self._exact_text.get(text_hash)
        if existing is not None:
            return DedupDecision(True, "exact_text", existing)

        for previous_text, previous_id in self._near_text.items():
            if jaccard_similarity(previous_text, text_payload) >= self.near_text_threshold:
                return DedupDecision(True, "near_text", previous_id)

        self._exact_text[text_hash] = record_id
        self._near_text[text_payload] = record_id
        return DedupDecision(False)

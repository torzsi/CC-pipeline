from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any


@dataclass
class GeneralMetadata:
    source_url: str
    canonical_url: str
    crawl_id: str | None = None
    warc_path: str | None = None
    warc_offset: int | None = None
    warc_length: int | None = None
    language: str | None = None
    title: str | None = None
    fetch_time: str | None = None
    text_quality_score: float | None = None
    image_quality_score: float | None = None
    mm_relevance_score: float | None = None
    safety_flags: dict[str, Any] = field(default_factory=dict)
    dedup_signatures: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_url": self.source_url,
            "canonical_url": self.canonical_url,
            "crawl_id": self.crawl_id,
            "warc_path": self.warc_path,
            "warc_offset": self.warc_offset,
            "warc_length": self.warc_length,
            "language": self.language,
            "title": self.title,
            "fetch_time": self.fetch_time,
            "text_quality_score": self.text_quality_score,
            "image_quality_score": self.image_quality_score,
            "mm_relevance_score": self.mm_relevance_score,
            "safety_flags": self.safety_flags,
            "dedup_signatures": self.dedup_signatures,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GeneralMetadata":
        return cls(
            source_url=payload["source_url"],
            canonical_url=payload["canonical_url"],
            crawl_id=payload.get("crawl_id"),
            warc_path=payload.get("warc_path"),
            warc_offset=payload.get("warc_offset"),
            warc_length=payload.get("warc_length"),
            language=payload.get("language"),
            title=payload.get("title"),
            fetch_time=payload.get("fetch_time"),
            text_quality_score=payload.get("text_quality_score"),
            image_quality_score=payload.get("image_quality_score"),
            mm_relevance_score=payload.get("mm_relevance_score"),
            safety_flags=payload.get("safety_flags", {}),
            dedup_signatures=payload.get("dedup_signatures", {}),
        )


@dataclass
class InterleavedRecord:
    texts: list[str | None]
    image: list[str | None]
    width: list[int | None]
    height: list[int | None]
    url: list[str | None]
    general_metadata: GeneralMetadata
    data_name: str = "commoncrawl_interleaved"
    meta: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        sizes = {
            len(self.texts),
            len(self.image),
            len(self.width),
            len(self.height),
            len(self.url),
        }
        if len(sizes) != 1:
            raise ValueError("texts, image, width, height, and url must have identical lengths")

        for idx, text in enumerate(self.texts):
            is_text_slot = text is not None
            has_image_payload = any(
                value is not None for value in (self.image[idx], self.width[idx], self.height[idx], self.url[idx])
            )
            if is_text_slot and has_image_payload:
                raise ValueError(f"slot {idx} mixes text and image payloads")
            if not is_text_slot and self.image[idx] is None and self.url[idx] is not None:
                raise ValueError(f"slot {idx} has url but no image")
            if not is_text_slot and self.image[idx] is None and (
                self.width[idx] is not None or self.height[idx] is not None
            ):
                raise ValueError(f"slot {idx} has dimensions but no image")
            if not is_text_slot and self.image[idx] is not None and self.url[idx] is None:
                raise ValueError(f"slot {idx} has image but no source url")

        if not any(text is not None for text in self.texts):
            raise ValueError("record must contain at least one text slot")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "texts": self.texts,
            "image": self.image,
            "width": self.width,
            "height": self.height,
            "url": self.url,
            "general_metadata": self.general_metadata.to_dict(),
            "data_name": self.data_name,
            "meta": self.meta,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=True)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "InterleavedRecord":
        return cls(
            texts=payload["texts"],
            image=payload["image"],
            width=payload["width"],
            height=payload["height"],
            url=payload["url"],
            general_metadata=GeneralMetadata.from_dict(payload["general_metadata"]),
            data_name=payload.get("data_name", "commoncrawl_interleaved"),
            meta=payload.get("meta", {}),
        )

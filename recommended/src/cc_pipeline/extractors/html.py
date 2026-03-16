from __future__ import annotations

from html.parser import HTMLParser

from .base import ExtractedImage, ExtractedText, ExtractionResult
from ..image import LAZY_IMAGE_ATTRS, resolve_image_url
from ..text import normalize_text


BLOCK_TAGS = {
    "article",
    "aside",
    "blockquote",
    "div",
    "figcaption",
    "figure",
    "footer",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "p",
    "section",
}
SKIP_TAGS = {"noscript", "script", "style"}


class _DOMInterleaver(HTMLParser):
    def __init__(self, *, page_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.in_title = False
        self.skip_depth = 0
        self.title: str | None = None
        self.language: str | None = None
        self._text_buffer: list[str] = []
        self._slots: list[ExtractedText | ExtractedImage] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs}
        if tag == "html":
            self.language = attr_map.get("lang") or self.language
        if tag == "title":
            self.in_title = True
        if tag in SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth:
            return
        if tag in BLOCK_TAGS:
            self._flush_text()
        if tag == "img":
            raw_url = next((attr_map.get(name) for name in LAZY_IMAGE_ATTRS if attr_map.get(name)), None)
            if not raw_url:
                return
            self._flush_text()
            self._slots.append(
                ExtractedImage(
                    source_url=resolve_image_url(self.page_url, raw_url),
                    width=_parse_dimension(attr_map.get("width")),
                    height=_parse_dimension(attr_map.get("height")),
                    alt_text=attr_map.get("alt"),
                )
            )

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False
        if tag in SKIP_TAGS and self.skip_depth:
            self.skip_depth -= 1
            return
        if self.skip_depth:
            return
        if tag in BLOCK_TAGS:
            self._flush_text()

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        normalized = normalize_text(data)
        if not normalized:
            return
        if self.in_title:
            self.title = normalized
            return
        self._text_buffer.append(normalized)

    def close(self) -> ExtractionResult:
        super().close()
        self._flush_text()
        return ExtractionResult(title=self.title, language=self.language, slots=self._slots)

    def _flush_text(self) -> None:
        if not self._text_buffer:
            return
        text = normalize_text(" ".join(self._text_buffer))
        self._text_buffer.clear()
        if text:
            self._slots.append(ExtractedText(text=text))


def _parse_dimension(raw: str | None) -> int | None:
    if raw is None:
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    return int(digits) if digits else None


class HTMLExtractor:
    def extract(self, content: str, *, page_url: str) -> ExtractionResult:
        parser = _DOMInterleaver(page_url=page_url)
        parser.feed(content)
        return parser.close()

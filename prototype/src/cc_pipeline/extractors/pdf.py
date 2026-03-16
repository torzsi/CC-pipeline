from __future__ import annotations

from .base import ExtractionResult


class PDFExtractor:
    def extract(self, content: str, *, page_url: str) -> ExtractionResult:
        raise NotImplementedError("PDF extraction is planned but not implemented in milestone 1")

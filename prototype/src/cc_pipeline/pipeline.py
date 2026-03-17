from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path

from .candidates import CCIndexEntry, CandidateSelector
from .cdxj import CDXJIndexClient
from .columnar import ColumnarIndexClient
from .extractors import HTMLExtractor, PDFExtractor
from .filters import RecordFilter
from .image import infer_storage_path
from .manifests import JsonlWriter
from .schema import GeneralMetadata, InterleavedRecord
from .warc import LocalWarcReader, RemoteWarcReader, WarcRecord


@dataclass
class PipelineConfig:
    output_jsonl: str | Path
    candidate_manifest: str | Path | None = None
    document_manifest: str | Path | None = None
    data_name: str = "commoncrawl_interleaved"
    crawl_id: str = "local-dev"


@dataclass
class PipelineResult:
    kept: bool
    reasons: list[str] = field(default_factory=list)
    record: InterleavedRecord | None = None


@dataclass
class ExtractionStats:
    candidates_seen: int = 0
    candidates_selected: int = 0
    records_built: int = 0
    failures: int = 0


class PipelineRunner:
    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.html_extractor = HTMLExtractor()
        self.pdf_extractor = PDFExtractor()
        self.filter = RecordFilter()
        self.selector = CandidateSelector()
        self.columnar = ColumnarIndexClient()
        self.cdxj = CDXJIndexClient()
        self.warc_reader = LocalWarcReader()
        self.remote_warc_reader = RemoteWarcReader()
        self.output_writer = JsonlWriter(config.output_jsonl)
        self.candidate_writer = JsonlWriter(config.candidate_manifest) if config.candidate_manifest else None
        self.document_writer = JsonlWriter(config.document_manifest) if config.document_manifest else None

    def process_html(self, html: str, *, page_url: str, record_id: str | None = None) -> PipelineResult:
        record_id = record_id or self._default_record_id(page_url)
        self._write_candidate_manifest(
            record_id=record_id,
            source_url=page_url,
            selection_reason="local_html_input",
        )
        extracted = self.html_extractor.extract(html, page_url=page_url)
        record = self._build_record(extracted, page_url=page_url)
        return self._finalize_record(record, page_url=page_url, record_id=record_id)

    def process_candidate(self, entry: CCIndexEntry) -> PipelineResult:
        result = self.extract_candidate(entry)
        if not result.kept or result.record is None:
            return result
        return self._finalize_record(
            result.record,
            page_url=result.record.general_metadata.source_url,
            record_id=self._default_record_id(result.record.general_metadata.source_url),
        )

    def extract_candidate(self, entry: CCIndexEntry) -> PipelineResult:
        decision = self.selector.evaluate(entry)
        record_id = self._default_record_id(entry.url)
        self._write_candidate_manifest(
            record_id=record_id,
            source_url=entry.url,
            score=decision.score,
            candidate_reasons=decision.reasons,
            warc_path=entry.filename,
            warc_offset=entry.offset,
            warc_length=entry.length,
        )
        if not decision.keep:
            return PipelineResult(False, decision.reasons)
        if entry.filename is None or entry.offset is None or entry.length is None:
            return PipelineResult(False, ["missing_warc_pointer"])
        warc_record = self._read_warc_record(entry.filename, offset=entry.offset, length=entry.length)
        return self.extract_warc_record(
            warc_record,
            page_url=entry.url,
            record_id=record_id,
            warc_path=entry.filename,
            warc_offset=entry.offset,
            warc_length=entry.length,
        )

    def process_warc_record(
        self,
        warc_record: WarcRecord,
        *,
        page_url: str | None = None,
        record_id: str | None = None,
        warc_path: str | None = None,
        warc_offset: int | None = None,
        warc_length: int | None = None,
    ) -> PipelineResult:
        source_url = page_url or warc_record.target_uri or ""
        record_id = record_id or self._default_record_id(source_url)
        content_type = warc_record.content_type or ""

        if content_type.startswith("text/html"):
            extracted = self.html_extractor.extract(warc_record.text(), page_url=source_url)
        elif content_type.startswith("application/pdf"):
            try:
                extracted = self.pdf_extractor.extract(warc_record.body.decode("latin-1"), page_url=source_url)
            except NotImplementedError:
                return PipelineResult(False, ["pdf_extraction_not_implemented"])
        else:
            return PipelineResult(False, ["unsupported_content_type"])

        record = self._build_record(
            extracted,
            page_url=source_url,
            warc_path=warc_path,
            warc_offset=warc_offset,
            warc_length=warc_length,
        )
        return self._finalize_record(record, page_url=source_url, record_id=record_id)

    def extract_warc_record(
        self,
        warc_record: WarcRecord,
        *,
        page_url: str | None = None,
        record_id: str | None = None,
        warc_path: str | None = None,
        warc_offset: int | None = None,
        warc_length: int | None = None,
    ) -> PipelineResult:
        source_url = page_url or warc_record.target_uri or ""
        record_id = record_id or self._default_record_id(source_url)
        content_type = warc_record.content_type or ""

        if content_type.startswith("text/html"):
            extracted = self.html_extractor.extract(warc_record.text(), page_url=source_url)
        elif content_type.startswith("application/pdf"):
            try:
                extracted = self.pdf_extractor.extract(warc_record.body.decode("latin-1"), page_url=source_url)
            except NotImplementedError:
                return PipelineResult(False, ["pdf_extraction_not_implemented"])
        else:
            return PipelineResult(False, ["unsupported_content_type"])

        record = self._build_record(
            extracted,
            page_url=source_url,
            warc_path=warc_path,
            warc_offset=warc_offset,
            warc_length=warc_length,
        )
        return PipelineResult(True, record=record)

    def _finalize_record(self, record: InterleavedRecord, *, page_url: str, record_id: str) -> PipelineResult:
        filter_decision = self.filter.evaluate(record)
        if not filter_decision.keep:
            return PipelineResult(False, filter_decision.reasons, record)

        self.output_writer.write(record.to_dict())
        if self.document_writer is not None:
            self.document_writer.write(
                {
                    "record_id": record_id,
                    "source_url": page_url,
                    "output_path": str(self.config.output_jsonl),
                    "warc_path": record.general_metadata.warc_path,
                    "warc_offset": record.general_metadata.warc_offset,
                    "warc_length": record.general_metadata.warc_length,
                    "written_at": _utc_now(),
                }
            )
        return PipelineResult(True, record=record)

    def write_record(self, record: InterleavedRecord, *, record_id: str | None = None) -> None:
        record_id = record_id or self._default_record_id(record.general_metadata.source_url)
        self.output_writer.write(record.to_dict())
        if self.document_writer is not None:
            self.document_writer.write(
                {
                    "record_id": record_id,
                    "source_url": record.general_metadata.source_url,
                    "output_path": str(self.config.output_jsonl),
                    "warc_path": record.general_metadata.warc_path,
                    "warc_offset": record.general_metadata.warc_offset,
                    "warc_length": record.general_metadata.warc_length,
                    "written_at": _utc_now(),
                }
            )

    def _build_record(
        self,
        extracted,
        *,
        page_url: str,
        warc_path: str | None = None,
        warc_offset: int | None = None,
        warc_length: int | None = None,
    ) -> InterleavedRecord:
        texts: list[str | None] = []
        images: list[str | None] = []
        widths: list[int | None] = []
        heights: list[int | None] = []
        urls: list[str | None] = []

        for slot in extracted.slots:
            if hasattr(slot, "text"):
                texts.append(slot.text)
                images.append(None)
                widths.append(None)
                heights.append(None)
                urls.append(None)
                continue

            texts.append(None)
            images.append(infer_storage_path(slot.source_url))
            widths.append(slot.width)
            heights.append(slot.height)
            urls.append(slot.source_url)

        metadata = GeneralMetadata(
            source_url=page_url,
            canonical_url=page_url,
            crawl_id=self.config.crawl_id,
            warc_path=warc_path,
            warc_offset=warc_offset,
            warc_length=warc_length,
            language=extracted.language,
            title=extracted.title,
            fetch_time=_utc_now(),
        )
        return InterleavedRecord(
            texts=texts,
            image=images,
            width=widths,
            height=heights,
            url=urls,
            general_metadata=metadata,
            data_name=self.config.data_name,
            meta={"slot_count": len(texts)},
        )

    @staticmethod
    def _default_record_id(page_url: str) -> str:
        return page_url.rstrip("/").rsplit("/", 1)[-1] or "root"

    def query_columnar_index(
        self,
        *,
        crawl: str,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        limit: int = 10,
        path_limit: int | None = None,
    ) -> list[CCIndexEntry]:
        if domain is None and host is None and path_prefix is None and url_like is None:
            result = self.columnar.query(
                crawl=crawl,
                where_sql=self.columnar.build_html_where_sql(),
                limit=limit,
                path_limit=path_limit,
            )
        else:
            result = self.columnar.find_html_candidates(
                crawl=crawl,
                domain=domain,
                host=host,
                path_prefix=path_prefix,
                url_like=url_like,
                limit=limit,
                path_limit=path_limit,
            )
        return result.to_index_entries()

    def query_cdxj_index(
        self,
        *,
        crawl: str,
        url_pattern: str | None = None,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        limit: int = 10,
    ) -> list[CCIndexEntry]:
        result = self.cdxj.find_records(
            crawl=crawl,
            url_pattern=url_pattern,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
            limit=limit,
        )
        return result.to_index_entries()

    def iter_columnar_candidates(
        self,
        *,
        crawl: str,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        path_limit: int | None = None,
        path_batch_size: int = 1,
        rows_per_batch: int | None = None,
        candidate_limit: int | None = None,
    ):
        yielded = 0
        for batch in self.columnar.iter_html_candidate_batches(
            crawl=crawl,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
            path_limit=path_limit,
            path_batch_size=path_batch_size,
            rows_per_batch=rows_per_batch,
        ):
            for entry in batch.to_index_entries():
                yield entry
                yielded += 1
                if candidate_limit is not None and yielded >= candidate_limit:
                    return

    def generate_candidate_manifest_from_columnar(
        self,
        *,
        crawl: str,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        path_limit: int | None = None,
        path_batch_size: int = 1,
        rows_per_batch: int | None = None,
        candidate_limit: int | None = None,
    ) -> ExtractionStats:
        if self.candidate_writer is None:
            raise ValueError("candidate_manifest is required for candidate generation")

        stats = ExtractionStats()
        for entry in self.iter_columnar_candidates(
            crawl=crawl,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
            path_limit=path_limit,
            path_batch_size=path_batch_size,
            rows_per_batch=rows_per_batch,
            candidate_limit=candidate_limit,
        ):
            stats.candidates_seen += 1
            decision = self.selector.evaluate(entry)
            self._write_candidate_entry(entry=entry, score=decision.score, reasons=decision.reasons)
            if decision.keep:
                stats.candidates_selected += 1
        return stats

    def generate_candidate_manifest_from_cdxj(
        self,
        *,
        crawl: str,
        url_pattern: str | None = None,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        candidate_limit: int | None = None,
    ) -> ExtractionStats:
        if self.candidate_writer is None:
            raise ValueError("candidate_manifest is required for candidate generation")

        stats = ExtractionStats()
        for entry in self.query_cdxj_index(
            crawl=crawl,
            url_pattern=url_pattern,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
            limit=candidate_limit or 10,
        ):
            stats.candidates_seen += 1
            decision = self.selector.evaluate(entry)
            self._write_candidate_entry(entry=entry, score=decision.score, reasons=decision.reasons)
            if decision.keep:
                stats.candidates_selected += 1
        return stats

    def extract_candidate_manifest(
        self,
        manifest_path: str | Path,
        *,
        record_limit: int | None = None,
        write_output: bool = True,
    ) -> ExtractionStats:
        stats = ExtractionStats()
        with Path(manifest_path).open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = line.strip()
                if not payload:
                    continue
                entry = CCIndexEntry.from_dict(json.loads(payload))
                stats.candidates_seen += 1
                decision = self.selector.evaluate(entry)
                if not decision.keep:
                    continue
                stats.candidates_selected += 1
                result = self.extract_candidate(entry)
                if not result.kept or result.record is None:
                    stats.failures += 1
                    continue
                if write_output:
                    self.write_record(result.record)
                stats.records_built += 1
                if record_limit is not None and stats.records_built >= record_limit:
                    break
        return stats

    def run_columnar_extraction(
        self,
        *,
        crawl: str,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        path_limit: int | None = None,
        path_batch_size: int = 1,
        rows_per_batch: int | None = None,
        candidate_limit: int | None = None,
        record_limit: int | None = None,
        write_output: bool = True,
    ) -> ExtractionStats:
        stats = ExtractionStats()
        for entry in self.iter_columnar_candidates(
            crawl=crawl,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
            path_limit=path_limit,
            path_batch_size=path_batch_size,
            rows_per_batch=rows_per_batch,
            candidate_limit=candidate_limit,
        ):
            stats.candidates_seen += 1
            decision = self.selector.evaluate(entry)
            self._write_candidate_entry(entry=entry, score=decision.score, reasons=decision.reasons)
            if not decision.keep:
                continue
            stats.candidates_selected += 1
            result = self.extract_candidate(entry)
            if not result.kept or result.record is None:
                stats.failures += 1
                continue
            if write_output:
                self.write_record(result.record)
            stats.records_built += 1
            if record_limit is not None and stats.records_built >= record_limit:
                break
        return stats

    def run_cdxj_extraction(
        self,
        *,
        crawl: str,
        url_pattern: str | None = None,
        domain: str | None = None,
        host: str | None = None,
        path_prefix: str | None = None,
        url_like: str | None = None,
        candidate_limit: int | None = None,
        record_limit: int | None = None,
        write_output: bool = True,
    ) -> ExtractionStats:
        stats = ExtractionStats()
        for entry in self.query_cdxj_index(
            crawl=crawl,
            url_pattern=url_pattern,
            domain=domain,
            host=host,
            path_prefix=path_prefix,
            url_like=url_like,
            limit=candidate_limit or 10,
        ):
            stats.candidates_seen += 1
            decision = self.selector.evaluate(entry)
            self._write_candidate_entry(entry=entry, score=decision.score, reasons=decision.reasons)
            if not decision.keep:
                continue
            stats.candidates_selected += 1
            result = self.extract_candidate(entry)
            if not result.kept or result.record is None:
                stats.failures += 1
                continue
            if write_output:
                self.write_record(result.record)
            stats.records_built += 1
            if record_limit is not None and stats.records_built >= record_limit:
                break
        return stats

    def _write_candidate_manifest(
        self,
        *,
        record_id: str,
        source_url: str,
        selection_reason: str | None = None,
        score: float | None = None,
        candidate_reasons: list[str] | None = None,
        warc_path: str | None = None,
        warc_offset: int | None = None,
        warc_length: int | None = None,
    ) -> None:
        if self.candidate_writer is None:
            return
        self.candidate_writer.write(
            {
                "record_id": record_id,
                "source_url": source_url,
                "crawl_id": self.config.crawl_id,
                "score": score,
                "selection_reason": selection_reason,
                "candidate_reasons": candidate_reasons or [],
                "warc_path": warc_path,
                "warc_offset": warc_offset,
                "warc_length": warc_length,
                "selected_at": _utc_now(),
            }
        )

    def _write_candidate_entry(
        self,
        *,
        entry: CCIndexEntry,
        score: float,
        reasons: list[str],
    ) -> None:
        if self.candidate_writer is None:
            return
        payload = entry.to_dict()
        payload.update(
            {
                "crawl_id": self.config.crawl_id,
                "score": score,
                "candidate_reasons": reasons,
                "selected_at": _utc_now(),
            }
        )
        self.candidate_writer.write(payload)

    def _read_warc_record(self, filename: str, *, offset: int, length: int) -> WarcRecord:
        path = Path(filename)
        if path.exists():
            return self.warc_reader.read_record(path, offset=offset, length=length)
        return self.remote_warc_reader.read_record(filename, offset=offset, length=length)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

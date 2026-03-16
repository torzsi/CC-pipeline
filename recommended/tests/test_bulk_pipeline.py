import gzip
import json

from cc_pipeline.columnar import ColumnarQueryResult
from cc_pipeline.pipeline import PipelineConfig, PipelineRunner


def test_generate_candidate_manifest_from_columnar_writes_selected_rows(tmp_path) -> None:
    runner = PipelineRunner(
        PipelineConfig(
            output_jsonl=tmp_path / "noop.jsonl",
            candidate_manifest=tmp_path / "candidates.jsonl",
            crawl_id="CC-MAIN-2026-08",
        )
    )

    rows = [
        {
            "url": "https://example.com/article/1",
            "content_mime_detected": "text/html",
            "fetch_status": 200,
            "content_languages": "eng",
            "content_charset": "utf-8",
            "warc_filename": "crawl-data/CC-MAIN-2026-08/segments/1.warc.gz",
            "warc_record_offset": 0,
            "warc_record_length": 4096,
            "fetch_time": "2026-02-01T00:00:00Z",
        },
        {
            "url": "https://example.com/assets/site.css",
            "content_mime_detected": "text/css",
            "fetch_status": 200,
            "content_languages": "eng",
            "content_charset": "utf-8",
            "warc_filename": "crawl-data/CC-MAIN-2026-08/segments/2.warc.gz",
            "warc_record_offset": 0,
            "warc_record_length": 4096,
            "fetch_time": "2026-02-01T00:00:00Z",
        },
    ]

    def fake_batches(**_kwargs):
        yield ColumnarQueryResult(rows=rows, sql="select 1", parquet_paths=["a.parquet"])

    runner.columnar.iter_html_candidate_batches = fake_batches

    stats = runner.generate_candidate_manifest_from_columnar(
        crawl="CC-MAIN-2026-08",
        domain=None,
    )

    manifest_lines = (tmp_path / "candidates.jsonl").read_text(encoding="utf-8").strip().splitlines()
    first = json.loads(manifest_lines[0])
    second = json.loads(manifest_lines[1])

    assert stats.candidates_seen == 2
    assert stats.candidates_selected == 1
    assert first["url"] == "https://example.com/article/1"
    assert first["mime"] == "text/html"
    assert first["candidate_reasons"] == []
    assert second["mime"] == "text/css"
    assert "unsupported_mime" in second["candidate_reasons"]


def test_extract_candidate_manifest_builds_raw_records(tmp_path) -> None:
    payload = (
        b"WARC/1.0\r\n"
        b"WARC-Type: response\r\n"
        b"WARC-Target-URI: https://example.com/post\r\n"
        b"Content-Length: 210\r\n"
        b"\r\n"
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><p>This is a sufficiently long paragraph that should survive the default text quality filter for testing.</p>"
        b"<img src=\"https://example.com/image.jpg\" width=\"640\" height=\"480\" /></body></html>\r\n"
    )
    compressed = gzip.compress(payload)
    warc_path = tmp_path / "sample.warc.gz"
    warc_path.write_bytes(compressed)

    candidate_manifest = tmp_path / "candidates.jsonl"
    candidate_manifest.write_text(
        json.dumps(
                {
                    "url": "https://example.com/post",
                    "mime": "text/html",
                    "status": 200,
                    "length": 4096,
                    "filename": str(warc_path),
                    "offset": 0,
                    "languages": "eng",
                "timestamp": "2026-02-01T00:00:00Z",
                "charset": "utf-8",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    output_path = tmp_path / "raw-records.jsonl"
    runner = PipelineRunner(
        PipelineConfig(
            output_jsonl=output_path,
            document_manifest=tmp_path / "documents.jsonl",
            crawl_id="CC-MAIN-2026-08",
        )
    )

    stats = runner.extract_candidate_manifest(candidate_manifest, write_output=True)

    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert stats.candidates_seen == 1
    assert stats.candidates_selected == 1
    assert stats.records_built == 1
    assert payload["general_metadata"]["crawl_id"] == "CC-MAIN-2026-08"
    assert payload["general_metadata"]["warc_path"] == str(warc_path)
    assert payload["texts"][0].startswith("This is a sufficiently long paragraph")
    assert payload["image"][1].startswith("s3://local-bucket/images/")

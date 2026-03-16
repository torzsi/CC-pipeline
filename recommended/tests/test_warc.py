import gzip

from cc_pipeline.pipeline import PipelineConfig, PipelineRunner
from cc_pipeline.warc import LocalWarcReader, parse_warc_record


def test_parse_warc_record_extracts_headers_and_body() -> None:
    payload = (
        b"WARC/1.0\r\n"
        b"WARC-Type: response\r\n"
        b"WARC-Target-URI: https://example.com/post\r\n"
        b"Content-Length: 118\r\n"
        b"\r\n"
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><p>Hello world with enough content for parsing.</p></body></html>\r\n"
    )

    record = parse_warc_record(payload)

    assert record.warc_type == "response"
    assert record.target_uri == "https://example.com/post"
    assert record.content_type == "text/html"
    assert b"Hello world" in record.body


def test_local_warc_reader_reads_gzip_member(tmp_path) -> None:
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

    record = LocalWarcReader().read_record(warc_path, offset=0, length=len(compressed))

    assert record.content_type == "text/html"
    assert record.target_uri == "https://example.com/post"


def test_pipeline_processes_warc_record(tmp_path) -> None:
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

    runner = PipelineRunner(PipelineConfig(output_jsonl=tmp_path / "documents.jsonl"))
    record = runner.warc_reader.read_record(warc_path, offset=0, length=len(compressed))
    result = runner.process_warc_record(
        record,
        page_url="https://example.com/post",
        warc_path=str(warc_path),
        warc_offset=0,
        warc_length=len(compressed),
    )

    assert result.kept is True
    assert result.record is not None
    assert result.record.general_metadata.warc_path == str(warc_path)
    assert result.record.general_metadata.warc_offset == 0
    assert result.record.general_metadata.warc_length == len(compressed)

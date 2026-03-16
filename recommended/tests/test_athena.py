import json

from cc_pipeline.athena import AthenaColumnarClient, AthenaQueryRequest
from cc_pipeline.pipeline import PipelineConfig, PipelineRunner


def test_athena_build_candidate_sql_contains_recommended_filters() -> None:
    client = AthenaColumnarClient()
    request = AthenaQueryRequest(
        crawl="CC-MAIN-2025-43",
        output_location="s3://athena-results/cc/",
        domain="wikipedia.org",
        limit=25,
    )

    sql = client.build_candidate_sql(request)

    assert "FROM ccindex.ccindex" in sql
    assert "crawl = 'CC-MAIN-2025-43'" in sql
    assert "subset = 'warc'" in sql
    assert "fetch_status = 200" in sql
    assert "lower(content_mime_detected) IN ('text/html')" in sql
    assert "lower(url_host_registered_domain) = 'wikipedia.org'" in sql
    assert "LIMIT 25" in sql


def test_generate_candidate_manifest_from_athena_writes_entries(tmp_path) -> None:
    runner = PipelineRunner(
        PipelineConfig(
            output_jsonl=tmp_path / "noop.jsonl",
            candidate_manifest=tmp_path / "candidates.jsonl",
            crawl_id="CC-MAIN-2025-43",
        )
    )

    rows = [
        {
            "url": "https://example.com/post",
            "content_mime_detected": "text/html",
            "fetch_status": "200",
            "content_languages": "eng",
            "content_charset": "utf-8",
            "warc_filename": "s3://commoncrawl/crawl-data/CC-MAIN-2025-43/segments/test.warc.gz",
            "warc_record_offset": "100",
            "warc_record_length": "4096",
            "fetch_time": "2025-10-16T17:59:23Z",
        },
        {
            "url": "https://example.com/file.css",
            "content_mime_detected": "text/css",
            "fetch_status": "200",
            "content_languages": "eng",
            "content_charset": "utf-8",
            "warc_filename": "s3://commoncrawl/crawl-data/CC-MAIN-2025-43/segments/test2.warc.gz",
            "warc_record_offset": "200",
            "warc_record_length": "4096",
            "fetch_time": "2025-10-16T17:59:23Z",
        },
    ]

    class FakeResult:
        def __init__(self, payload):
            self.payload = payload

        def to_index_entries(self):
            return AthenaColumnarClient().plan_candidate_query  # type: ignore[return-value]

    class FakeAthenaResult:
        def __init__(self, payload):
            self.payload = payload

        def to_index_entries(self):
            from cc_pipeline.athena import AthenaQueryResult

            return AthenaQueryResult(rows=self.payload, sql="select 1").to_index_entries()

    runner.athena.execute_candidate_query = lambda _request: FakeAthenaResult(rows)  # type: ignore[method-assign]

    stats = runner.generate_candidate_manifest_from_athena(
        AthenaQueryRequest(
            crawl="CC-MAIN-2025-43",
            output_location="s3://athena-results/cc/",
        )
    )

    lines = (tmp_path / "candidates.jsonl").read_text(encoding="utf-8").strip().splitlines()
    first = json.loads(lines[0])
    second = json.loads(lines[1])

    assert stats.candidates_seen == 2
    assert stats.candidates_selected == 1
    assert first["url"] == "https://example.com/post"
    assert first["candidate_reasons"] == []
    assert second["url"] == "https://example.com/file.css"
    assert "unsupported_mime" in second["candidate_reasons"]

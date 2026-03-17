import json

from cc_pipeline.cdxj import CDXJQueryResult
from cc_pipeline.pipeline import PipelineConfig, PipelineRunner


def test_pipeline_writes_jsonl(tmp_path) -> None:
    html = """
    <html lang="en">
      <body>
        <article>
          <p>This is a sufficiently long paragraph that should survive the default text quality filter for testing.</p>
          <img src="https://example.com/hero.jpg" width="640" height="480" />
          <p>This is another sufficiently descriptive paragraph to complete the record.</p>
        </article>
      </body>
    </html>
    """
    output_path = tmp_path / "documents.jsonl"
    runner = PipelineRunner(PipelineConfig(output_jsonl=output_path))

    result = runner.process_html(html, page_url="https://example.com/post")

    assert result.kept is True
    lines = output_path.read_text(encoding="utf-8").strip().splitlines()
    payload = json.loads(lines[0])
    assert payload["texts"][0].startswith("This is a sufficiently long paragraph")
    assert payload["image"][1].startswith("s3://local-bucket/images/")
    assert payload["width"][1] == 640
    assert payload["height"][1] == 480

def test_pipeline_query_cdxj_index_uses_cdxj_client(tmp_path) -> None:
    runner = PipelineRunner(PipelineConfig(output_jsonl=tmp_path / "documents.jsonl"))

    def fake_find_records(**kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["crawl"] == "CC-MAIN-2026-08"
        assert kwargs["host"] == "en.wikipedia.org"
        assert kwargs["path_prefix"] == "/wiki/Cat"
        return CDXJQueryResult(
            rows=[
                {
                    "url": "https://en.wikipedia.org/wiki/Cat",
                    "mime-detected": "text/html",
                    "status": "200",
                    "filename": "crawl-data/sample.warc.gz",
                    "offset": "100",
                    "length": "2000",
                }
            ],
            query_url="https://index.commoncrawl.org/test",
        )

    runner.cdxj.find_records = fake_find_records  # type: ignore[method-assign]

    entries = runner.query_cdxj_index(
        crawl="CC-MAIN-2026-08",
        host="en.wikipedia.org",
        path_prefix="/wiki/Cat",
        limit=3,
    )

    assert len(entries) == 1
    assert entries[0].url == "https://en.wikipedia.org/wiki/Cat"

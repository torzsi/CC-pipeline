import json

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


def test_pipeline_drops_duplicate_documents(tmp_path) -> None:
    html = """
    <html>
      <body>
        <p>This is a sufficiently long paragraph that should survive the default text quality filter for testing.</p>
        <img src="https://example.com/hero.jpg" width="640" height="480" />
      </body>
    </html>
    """
    runner = PipelineRunner(PipelineConfig(output_jsonl=tmp_path / "documents.jsonl"))

    first = runner.process_html(html, page_url="https://example.com/post")
    second = runner.process_html(html, page_url="https://example.com/post-2")

    assert first.kept is True
    assert second.kept is False
    assert second.reasons == ["exact_text"]

from cc_pipeline.candidates import CCIndexEntry, CandidateSelector


def test_candidate_selector_accepts_viable_html_entry() -> None:
    selector = CandidateSelector()
    entry = CCIndexEntry(
        url="https://example.com/article/with-image",
        mime="text/html",
        status="200",
        length=8192,
        filename="crawl-data/CC-MAIN-2026-10/segments/1.warc.gz",
        offset=1024,
        languages="eng",
    )

    decision = selector.evaluate(entry)

    assert decision.keep is True
    assert decision.score > 0
    assert decision.reasons == []


def test_candidate_selector_rejects_non_html_asset() -> None:
    selector = CandidateSelector()
    entry = CCIndexEntry(
        url="https://example.com/assets/site.css",
        mime="text/css",
        status="200",
        length=2048,
        filename="crawl-data/CC-MAIN-2026-10/segments/1.warc.gz",
        offset=1024,
    )

    decision = selector.evaluate(entry)

    assert decision.keep is False
    assert "unsupported_mime" in decision.reasons

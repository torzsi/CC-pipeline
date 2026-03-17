from cc_pipeline.cdxj import CDXJIndexClient


def test_build_target_from_host_and_path_prefix() -> None:
    client = CDXJIndexClient()

    target = client.build_target(host="en.wikipedia.org", path_prefix="/wiki/Cat")

    assert target == "https://en.wikipedia.org/wiki/Cat*"


def test_build_target_from_url_like_converts_trailing_percent() -> None:
    client = CDXJIndexClient()

    target = client.build_target(url_like="https://en.wikipedia.org/wiki/Cat%")

    assert target == "https://en.wikipedia.org/wiki/Cat*"


def test_build_query_params_uses_prefix_match_for_wildcard() -> None:
    client = CDXJIndexClient()

    params = client.build_query_params(target="https://en.wikipedia.org/wiki/Cat*", limit=3)

    assert params["output"] == "json"
    assert params["matchType"] == "prefix"
    assert params["url"] == "https://en.wikipedia.org/wiki/Cat"
    assert params["limit"] == "3"


def test_query_result_to_index_entries_parses_cdxj_fields() -> None:
    payload = {
        "url": "https://en.wikipedia.org/wiki/Cat",
        "timestamp": "20260214023442",
        "status": "200",
        "mime": "text/html",
        "mime-detected": "text/html",
        "filename": "crawl-data/sample.warc.gz",
        "offset": "123",
        "length": "456",
        "languages": "eng",
        "digest": "ABC",
        "encoding": "UTF-8",
    }

    from cc_pipeline.cdxj import CDXJQueryResult

    entries = CDXJQueryResult(rows=[payload], query_url="https://example.com").to_index_entries()

    assert len(entries) == 1
    entry = entries[0]
    assert entry.url == "https://en.wikipedia.org/wiki/Cat"
    assert entry.mime == "text/html"
    assert entry.status == "200"
    assert entry.offset == 123
    assert entry.length == 456
    assert entry.filename == "crawl-data/sample.warc.gz"
    assert entry.charset == "UTF-8"

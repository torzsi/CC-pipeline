from cc_pipeline.columnar import ColumnarIndexClient


def test_build_html_where_sql_supports_targeted_filters() -> None:
    client = ColumnarIndexClient()

    where_sql = client.build_html_where_sql(
        domain="wikipedia.org",
        host="en.wikipedia.org",
        path_prefix="/wiki/Cat",
        url_like="https://en.wikipedia.org/wiki/Cat%",
    )

    assert "lower(url_host_registered_domain) = 'wikipedia.org'" in where_sql
    assert "lower(url_host_name) = 'en.wikipedia.org'" in where_sql
    assert "lower(url_path) LIKE '/wiki/cat%'" in where_sql
    assert "lower(url) LIKE 'https://en.wikipedia.org/wiki/cat%'" in where_sql


def test_find_html_candidates_passes_targeted_filters_to_query() -> None:
    client = ColumnarIndexClient()
    captured: dict[str, object] = {}

    def fake_query(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return object()

    client.query = fake_query  # type: ignore[method-assign]

    client.find_html_candidates(
        crawl="CC-MAIN-2025-43",
        domain="wikipedia.org",
        host="en.wikipedia.org",
        path_prefix="/wiki/Cat",
        url_like="https://en.wikipedia.org/wiki/Cat%",
        limit=7,
        path_limit=3,
    )

    assert captured["crawl"] == "CC-MAIN-2025-43"
    assert captured["limit"] == 7
    assert captured["path_limit"] == 3
    where_sql = str(captured["where_sql"])
    assert "lower(url_host_registered_domain) = 'wikipedia.org'" in where_sql
    assert "lower(url_host_name) = 'en.wikipedia.org'" in where_sql
    assert "lower(url_path) LIKE '/wiki/cat%'" in where_sql
    assert "lower(url) LIKE 'https://en.wikipedia.org/wiki/cat%'" in where_sql

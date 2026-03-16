from cc_pipeline.columnar import ColumnarIndexClient, ColumnarQueryResult


def test_manifest_url_uses_expected_commoncrawl_layout() -> None:
    client = ColumnarIndexClient()

    url = client.manifest_url("CC-MAIN-2025-51")

    assert url == "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-51/cc-index-table.paths.gz"


def test_columnar_result_maps_rows_to_index_entries() -> None:
    result = ColumnarQueryResult(
        rows=[
            {
                "url": "https://example.com/post",
                "content_mime_detected": "text/html",
                "status": 200,
                "content_languages": "eng",
                "content_charset": "utf-8",
                "warc_filename": "crawl-data/CC-MAIN-2025-51/segments/1.warc.gz",
                "warc_record_offset": 1234,
                "warc_record_length": 5678,
                "fetch_time": "2025-12-18T00:00:00Z",
            }
        ],
        sql="select 1",
        parquet_paths=["https://data.commoncrawl.org/example.parquet"],
    )

    entries = result.to_index_entries()

    assert entries[0].url == "https://example.com/post"
    assert entries[0].filename == "crawl-data/CC-MAIN-2025-51/segments/1.warc.gz"
    assert entries[0].offset == 1234
    assert entries[0].length == 5678

from cc_pipeline.schema import GeneralMetadata, InterleavedRecord


def test_interleaved_record_validation_accepts_aligned_slots() -> None:
    record = InterleavedRecord(
        texts=["intro", None, "caption"],
        image=[None, "s3://bucket/img.jpg", None],
        width=[None, 640, None],
        height=[None, 480, None],
        url=[None, "https://example.com/img.jpg", None],
        general_metadata=GeneralMetadata(
            source_url="https://example.com/page",
            canonical_url="https://example.com/page",
        ),
    )

    payload = record.to_dict()

    assert payload["image"][1] == "s3://bucket/img.jpg"
    assert payload["width"][1] == 640


def test_interleaved_record_validation_rejects_mixed_slot() -> None:
    record = InterleavedRecord(
        texts=["intro"],
        image=["s3://bucket/img.jpg"],
        width=[640],
        height=[480],
        url=["https://example.com/img.jpg"],
        general_metadata=GeneralMetadata(
            source_url="https://example.com/page",
            canonical_url="https://example.com/page",
        ),
    )

    try:
        record.to_dict()
    except ValueError as exc:
        assert "mixes text and image payloads" in str(exc)
    else:
        raise AssertionError("expected mixed slot validation error")

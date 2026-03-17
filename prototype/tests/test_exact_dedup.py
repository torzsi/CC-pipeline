from __future__ import annotations

import json
import hashlib

from cc_pipeline.cli import main
from cc_pipeline.dedup import ExactTextDeduplicator, canonicalize_url_for_dedup, stable_url_hash
from cc_pipeline.schema import GeneralMetadata, InterleavedRecord


def _record(source_url: str, text: str, *, image_ref: str, canonical_url: str | None = None) -> InterleavedRecord:
    return InterleavedRecord(
        texts=[text, None],
        image=[None, image_ref],
        width=[None, 640],
        height=[None, 480],
        url=[None, "https://example.com/image.jpg"],
        general_metadata=GeneralMetadata(
            source_url=source_url,
            canonical_url=canonical_url or source_url,
        ),
        meta={"slot_count": 2},
    )


def test_exact_text_deduplicator_clusters_formatted_records(tmp_path) -> None:
    shared_one = tmp_path / "shared-one.bin"
    shared_two = tmp_path / "shared-two.bin"
    unique_image = tmp_path / "unique.bin"
    shared_bytes = b"same-image-bytes"
    shared_one.write_bytes(shared_bytes)
    shared_two.write_bytes(shared_bytes)
    unique_image.write_bytes(b"different-image-bytes")

    first = _record(
        "https://example.com/a",
        "Cat facts are useful for testing exact dedup.",
        image_ref=str(shared_one),
        canonical_url="https://Example.com/cats?id=2&lang=en#section",
    )
    duplicate = _record(
        "https://example.com/b",
        "Cat facts are useful for testing exact dedup.",
        image_ref=str(shared_two),
        canonical_url="https://example.com/cats?lang=en&id=2",
    )
    unique = _record(
        "https://example.com/c",
        "Different content should form its own exact dedup cluster.",
        image_ref=str(unique_image),
    )

    result = ExactTextDeduplicator().run(
        [
            ("a", first),
            ("b", duplicate),
            ("c", unique),
        ]
    )

    assert result.stats.records_seen == 3
    assert result.stats.unique_records == 2
    assert result.stats.duplicate_records == 1
    assert result.stats.exact_clusters == 2

    cluster_sizes = sorted(cluster.exact_text_cluster_size for cluster in result.clusters)
    assert cluster_sizes == [1, 2]

    annotated = [record.to_dict() for record in result.unique_records]
    duplicate_cluster_record = next(
        payload for payload in annotated if payload["general_metadata"]["source_url"] == "https://example.com/a"
    )
    signatures = duplicate_cluster_record["general_metadata"]["dedup_signatures"]
    assert signatures["exact_text_cluster_size"] == 2
    assert signatures["exact_text_is_representative"] is True
    assert signatures["canonical_url"] == "https://example.com/cats?id=2&lang=en"
    assert signatures["source_capture_count"] == 2
    assert signatures["image_exact_hash_count"] == 1
    assert signatures["image_exact_hashes"][1] == hashlib.sha256(shared_bytes).hexdigest()


def test_exact_dedup_cli_writes_expected_outputs(tmp_path) -> None:
    input_path = tmp_path / "formatted.jsonl"
    unique_path = tmp_path / "unique.jsonl"
    duplicate_path = tmp_path / "duplicates.jsonl"
    cluster_path = tmp_path / "clusters.jsonl"
    shared_one = tmp_path / "shared-one.bin"
    shared_two = tmp_path / "shared-two.bin"
    unique_image = tmp_path / "unique.bin"
    shared_one.write_bytes(b"same-image-bytes")
    shared_two.write_bytes(b"same-image-bytes")
    unique_image.write_bytes(b"different-image-bytes")

    records = [
        _record(
            "https://example.com/a",
            "Repeated cat content for exact dedup.",
            image_ref=str(shared_one),
            canonical_url="https://Example.com/cats?id=2&lang=en#part",
        ),
        _record(
            "https://example.com/b",
            "Repeated cat content for exact dedup.",
            image_ref=str(shared_two),
            canonical_url="https://example.com/cats?lang=en&id=2",
        ),
        _record(
            "https://example.com/c",
            "A different cat record that should stay unique.",
            image_ref=str(unique_image),
        ),
    ]
    with input_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(record.to_json())
            handle.write("\n")

    exit_code = main(
        [
            "exact-dedup-jsonl",
            "--input-jsonl",
            str(input_path),
            "--unique-output-jsonl",
            str(unique_path),
            "--duplicate-manifest",
            str(duplicate_path),
            "--cluster-stats-jsonl",
            str(cluster_path),
        ]
    )

    assert exit_code == 0

    unique_rows = [json.loads(line) for line in unique_path.read_text(encoding="utf-8").splitlines()]
    duplicate_rows = [json.loads(line) for line in duplicate_path.read_text(encoding="utf-8").splitlines()]
    cluster_rows = [json.loads(line) for line in cluster_path.read_text(encoding="utf-8").splitlines()]

    assert len(unique_rows) == 2
    assert len(duplicate_rows) == 3
    assert len(cluster_rows) == 2

    repeated_cluster = next(row for row in cluster_rows if row["exact_text_cluster_size"] == 2)
    repeated_members = [row for row in duplicate_rows if row["exact_text_cluster_id"] == repeated_cluster["exact_text_cluster_id"]]
    assert len(repeated_members) == 2
    assert sum(1 for row in repeated_members if row["exact_text_is_representative"]) == 1
    assert all(row["source_capture_count"] == 2 for row in repeated_members)

    repeated_unique = next(
        row for row in unique_rows if row["general_metadata"]["source_url"] == "https://example.com/a"
    )
    dedup_signatures = repeated_unique["general_metadata"]["dedup_signatures"]
    assert dedup_signatures["canonical_url"] == canonicalize_url_for_dedup("https://Example.com/cats?id=2&lang=en#part")
    assert dedup_signatures["canonical_url_hash"] == stable_url_hash("https://example.com/cats?lang=en&id=2")
    assert dedup_signatures["image_exact_hashes"][1] == hashlib.sha256(b"same-image-bytes").hexdigest()

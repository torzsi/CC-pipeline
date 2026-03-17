from __future__ import annotations

from dataclasses import dataclass
import json
import hashlib
from pathlib import Path
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .manifests import JsonlWriter
from .schema import InterleavedRecord
from .text import stable_text_hash


def normalize_text_for_exact_dedup(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    return " ".join(normalized.split())


def record_text_for_exact_dedup(record: InterleavedRecord) -> str:
    return normalize_text_for_exact_dedup(" ".join(text for text in record.texts if text))


def canonicalize_url_for_dedup(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    port = parsed.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        netloc = hostname
    elif port is not None:
        netloc = f"{hostname}:{port}"
    else:
        netloc = hostname

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query = urlencode(sorted(query_pairs))
    return urlunparse((scheme, netloc, path, "", query, ""))


def stable_url_hash(url: str) -> str:
    return hashlib.sha256(canonicalize_url_for_dedup(url).encode("utf-8")).hexdigest()


def exact_image_hash(image_ref: str | None) -> str | None:
    if image_ref is None:
        return None

    if image_ref.startswith("file://"):
        path = Path(urlparse(image_ref).path)
    else:
        parsed = urlparse(image_ref)
        if parsed.scheme not in ("", "file"):
            return None
        path = Path(image_ref)

    if not path.exists() or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@dataclass
class ExactTextDedupMembership:
    record_id: str
    source_url: str
    canonical_url: str
    canonical_url_hash: str
    source_capture_count: int
    exact_text_hash: str
    exact_text_cluster_id: str
    exact_text_cluster_size: int
    exact_text_is_representative: bool
    normalization_version: str = "exact_text_v1"

    def to_dict(self) -> dict[str, object]:
        return {
            "record_id": self.record_id,
            "source_url": self.source_url,
            "canonical_url": self.canonical_url,
            "canonical_url_hash": self.canonical_url_hash,
            "source_capture_count": self.source_capture_count,
            "exact_text_hash": self.exact_text_hash,
            "exact_text_cluster_id": self.exact_text_cluster_id,
            "exact_text_cluster_size": self.exact_text_cluster_size,
            "exact_text_is_representative": self.exact_text_is_representative,
            "normalization_version": self.normalization_version,
        }


@dataclass
class ExactTextDedupCluster:
    exact_text_hash: str
    exact_text_cluster_id: str
    exact_text_cluster_size: int
    representative_record_id: str
    representative_source_url: str
    normalization_version: str = "exact_text_v1"

    def to_dict(self) -> dict[str, object]:
        return {
            "exact_text_hash": self.exact_text_hash,
            "exact_text_cluster_id": self.exact_text_cluster_id,
            "exact_text_cluster_size": self.exact_text_cluster_size,
            "representative_record_id": self.representative_record_id,
            "representative_source_url": self.representative_source_url,
            "normalization_version": self.normalization_version,
        }


@dataclass
class ExactDedupStats:
    records_seen: int = 0
    unique_records: int = 0
    duplicate_records: int = 0
    exact_clusters: int = 0


@dataclass
class ExactTextDedupResult:
    stats: ExactDedupStats
    memberships: list[ExactTextDedupMembership]
    clusters: list[ExactTextDedupCluster]
    unique_records: list[InterleavedRecord]


class ExactTextDeduplicator:
    normalization_version = "exact_text_v1"

    def run(self, records: list[tuple[str, InterleavedRecord]]) -> ExactTextDedupResult:
        clusters_by_hash: dict[str, list[tuple[str, InterleavedRecord]]] = {}
        source_capture_counts: dict[str, int] = {}
        for record_id, record in records:
            text_hash = stable_text_hash(record_text_for_exact_dedup(record))
            clusters_by_hash.setdefault(text_hash, []).append((record_id, record))
            canonical_url_hash = stable_url_hash(record.general_metadata.canonical_url)
            source_capture_counts[canonical_url_hash] = source_capture_counts.get(canonical_url_hash, 0) + 1

        memberships: list[ExactTextDedupMembership] = []
        clusters: list[ExactTextDedupCluster] = []
        unique_records: list[InterleavedRecord] = []

        for text_hash, grouped_records in clusters_by_hash.items():
            cluster_id = f"exact-text-{text_hash[:16]}"
            cluster_size = len(grouped_records)
            representative_id, representative_record = grouped_records[0]

            clusters.append(
                ExactTextDedupCluster(
                    exact_text_hash=text_hash,
                    exact_text_cluster_id=cluster_id,
                    exact_text_cluster_size=cluster_size,
                    representative_record_id=representative_id,
                    representative_source_url=representative_record.general_metadata.source_url,
                    normalization_version=self.normalization_version,
                )
            )

            annotated_record = self._annotate_record(
                representative_record,
                exact_text_hash=text_hash,
                cluster_id=cluster_id,
                cluster_size=cluster_size,
                is_representative=True,
                canonical_url_hash=stable_url_hash(representative_record.general_metadata.canonical_url),
                source_capture_count=source_capture_counts[
                    stable_url_hash(representative_record.general_metadata.canonical_url)
                ],
            )
            unique_records.append(annotated_record)

            for record_id, record in grouped_records:
                canonical_url = canonicalize_url_for_dedup(record.general_metadata.canonical_url)
                canonical_url_hash = stable_url_hash(record.general_metadata.canonical_url)
                memberships.append(
                    ExactTextDedupMembership(
                        record_id=record_id,
                        source_url=record.general_metadata.source_url,
                        canonical_url=canonical_url,
                        canonical_url_hash=canonical_url_hash,
                        source_capture_count=source_capture_counts[canonical_url_hash],
                        exact_text_hash=text_hash,
                        exact_text_cluster_id=cluster_id,
                        exact_text_cluster_size=cluster_size,
                        exact_text_is_representative=record_id == representative_id,
                        normalization_version=self.normalization_version,
                    )
                )

        memberships.sort(key=lambda item: (item.exact_text_cluster_id, item.record_id))
        clusters.sort(key=lambda item: item.exact_text_cluster_id)
        unique_records.sort(key=lambda item: item.general_metadata.source_url)

        stats = ExactDedupStats(
            records_seen=len(records),
            unique_records=len(unique_records),
            duplicate_records=len(records) - len(unique_records),
            exact_clusters=len(clusters),
        )
        return ExactTextDedupResult(
            stats=stats,
            memberships=memberships,
            clusters=clusters,
            unique_records=unique_records,
        )

    def run_jsonl(
        self,
        *,
        input_jsonl: str | Path,
        unique_output_jsonl: str | Path,
        duplicate_manifest: str | Path,
        cluster_stats_jsonl: str | Path,
    ) -> ExactDedupStats:
        records: list[tuple[str, InterleavedRecord]] = []
        with Path(input_jsonl).open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                payload = json.loads(line)
                record = InterleavedRecord.from_dict(payload)
                record_id = self._record_id_for_payload(payload, line_number=line_number)
                records.append((record_id, record))

        result = self.run(records)
        unique_writer = JsonlWriter(unique_output_jsonl)
        duplicate_writer = JsonlWriter(duplicate_manifest)
        cluster_writer = JsonlWriter(cluster_stats_jsonl)

        for record in result.unique_records:
            unique_writer.write(record.to_dict())
        for membership in result.memberships:
            duplicate_writer.write(membership.to_dict())
        for cluster in result.clusters:
            cluster_writer.write(cluster.to_dict())

        return result.stats

    def _annotate_record(
        self,
        record: InterleavedRecord,
        *,
        exact_text_hash: str,
        cluster_id: str,
        cluster_size: int,
        is_representative: bool,
        canonical_url_hash: str,
        source_capture_count: int,
    ) -> InterleavedRecord:
        image_exact_hashes = [exact_image_hash(image_ref) for image_ref in record.image]
        signatures = dict(record.general_metadata.dedup_signatures)
        signatures.update(
            {
                "normalization_version": self.normalization_version,
                "exact_text_hash": exact_text_hash,
                "exact_text_cluster_id": cluster_id,
                "exact_text_cluster_size": cluster_size,
                "exact_text_is_representative": is_representative,
                "canonical_url": canonicalize_url_for_dedup(record.general_metadata.canonical_url),
                "canonical_url_hash": canonical_url_hash,
                "source_capture_count": source_capture_count,
                "image_exact_hashes": image_exact_hashes,
                "image_exact_hash_count": sum(1 for value in image_exact_hashes if value is not None),
            }
        )
        record.general_metadata.dedup_signatures = signatures
        return record

    @staticmethod
    def _record_id_for_payload(payload: dict[str, object], *, line_number: int) -> str:
        metadata = payload.get("general_metadata")
        if isinstance(metadata, dict):
            source_url = metadata.get("source_url")
            if isinstance(source_url, str) and source_url:
                return source_url.rstrip("/").rsplit("/", 1)[-1] or f"line-{line_number}"
        return f"line-{line_number}"

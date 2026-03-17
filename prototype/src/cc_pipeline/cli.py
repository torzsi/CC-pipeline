from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .dedup import ExactTextDeduplicator
from .pipeline import PipelineConfig, PipelineRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process Common Crawl interleaved text-image records")
    subparsers = parser.add_subparsers(dest="command", required=True)

    html_parser = subparsers.add_parser("local-html", help="Process a local HTML file")
    html_parser.add_argument("--input-html", required=True, help="Path to a local HTML file")
    html_parser.add_argument("--page-url", required=True, help="Source page URL used for canonicalization")
    html_parser.add_argument("--output-jsonl", required=True, help="Output JSONL file path")
    html_parser.add_argument("--candidate-manifest", help="Optional candidate manifest path")
    html_parser.add_argument("--document-manifest", help="Optional document manifest path")
    html_parser.add_argument("--crawl-id", default="local-dev", help="Crawl identifier to stamp into metadata")

    columnar_parser = subparsers.add_parser("query-columnar", help="Query the Common Crawl columnar index")
    columnar_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    columnar_parser.add_argument("--domain", help="Optional registered domain to query, e.g. commoncrawl.org")
    columnar_parser.add_argument("--host", help="Optional exact host filter, e.g. en.wikipedia.org")
    columnar_parser.add_argument("--path-prefix", help="Optional URL path prefix filter, e.g. /wiki/Cat")
    columnar_parser.add_argument("--url-like", help="Optional SQL LIKE pattern on the full URL")
    columnar_parser.add_argument("--limit", type=int, default=5, help="Maximum number of rows to return")
    columnar_parser.add_argument("--path-limit", type=int, help="Optional cap on parquet files scanned")

    cdxj_parser = subparsers.add_parser("query-cdxj", help="Query the Common Crawl CDXJ index")
    cdxj_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    cdxj_parser.add_argument("--url-pattern", help="Explicit CDXJ URL pattern, e.g. https://en.wikipedia.org/wiki/Cat*")
    cdxj_parser.add_argument("--domain", help="Optional registered domain fallback for target construction")
    cdxj_parser.add_argument("--host", help="Optional exact host filter, e.g. en.wikipedia.org")
    cdxj_parser.add_argument("--path-prefix", help="Optional URL path prefix filter, e.g. /wiki/Cat")
    cdxj_parser.add_argument("--url-like", help="Optional SQL-LIKE-style URL prefix, e.g. https://en.wikipedia.org/wiki/Cat%")
    cdxj_parser.add_argument("--limit", type=int, default=5, help="Maximum number of rows to return")

    generate_parser = subparsers.add_parser(
        "generate-candidates",
        help="Generate a candidate manifest from Common Crawl columnar index shards",
    )
    generate_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    generate_parser.add_argument("--candidate-manifest", required=True, help="Output candidate manifest JSONL path")
    generate_parser.add_argument("--domain", help="Optional registered domain to query, e.g. commoncrawl.org")
    generate_parser.add_argument("--host", help="Optional exact host filter, e.g. en.wikipedia.org")
    generate_parser.add_argument("--path-prefix", help="Optional URL path prefix filter, e.g. /wiki/Cat")
    generate_parser.add_argument("--url-like", help="Optional SQL LIKE pattern on the full URL")
    generate_parser.add_argument("--path-limit", type=int, help="Optional cap on parquet files scanned")
    generate_parser.add_argument("--path-batch-size", type=int, default=1, help="Parquet files to scan per query")
    generate_parser.add_argument("--rows-per-batch", type=int, help="Optional row cap per parquet batch query")
    generate_parser.add_argument("--candidate-limit", type=int, help="Optional total candidate cap")

    generate_cdxj_parser = subparsers.add_parser(
        "generate-candidates-cdxj",
        help="Generate a candidate manifest from the Common Crawl CDXJ index",
    )
    generate_cdxj_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    generate_cdxj_parser.add_argument("--candidate-manifest", required=True, help="Output candidate manifest JSONL path")
    generate_cdxj_parser.add_argument("--url-pattern", help="Explicit CDXJ URL pattern, e.g. https://en.wikipedia.org/wiki/Cat*")
    generate_cdxj_parser.add_argument("--domain", help="Optional registered domain fallback for target construction")
    generate_cdxj_parser.add_argument("--host", help="Optional exact host filter, e.g. en.wikipedia.org")
    generate_cdxj_parser.add_argument("--path-prefix", help="Optional URL path prefix filter, e.g. /wiki/Cat")
    generate_cdxj_parser.add_argument("--url-like", help="Optional SQL-LIKE-style URL prefix")
    generate_cdxj_parser.add_argument("--candidate-limit", type=int, help="Optional total candidate cap")

    extract_parser = subparsers.add_parser(
        "extract-candidates",
        help="Build raw interleaved records from a candidate manifest without filtering or dedup",
    )
    extract_parser.add_argument("--candidate-manifest", required=True, help="Input candidate manifest JSONL path")
    extract_parser.add_argument("--output-jsonl", required=True, help="Output JSONL for extracted raw records")
    extract_parser.add_argument("--document-manifest", help="Optional extracted document manifest path")
    extract_parser.add_argument("--crawl-id", default="local-dev", help="Crawl identifier to stamp into metadata")
    extract_parser.add_argument("--record-limit", type=int, help="Optional total record cap")

    exact_dedup_parser = subparsers.add_parser(
        "exact-dedup-jsonl",
        help="Run exact-text dedup over already-formatted JSONL records",
    )
    exact_dedup_parser.add_argument("--input-jsonl", required=True, help="Input formatted JSONL records")
    exact_dedup_parser.add_argument("--unique-output-jsonl", required=True, help="Output JSONL for unique records")
    exact_dedup_parser.add_argument("--duplicate-manifest", required=True, help="Output JSONL for duplicate membership")
    exact_dedup_parser.add_argument("--cluster-stats-jsonl", required=True, help="Output JSONL for exact cluster stats")

    run_parser = subparsers.add_parser(
        "run-columnar-extraction",
        help="Combined candidate generation plus raw extraction for a Common Crawl slice",
    )
    run_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    run_parser.add_argument("--output-jsonl", required=True, help="Output JSONL for extracted raw records")
    run_parser.add_argument("--candidate-manifest", help="Optional candidate manifest JSONL path")
    run_parser.add_argument("--document-manifest", help="Optional extracted document manifest path")
    run_parser.add_argument("--domain", help="Optional registered domain to query, e.g. commoncrawl.org")
    run_parser.add_argument("--host", help="Optional exact host filter, e.g. en.wikipedia.org")
    run_parser.add_argument("--path-prefix", help="Optional URL path prefix filter, e.g. /wiki/Cat")
    run_parser.add_argument("--url-like", help="Optional SQL LIKE pattern on the full URL")
    run_parser.add_argument("--path-limit", type=int, help="Optional cap on parquet files scanned")
    run_parser.add_argument("--path-batch-size", type=int, default=1, help="Parquet files to scan per query")
    run_parser.add_argument("--rows-per-batch", type=int, help="Optional row cap per parquet batch query")
    run_parser.add_argument("--candidate-limit", type=int, help="Optional total candidate cap")
    run_parser.add_argument("--record-limit", type=int, help="Optional total extracted record cap")

    run_cdxj_parser = subparsers.add_parser(
        "run-cdxj-extraction",
        help="Combined CDXJ candidate generation plus raw extraction for a targeted Common Crawl search",
    )
    run_cdxj_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    run_cdxj_parser.add_argument("--output-jsonl", required=True, help="Output JSONL for extracted raw records")
    run_cdxj_parser.add_argument("--candidate-manifest", help="Optional candidate manifest JSONL path")
    run_cdxj_parser.add_argument("--document-manifest", help="Optional extracted document manifest path")
    run_cdxj_parser.add_argument("--url-pattern", help="Explicit CDXJ URL pattern, e.g. https://en.wikipedia.org/wiki/Cat*")
    run_cdxj_parser.add_argument("--domain", help="Optional registered domain fallback for target construction")
    run_cdxj_parser.add_argument("--host", help="Optional exact host filter, e.g. en.wikipedia.org")
    run_cdxj_parser.add_argument("--path-prefix", help="Optional URL path prefix filter, e.g. /wiki/Cat")
    run_cdxj_parser.add_argument("--url-like", help="Optional SQL-LIKE-style URL prefix")
    run_cdxj_parser.add_argument("--candidate-limit", type=int, help="Optional total candidate cap")
    run_cdxj_parser.add_argument("--record-limit", type=int, help="Optional total extracted record cap")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "local-html":
        html = Path(args.input_html).read_text(encoding="utf-8")
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=args.output_jsonl,
                candidate_manifest=args.candidate_manifest,
                document_manifest=args.document_manifest,
                crawl_id=args.crawl_id,
            )
        )
        result = runner.process_html(html, page_url=args.page_url)
        if not result.kept:
            print(f"dropped: {','.join(result.reasons)}", file=sys.stderr)
            return 1
        print("kept")
        return 0

    if args.command == "query-columnar":
        runner = PipelineRunner(PipelineConfig(output_jsonl=Path("out") / "noop.jsonl"))
        entries = runner.query_columnar_index(
            crawl=args.crawl,
            domain=args.domain,
            host=args.host,
            path_prefix=args.path_prefix,
            url_like=args.url_like,
            limit=args.limit,
            path_limit=args.path_limit,
        )
        print(json.dumps([entry.__dict__ for entry in entries], ensure_ascii=True, indent=2))
        return 0

    if args.command == "query-cdxj":
        runner = PipelineRunner(PipelineConfig(output_jsonl=Path("out") / "noop.jsonl"))
        entries = runner.query_cdxj_index(
            crawl=args.crawl,
            url_pattern=args.url_pattern,
            domain=args.domain,
            host=args.host,
            path_prefix=args.path_prefix,
            url_like=args.url_like,
            limit=args.limit,
        )
        print(json.dumps([entry.__dict__ for entry in entries], ensure_ascii=True, indent=2))
        return 0

    if args.command == "generate-candidates":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=Path("out") / "noop.jsonl",
                candidate_manifest=args.candidate_manifest,
                crawl_id=args.crawl,
            )
        )
        stats = runner.generate_candidate_manifest_from_columnar(
            crawl=args.crawl,
            domain=args.domain,
            host=args.host,
            path_prefix=args.path_prefix,
            url_like=args.url_like,
            path_limit=args.path_limit,
            path_batch_size=args.path_batch_size,
            rows_per_batch=args.rows_per_batch,
            candidate_limit=args.candidate_limit,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "generate-candidates-cdxj":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=Path("out") / "noop.jsonl",
                candidate_manifest=args.candidate_manifest,
                crawl_id=args.crawl,
            )
        )
        stats = runner.generate_candidate_manifest_from_cdxj(
            crawl=args.crawl,
            url_pattern=args.url_pattern,
            domain=args.domain,
            host=args.host,
            path_prefix=args.path_prefix,
            url_like=args.url_like,
            candidate_limit=args.candidate_limit,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "extract-candidates":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=args.output_jsonl,
                document_manifest=args.document_manifest,
                crawl_id=args.crawl_id,
            )
        )
        stats = runner.extract_candidate_manifest(
            args.candidate_manifest,
            record_limit=args.record_limit,
            write_output=True,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "exact-dedup-jsonl":
        stats = ExactTextDeduplicator().run_jsonl(
            input_jsonl=args.input_jsonl,
            unique_output_jsonl=args.unique_output_jsonl,
            duplicate_manifest=args.duplicate_manifest,
            cluster_stats_jsonl=args.cluster_stats_jsonl,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "run-columnar-extraction":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=args.output_jsonl,
                candidate_manifest=args.candidate_manifest,
                document_manifest=args.document_manifest,
                crawl_id=args.crawl,
            )
        )
        stats = runner.run_columnar_extraction(
            crawl=args.crawl,
            domain=args.domain,
            host=args.host,
            path_prefix=args.path_prefix,
            url_like=args.url_like,
            path_limit=args.path_limit,
            path_batch_size=args.path_batch_size,
            rows_per_batch=args.rows_per_batch,
            candidate_limit=args.candidate_limit,
            record_limit=args.record_limit,
            write_output=True,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "run-cdxj-extraction":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=args.output_jsonl,
                candidate_manifest=args.candidate_manifest,
                document_manifest=args.document_manifest,
                crawl_id=args.crawl,
            )
        )
        stats = runner.run_cdxj_extraction(
            crawl=args.crawl,
            url_pattern=args.url_pattern,
            domain=args.domain,
            host=args.host,
            path_prefix=args.path_prefix,
            url_like=args.url_like,
            candidate_limit=args.candidate_limit,
            record_limit=args.record_limit,
            write_output=True,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

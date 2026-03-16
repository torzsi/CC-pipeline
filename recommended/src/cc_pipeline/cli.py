from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .athena import AthenaQueryRequest
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
    columnar_parser.add_argument("--limit", type=int, default=5, help="Maximum number of rows to return")
    columnar_parser.add_argument("--path-limit", type=int, help="Optional cap on parquet files scanned")

    generate_parser = subparsers.add_parser(
        "generate-candidates",
        help="Generate a candidate manifest from Common Crawl columnar index shards",
    )
    generate_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    generate_parser.add_argument("--candidate-manifest", required=True, help="Output candidate manifest JSONL path")
    generate_parser.add_argument("--domain", help="Optional registered domain to query, e.g. commoncrawl.org")
    generate_parser.add_argument("--path-limit", type=int, help="Optional cap on parquet files scanned")
    generate_parser.add_argument("--path-batch-size", type=int, default=1, help="Parquet files to scan per query")
    generate_parser.add_argument("--rows-per-batch", type=int, help="Optional row cap per parquet batch query")
    generate_parser.add_argument("--candidate-limit", type=int, help="Optional total candidate cap")

    extract_parser = subparsers.add_parser(
        "extract-candidates",
        help="Build raw interleaved records from a candidate manifest without filtering or dedup",
    )
    extract_parser.add_argument("--candidate-manifest", required=True, help="Input candidate manifest JSONL path")
    extract_parser.add_argument("--output-jsonl", required=True, help="Output JSONL for extracted raw records")
    extract_parser.add_argument("--document-manifest", help="Optional extracted document manifest path")
    extract_parser.add_argument("--crawl-id", default="local-dev", help="Crawl identifier to stamp into metadata")
    extract_parser.add_argument("--record-limit", type=int, help="Optional total record cap")

    run_parser = subparsers.add_parser(
        "run-columnar-extraction",
        help="Combined candidate generation plus raw extraction for a Common Crawl slice",
    )
    run_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    run_parser.add_argument("--output-jsonl", required=True, help="Output JSONL for extracted raw records")
    run_parser.add_argument("--candidate-manifest", help="Optional candidate manifest JSONL path")
    run_parser.add_argument("--document-manifest", help="Optional extracted document manifest path")
    run_parser.add_argument("--domain", help="Optional registered domain to query, e.g. commoncrawl.org")
    run_parser.add_argument("--path-limit", type=int, help="Optional cap on parquet files scanned")
    run_parser.add_argument("--path-batch-size", type=int, default=1, help="Parquet files to scan per query")
    run_parser.add_argument("--rows-per-batch", type=int, help="Optional row cap per parquet batch query")
    run_parser.add_argument("--candidate-limit", type=int, help="Optional total candidate cap")
    run_parser.add_argument("--record-limit", type=int, help="Optional total extracted record cap")

    athena_plan_parser = subparsers.add_parser(
        "athena-plan-stage-a",
        help="Render the recommended Athena SQL for bulk candidate generation",
    )
    athena_plan_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    athena_plan_parser.add_argument("--output-location", required=True, help="Athena query output S3 prefix")
    athena_plan_parser.add_argument("--domain", help="Optional registered domain filter")
    athena_plan_parser.add_argument("--table", default="ccindex.ccindex", help="Athena table reference")
    athena_plan_parser.add_argument("--database", help="Optional Athena database")
    athena_plan_parser.add_argument("--workgroup", default="primary", help="Athena workgroup")
    athena_plan_parser.add_argument("--limit", type=int, help="Optional SQL LIMIT for small proofs")
    athena_plan_parser.add_argument("--include-pdf", action="store_true", help="Include PDF candidates")

    athena_generate_parser = subparsers.add_parser(
        "athena-generate-candidates",
        help="Run Stage A candidate generation through Athena and write a manifest",
    )
    athena_generate_parser.add_argument("--crawl", required=True, help="Crawl ID, e.g. CC-MAIN-2025-51")
    athena_generate_parser.add_argument("--candidate-manifest", required=True, help="Output candidate manifest JSONL path")
    athena_generate_parser.add_argument("--output-location", required=True, help="Athena query output S3 prefix")
    athena_generate_parser.add_argument("--domain", help="Optional registered domain filter")
    athena_generate_parser.add_argument("--table", default="ccindex.ccindex", help="Athena table reference")
    athena_generate_parser.add_argument("--database", help="Optional Athena database")
    athena_generate_parser.add_argument("--workgroup", default="primary", help="Athena workgroup")
    athena_generate_parser.add_argument("--limit", type=int, help="Optional SQL LIMIT for small proofs")
    athena_generate_parser.add_argument("--include-pdf", action="store_true", help="Include PDF candidates")

    stage_b_parser = subparsers.add_parser(
        "stage-b-extract",
        help="Run the recommended Stage B extraction from a candidate manifest",
    )
    stage_b_parser.add_argument("--candidate-manifest", required=True, help="Input candidate manifest JSONL path")
    stage_b_parser.add_argument("--output-jsonl", required=True, help="Output JSONL for extracted raw records")
    stage_b_parser.add_argument("--document-manifest", help="Optional extracted document manifest path")
    stage_b_parser.add_argument("--crawl-id", required=True, help="Crawl identifier to stamp into metadata")
    stage_b_parser.add_argument("--record-limit", type=int, help="Optional total record cap")
    stage_b_parser.add_argument(
        "--prefer-s3-warc-reads",
        action="store_true",
        help="Interpret Common Crawl WARC paths as s3://commoncrawl/... instead of HTTPS",
    )
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
            limit=args.limit,
            path_limit=args.path_limit,
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
            path_limit=args.path_limit,
            path_batch_size=args.path_batch_size,
            rows_per_batch=args.rows_per_batch,
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
            path_limit=args.path_limit,
            path_batch_size=args.path_batch_size,
            rows_per_batch=args.rows_per_batch,
            candidate_limit=args.candidate_limit,
            record_limit=args.record_limit,
            write_output=True,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "athena-plan-stage-a":
        runner = PipelineRunner(PipelineConfig(output_jsonl=Path("out") / "noop.jsonl", crawl_id=args.crawl))
        request = AthenaQueryRequest(
            crawl=args.crawl,
            output_location=args.output_location,
            table=args.table,
            domain=args.domain,
            limit=args.limit,
            include_pdf=args.include_pdf,
            workgroup=args.workgroup,
            database=args.database,
        )
        plan = runner.athena.plan_candidate_query(request)
        print(
            json.dumps(
                {
                    "sql": plan.sql,
                    "output_location": plan.output_location,
                    "workgroup": plan.workgroup,
                    "database": plan.database,
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        return 0

    if args.command == "athena-generate-candidates":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=Path("out") / "noop.jsonl",
                candidate_manifest=args.candidate_manifest,
                crawl_id=args.crawl,
            )
        )
        request = AthenaQueryRequest(
            crawl=args.crawl,
            output_location=args.output_location,
            table=args.table,
            domain=args.domain,
            limit=args.limit,
            include_pdf=args.include_pdf,
            workgroup=args.workgroup,
            database=args.database,
        )
        stats = runner.generate_candidate_manifest_from_athena(request)
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    if args.command == "stage-b-extract":
        runner = PipelineRunner(
            PipelineConfig(
                output_jsonl=args.output_jsonl,
                document_manifest=args.document_manifest,
                crawl_id=args.crawl_id,
                prefer_s3_warc_reads=args.prefer_s3_warc_reads,
            )
        )
        stats = runner.extract_candidate_manifest(
            args.candidate_manifest,
            record_limit=args.record_limit,
            write_output=True,
        )
        print(json.dumps(stats.__dict__, ensure_ascii=True, indent=2))
        return 0

    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

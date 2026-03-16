# CC Pipeline Recommended

This project is the AWS-oriented version of the Common Crawl pipeline. It keeps the extraction core from the prototype, but changes the orchestration model to match the large-scale access pattern Common Crawl recommends.

## Target Architecture

- Stage A: bulk candidate generation from the Columnar Index
- Stage B: manifest-driven WARC extraction
- runtime in AWS `us-east-1`
- Athena or Spark for Stage A
- S3-based WARC access for Stage B

The project is kept separate from the prototype so the local fallback path remains available.

## What Is Implemented

- canonical multimodal schema
- HTML interleaved extraction
- Athena SQL planning and execution scaffolding
- candidate manifest generation
- local, HTTPS, and S3-oriented WARC readers
- Stage B extraction from manifests into raw canonical records
- tests for the Athena and S3-oriented code path

## What Is Not Proven Yet

This project is ready for repository use, but live AWS execution still depends on external infrastructure:

- AWS credentials
- Athena access to the Common Crawl columnar table
- an S3 bucket for Athena query outputs
- compute in `us-east-1`

## Project Layout

- `src/cc_pipeline/`: AWS-oriented pipeline code
- `tests/`: tests for Athena planning, manifest generation, and S3 WARC handling

## Installation

Local development:

```bash
python3 -m pip install -e .[dev]
```

AWS-oriented development:

```bash
python3 -m pip install -e .[dev,aws]
```

## Example Workflow

Render the Stage A Athena SQL:

```bash
PYTHONPATH=src python3 -m cc_pipeline.cli athena-plan-stage-a \
  --crawl CC-MAIN-2025-43 \
  --output-location s3://my-athena-results/cc-pipeline/ \
  --domain wikipedia.org \
  --limit 100
```

Generate a candidate manifest:

```bash
PYTHONPATH=src python3 -m cc_pipeline.cli athena-generate-candidates \
  --crawl CC-MAIN-2025-43 \
  --candidate-manifest out/candidates.jsonl \
  --output-location s3://my-athena-results/cc-pipeline/ \
  --domain wikipedia.org \
  --limit 100
```

Run Stage B extraction:

```bash
PYTHONPATH=src python3 -m cc_pipeline.cli stage-b-extract \
  --candidate-manifest out/candidates.jsonl \
  --output-jsonl out/raw.jsonl \
  --document-manifest out/documents.jsonl \
  --crawl-id CC-MAIN-2025-43 \
  --prefer-s3-warc-reads
```

## When To Use This Project

Use this project when:

- the target architecture is AWS-based
- candidate generation should happen in Athena or Spark
- extraction workers should read WARC content from S3

Use the prototype project instead when:

- AWS access is unavailable
- data is already fetched locally
- you want to iterate on extraction quality or downstream data logic first

## Tests

```bash
python3 -m pytest
```

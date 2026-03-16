# CC Pipeline Monorepo

This repository contains two related Common Crawl multimodal data pipeline projects:

- `prototype/`: the original local-first pipeline for iterating on extraction, filtering, deduplication, and export logic when data is already available locally
- `recommended/`: the AWS-oriented pipeline that follows the access pattern Common Crawl recommends for large-scale production runs

Both projects target the same core problem: turning Common Crawl documents into aligned interleaved text-image records for multimodal model training.

## Repository Structure

```text
cc-pipeline-monorepo/
├── prototype/
│   ├── src/
│   ├── tests/
│   ├── docs/
│   ├── pyproject.toml
│   └── README.md
├── recommended/
│   ├── src/
│   ├── tests/
│   ├── pyproject.toml
│   └── README.md
└── .gitignore
```

## Which Project To Use

Use `prototype/` when:

- you want to work locally
- you want to assume WARC or HTML data has already been fetched
- you want to improve extraction quality before production infrastructure is ready

Use `recommended/` when:

- you want an architecture aligned with Common Crawl's large-scale usage guidance
- you plan to run candidate generation with Athena or Spark
- you plan to run extraction workers in AWS `us-east-1`

## Shared Canonical Record Format

Both projects emit document-level records with aligned slots:

```json
{
  "texts": ["paragraph", null, "caption", null],
  "image": [null, "s3://bucket/images/img1.jpg", null, "s3://bucket/images/img2.jpg"],
  "width": [null, 640, null, 1200],
  "height": [null, 480, null, 800],
  "url": [null, "https://example.com/a.jpg", null, "https://example.com/b.jpg"],
  "general_metadata": {
    "source_url": "https://example.com/article",
    "crawl_id": "CC-MAIN-2025-43"
  },
  "data_name": "commoncrawl_interleaved",
  "meta": {
    "slot_count": 4
  }
}
```

## Quick Start

Prototype:

```bash
cd prototype
python3 -m pip install -e .[dev]
python3 -m pytest
```

Recommended:

```bash
cd recommended
python3 -m pip install -e .[dev]
python3 -m pytest
```

## Status

- `prototype/` is operational for local parsing, HTML extraction, canonical record building, basic filtering, and basic text deduplication
- `recommended/` is structured for Athena plus S3-based execution, but still requires AWS credentials and runtime infrastructure for live end-to-end runs

## Publishing To GitHub

This repository is initialized as a local Git repository. To publish it:

```bash
cd /Users/hanyu.zhou/cc-pipeline-monorepo
git add .
git commit -m "Initial import of Common Crawl pipeline projects"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

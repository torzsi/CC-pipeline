# Common Crawl Investigation and Dedup Roadmap

## Purpose

This note is for collaborators who are not yet familiar with Common Crawl or our current `CC_pipeline` prototype. It summarizes:

- what Common Crawl provides
- how we can access raw data and indexes
- how our current pipeline constructs interleaved multimodal records
- whether targeted supplementation is feasible
- what deduplication work is still required

The focus is the HTML/PDF-to-interleaved-document pipeline for multimodal pretraining.

## 1. What Common Crawl Provides

Common Crawl publishes several related datasets:

- `WARC`: raw crawl records, including HTTP responses and crawl metadata
- `WAT`: computed metadata derived from WARC, especially useful for links and some extracted metadata
- `WET`: extracted plaintext from crawled pages
- `CDXJ Index`: URL index optimized for looking up individual captures
- `Columnar Index`: Parquet index optimized for analytical and bulk filtering
- `Web Graph`: graph over hosts/domains and links
- `Host Index`: per-host per-crawl summary data, including statuses and some crawl statistics

For our use case:

- `WARC` is the source of truth for HTML response bodies
- `Columnar Index` is the best primary index for bulk candidate selection
- `CDXJ` is useful for debugging or targeted per-URL lookups
- `WET` is useful for large-scale text analysis or keyword/topic discovery
- `Host Index` and `Web Graph` are useful for understanding crawl composition at the host/domain level

## 2. How Common Crawl Can Be Accessed

### 2.1 Raw data access

Common Crawl data is hosted on AWS in `us-east-1`. It can be accessed:

- over HTTPS from anywhere, e.g. `https://data.commoncrawl.org/...`
- via S3 paths such as `s3://commoncrawl/...`

Important practical point:

- HTTPS is fine for testing and downloading whole files
- range-request-heavy workflows are much less stable over HTTPS
- for large-scale production access, inside-AWS S3 access is better

### 2.2 Index access

There are two main index paths:

1. `CDXJ`
- good for looking up specific URLs or small targeted queries
- returns `filename`, `offset`, `length` for a capture

2. `Columnar Index`
- Parquet on S3
- better for bulk filtering and aggregation
- queryable with Athena, Spark, DuckDB, Polars, Arrow, etc.

For our pipeline:

- bulk candidate generation should be `Columnar Index` first
- `CDXJ` should remain a secondary tool for debugging and spot checks

## 3. How We Interacted with Common Crawl So Far

### 3.1 What we already proved in `CC_pipeline`

The original project at [`/Users/hanyu.zhou/CC_pipeline`](/Users/hanyu.zhou/CC_pipeline) already supports:

- candidate objects from index metadata
- WARC record parsing
- HTML interleaved extraction
- canonical record construction into aligned `texts` / `image` / `width` / `height` / `url`

Relevant code:

- candidate model: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/candidates.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/candidates.py)
- columnar query client: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/columnar.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/columnar.py)
- WARC reader/parser: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/warc.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/warc.py)
- HTML extractor: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/extractors/html.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/extractors/html.py)
- record schema: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/schema.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/schema.py)
- pipeline orchestration: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/pipeline.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/pipeline.py)

We also successfully fetched and parsed real Common Crawl records during testing. The main bottleneck was not correctness; it was the stability and speed of large-scale remote access.

### 3.2 What failed operationally

The unstable part was:

- local DuckDB queries over remote Columnar Index parquet files via HTTPS
- followed by many partial HTTP range requests against WARC files

This is slow because:

- the parquet files are large
- small `LIMIT`s do not imply tiny reads
- remote parquet scans from a laptop have high latency and low throughput
- HTTPS partial downloads are exactly the access pattern Common Crawl warns about

## 4. Recommended vs Fallback Access Pattern

### Recommended production path

For large-scale work, the practical path is:

1. use `Columnar Index` for bulk candidate selection
2. query it in AWS `us-east-1`
3. treat the job as two stages:
   - Stage A: index query -> candidate manifest
   - Stage B: candidate manifest -> WARC extraction
4. read WARC data from `s3://commoncrawl/...` inside AWS

### Fallback/dev path

For local development, the practical path is:

1. assume data or WARC pointers are already available
2. process local WARC/HTML/PDF inputs
3. validate extraction, filtering, dedup, and export logic without depending on CC network access

This is the path we should use now until AWS setup becomes available.

## 5. How We Construct Interleaved Multimodal Data

The current HTML path is:

1. start from a WARC response or local HTML
2. parse the HTML DOM
3. walk the DOM in document order
4. extract block text and image-bearing tags
5. resolve image URLs, including some lazy-load variants
6. emit aligned multimodal slots
7. build the canonical record

The canonical record shape is:

```json
{
  "texts": ["text A", null, "text B", null],
  "image": [null, "s3://.../img1.jpg", null, "s3://.../img2.jpg"],
  "width": [null, 640, null, 800],
  "height": [null, 480, null, 600],
  "url": [null, "https://...", null, "https://..."],
  "general_metadata": {
    "source_url": "...",
    "canonical_url": "...",
    "crawl_id": "...",
    "warc_path": "...",
    "warc_offset": 0,
    "warc_length": 0
  }
}
```

Current implementation details:

- DOM-native ordering is the primary source of interleaving truth
- text slots occupy `texts`
- image slots occupy `image`, `width`, `height`, `url`
- widths and heights currently come from extracted metadata, not validated fetched image bytes

Current limitations:

- boilerplate removal is weak
- PDF extraction is not implemented
- actual image bytes are not yet fetched and validated in the local path

## 6. Feasibility of Targeted Supplementation

### Short answer

Yes, targeted supplementation is feasible, but only for some definitions of "targeted".

### 6.1 What is easy

The following are feasible directly from Common Crawl metadata or indexes:

- website/domain targeting
- URL-pattern targeting
- language targeting, where the index provides language metadata
- crawl/time targeting
- MIME-type targeting

Examples:

- all pages from `catster.com`
- all pages whose URL pattern looks like `/wiki/Cat`
- all English HTML pages from one crawl

This is because the Columnar Index contains fields such as:

- URL
- registered domain / host
- MIME
- status
- crawl partition
- WARC pointer
- some language/charset metadata

### 6.2 What is possible, but requires extra processing

The following are not directly available as a first-class Common Crawl index feature:

- "all cat pages"
- crawl topic composition
- keyword inventory for a crawl
- semantic categories such as animals, medicine, sports, etc.

To get these, we need an additional content-discovery layer. The feasible options are:

1. `WET`-based text search
- use extracted plaintext to build keyword or inverted indexes
- good for topic/keyword discovery
- weak for image-specific content and weak for HTML structure

2. `WARC`/HTML content scan
- parse page text from HTML directly
- more expensive, but preserves richer context

3. `Host Index` and `Web Graph`
- useful for understanding which hosts/domains are present and how prominent they are
- not enough alone for semantic topic coverage

4. local or distributed content classifiers
- keyword matchers
- topic classifiers
- entity matchers
- CLIP/SigLIP-like retrieval over images and text

### 6.3 What this means in practice for "cat supplementation"

If the dataset lacks the entity `cat`, a realistic supplementation strategy is:

1. use host/domain priors
- e.g. Wikipedia, pet sites, image-heavy animal sites

2. use URL/title/text keyword filters
- `cat`, `cats`, `kitten`, breed names, etc.

3. parse candidate pages into interleaved records

4. score them with text and image relevance models
- text-side entity filter
- image-side cat detector or CLIP retrieval

5. merge them into the corpus

So the answer is:

- yes, targeted supplementation is feasible
- no, Common Crawl does not directly provide a clean "topic inventory" for a crawl
- we need to build our own content-index or classifier layer on top of CC

## 7. Can We Know the Composition of One Crawl?

### What we can know relatively easily

- how many captures, domains, hosts, MIME types, languages, and statuses exist
- which hosts are prominent
- how one crawl differs from another at host/domain level

Useful CC resources:

- `Columnar Index` for page-level aggregated analysis
- `Host Index` for host-level composition
- `Web Graph` for host/domain linkage and importance signals

### What we cannot know directly

- exact topical composition of the crawl
- exact keyword coverage across all content
- exact entity coverage of the final multimodal training set

Those require secondary indexing or analysis over page text and images.

## 8. Deduplication: Why It Is Urgent

Dedup is currently the most urgent pipeline gap because:

- raw web corpora are highly repetitive
- duplicates hurt training efficiency
- duplicates increase memorization risk
- duplicates distort long-tail entity frequencies
- duplicates can leak benchmark/test content into training

For multimodal interleaved data, dedup is harder than pure text because duplication appears at multiple levels:

- same URL
- same canonical page under multiple URLs
- mirrored or templated HTML across domains
- same image under different URLs
- resized/re-encoded near-duplicate images
- same document with slightly edited text but identical images

## 9. What We Have Today in `CC_pipeline`

Current implementation:

- file: [`/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/dedup.py`](/Users/hanyu.zhou/CC_pipeline/src/cc_pipeline/dedup.py)
- exact text dedup using a normalized text hash
- near-text dedup using in-memory Jaccard similarity against prior texts

This is enough only for very small local tests.

Main limitations:

- no URL dedup
- no image dedup
- no document-level multimodal dedup
- no persistent signatures
- no scalable approximate nearest-neighbor or LSH index
- current near-text check is quadratic in the number of seen documents

## 10. Recommended Dedup Design

Dedup should be split into layers. Earlier layers should be cheap and high-precision; later layers can be more expensive.

### Layer 1: URL and canonical URL dedup

Goal:

- catch exact duplicate pages before heavier processing

Recommended signatures:

- normalized source URL
- normalized canonical URL
- optional redirect-normalized final URL

Keep:

- one canonical representative

### Layer 2: Exact text dedup

Goal:

- remove byte-for-byte or normalized-text duplicates

Recommended signature:

- normalized document text hash

Notes:

- normalize whitespace
- normalize boilerplate if possible
- consider title plus main-content text, not full noisy DOM text

### Layer 3: Near-text dedup

Goal:

- remove mirrored or lightly edited copies

Recommended methods:

- MinHash over token shingles
- LSH buckets for scalable retrieval

This should replace the current all-pairs Jaccard loop.

### Layer 4: Exact image dedup

Goal:

- remove repeated images across URLs or pages

Recommended signatures:

- original image URL hash
- fetched image bytes hash

Notes:

- URL hash is cheap but weak
- bytes hash is stronger once image bytes are available

### Layer 5: Near-image dedup

Goal:

- catch resized, recompressed, or slightly edited copies

Recommended methods:

- perceptual hash (`pHash`, `dHash`, `aHash`) for cheap screening
- embedding-based nearest-neighbor search for stronger recall if needed

### Layer 6: Document-level multimodal dedup

Goal:

- catch documents that are not text-identical but are effectively the same multimodal example

Recommended signatures:

- near-text cluster ID
- sorted multiset of exact image hashes
- optionally page title and domain priors

Example rule:

- if two records have very similar text and strongly overlapping image sets, merge them

### Layer 7: Train/eval contamination dedup

Goal:

- prevent overlap with held-out benchmarks or internal evaluation sets

Recommended methods:

- exact substring matching against eval corpora
- near-text matching against eval prompts/documents
- image hash matching against eval images where relevant

## 11. Recommended Dedup Order in Our Pipeline

Assuming we ignore fetching for now and work on local extracted records, the practical order is:

1. URL canonical dedup
2. exact text dedup
3. near-text dedup with MinHash/LSH
4. exact image dedup once image bytes are available locally
5. near-image dedup
6. document-level multimodal dedup

This order keeps costs low and lets us ship usable improvements early.

## 12. Immediate Implementation Plan for Dedup

### Phase 1: replace the current text-only toy dedup

Implement in `CC_pipeline`:

- persistent dedup signature object stored in `general_metadata.dedup_signatures`
- URL normalization and exact URL dedup
- exact text hash dedup
- MinHash-based near-text dedup scaffold

Output:

- per-record dedup metadata
- explicit duplicate reason codes

### Phase 2: image-aware dedup

After local image availability is in place:

- exact image URL hash
- exact image bytes hash
- within-document image dedup
- cross-document image repetition limits

### Phase 3: multimodal document dedup

- combine text cluster signals with image-set overlap
- keep one representative document
- retain provenance of removed duplicates

## 13. Practical Conclusions

- Common Crawl is a feasible source for large-scale interleaved multimodal data.
- The best bulk index is the `Columnar Index`.
- `CDXJ` is still useful for narrow lookups and debugging.
- Targeted supplementation is feasible, but topic/entity targeting requires an extra content-index or classifier layer.
- For our current local-first development path, dedup should be the next major engineering focus.
- The current `dedup.py` is only a prototype and should be replaced with a layered, persistent, image-aware design.

## Sources

- Common Crawl Get Started: https://commoncrawl.org/get-started
- Common Crawl Columnar Index: https://commoncrawl.org/columnar-index
- Common Crawl CDXJ Index: https://commoncrawl.org/cdxj-index
- Common Crawl Index Server: https://index.commoncrawl.org/
- Common Crawl Infrastructure Status: https://status.commoncrawl.org/
- Common Crawl Host Index announcement: https://commoncrawl.org/blog/introducing-the-host-index
- Common Crawl index-table repo: https://github.com/commoncrawl/cc-index-table
- Common Crawl cc-pyspark repo: https://github.com/commoncrawl/cc-pyspark
- Lee et al., Deduplicating Training Data Makes Language Models Better: https://aclanthology.org/2022.acl-long.577/
- Google deduplicate-text-datasets repo: https://github.com/google-research/deduplicate-text-datasets
- MM1 paper: https://arxiv.org/pdf/2403.09611
- MINT-1T paper: https://proceedings.neurips.cc/paper_files/paper/2024/file/40b9196c25fe1d64d87ca3a80a91d0ce-Paper-Datasets_and_Benchmarks_Track.pdf

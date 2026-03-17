# Dedup Investigation And Design

## Goal

Design a deduplication stage for the Common Crawl multimodal pipeline that is:

- stable enough to support dataset statistics and labels
- flexible enough to support later sampling and mixture decisions
- compatible with interleaved text-image documents, not only plain text

The key conclusion is:

- `exact dedup` should be treated as stable dataset metadata
- `fuzzy dedup` should be treated as a configurable selection policy

Those two layers should not be merged.

## Why Dedup Matters

Dedup is not only about removing waste. It changes:

- compute cost
- tokenization cost
- label computation cost
- training distribution
- mixture weights across domains and quality buckets

For our pipeline, dedup must therefore answer two different questions:

1. What is factually duplicated?
2. What should be removed for a given training run?

The first question should produce persistent metadata.
The second question should remain configurable.

## Mentor Notes, Interpreted

The mentor feedback can be summarized as:

- Exact duplicates are measurable and stable.
- Fuzzy duplicates depend on the algorithm and thresholds, so they are not a single permanent truth.
- Repetition on the web may carry weak positive quality or importance signals.
- Removing duplicates across multiple curated sources may unintentionally downweight the most consistently high-quality documents.

This implies that:

- we should compute exact duplicate statistics early
- we should preserve duplicate counts instead of discarding them
- we should separate dedup metadata from final sampling decisions

## What Popular Methods Do

## 1. NeMo Curator

NeMo Curator explicitly separates:

- exact dedup
- fuzzy dedup
- semantic dedup

Its fuzzy dedup pipeline is the standard large-scale pattern:

- normalize text
- extract character n-grams
- compute MinHash signatures
- use LSH to propose candidate duplicate pairs
- optionally verify candidates
- build connected components
- keep one representative per component

This is a good reference design for our fuzzy stage.

## 2. DataTrove

DataTrove exposes dedup as a pipeline stage and includes examples for:

- MinHash deduplication
- sentence deduplication
- exact substring deduplication

The main lesson is engineering structure rather than one specific algorithm:

- shardable execution
- resumable stages
- explicit intermediate artifacts
- dedup as an independent stage, not hidden inside filtering

## 3. Google Deduplicate Text Datasets

Google showed that web-scale corpora contain severe exact and substring duplication, and that removing duplicated training data can improve evaluation behavior. The relevant lesson for us is that document-level exact dedup is necessary but not sufficient: many duplicates are partial copies, templates, or long repeated passages.

## 4. FineWeb 2

FineWeb 2 keeps cluster-size metadata after dedup and later supports rehydrating the original distribution. This matches the mentor's point exactly: dedup and training-time reweighting should be separate decisions.

## 5. DCLM / Zyphra

Open descriptions of DCLM-style dedup show a practical fuzzy setup:

- character n-grams
- MinHash signatures
- LSH banding
- connected components
- one kept representative per component

This confirms that the mainstream fuzzy dedup recipe is not exotic. The important part is careful parameterization and metadata retention.

## Design Principles For Our Pipeline

## 1. Separate Stable Facts From Experimental Policy

We should divide dedup into two layers:

- `stable dedup facts`
- `experimental dedup decisions`

Stable facts are things that should always recompute to the same result after normalization, such as exact hash clusters.

Experimental decisions are things that depend on a chosen fuzzy algorithm and threshold, such as whether two near-duplicate pages should collapse into one training example.

## 2. Preserve Duplicate Statistics

For exact duplicates, we should always preserve:

- normalized text hash
- exact cluster id
- exact cluster size
- representative record id

Even if we keep only unique documents for downstream heavy processing, we should retain enough metadata to:

- restore the original frequency distribution later
- measure how repeated each document was
- analyze duplication by domain, language, or quality bucket

## 3. Treat Fuzzy Dedup As Versioned Policy

Fuzzy dedup should always carry:

- normalization version
- tokenization or n-gram definition
- MinHash parameters
- threshold or similarity criterion
- connected-component policy

In practice, fuzzy dedup outputs should be tied to a config version, not treated as timeless truth.

## 4. Keep Document-, Text-, And Image-Level Views Separate

Our final corpus is interleaved multimodal documents. That means dedup has to exist at multiple levels:

- text exact duplicates
- text near duplicates
- image exact duplicates
- image near duplicates
- multimodal document duplicates

We should not force these into one single score too early.

## Our Proposed Dedup Architecture

## Stage 0. Canonical Normalization

Before any dedup, define a stable normalization pipeline for text:

- Unicode normalization
- whitespace normalization
- boilerplate-safe joining of text slots
- optional case normalization for exact-text dedup

For images, define:

- content hash on fetched bytes for exact dedup
- metadata normalization for URL-based analysis

The normalization version must be stored.

## Stage 1. Exact Text Dedup

Input:

- canonical raw records after extraction

Method:

- join text slots into a normalized document text
- compute stable hash
- group records by hash

Outputs:

- unique record table
- exact duplicate mapping table
- exact cluster statistics

Persistent metadata to save per record:

- `exact_text_hash`
- `exact_text_cluster_id`
- `exact_text_cluster_size`
- `exact_text_is_representative`

This stage should happen early, before expensive downstream processing such as tokenization and labeling.

## Stage 2. Exact URL And Source Dedup

Method:

- canonicalize source URL
- hash canonical URL
- track repeated captures of the same page

Purpose:

- distinguish exact textual duplication from repeated source captures
- support crawl-level accounting

Persistent metadata:

- `canonical_url`
- `canonical_url_hash`
- `source_capture_count`

## Stage 3. Fuzzy Text Dedup

Method:

- character or token n-grams
- MinHash
- LSH candidate generation
- optional pairwise verification
- connected components

Outputs:

- fuzzy duplicate graph or pair table
- fuzzy cluster id
- fuzzy cluster size
- representative record id
- run config id

Recommended rule:

- compute fuzzy clusters
- do not hard-delete them immediately
- let the training-data selection stage decide whether to:
  - keep one representative
  - partially preserve frequency
  - preserve more duplicates in some domains or quality buckets

## Stage 4. Image Dedup

Exact image dedup:

- content hash on actual image bytes

Near-image dedup:

- perceptual hash or embedding-based similarity

This should be designed separately from text dedup because:

- the same image may appear in many different documents
- some repeated images are semantically useful
- repeated logos, icons, and site chrome should usually be handled by filtering, not only dedup

## Stage 5. Multimodal Document Dedup

Once text and image dedup metadata exist, define a document-level multimodal signature from:

- normalized text hash or fuzzy cluster id
- set or multiset of image exact hashes
- basic layout statistics such as slot count

This stage should target:

- mirrored pages
- template-heavy copies
- pages with identical text and mostly identical image sets

It should come after exact text dedup and image exact dedup.

## Stage 6. Training-Time Reweighting

This is not dedup itself. It is the decision layer.

Given exact and fuzzy dedup metadata, the training mixer can decide whether to:

- train only on exact-unique documents
- restore original frequency by cluster size
- partially restore duplicates in high-quality buckets
- keep more duplicates in scarce domains or languages

This is where the mentor's concern matters most. We should avoid baking irreversible policy into the earliest dedup stages.

## Data Model We Should Persist

At minimum, each record should eventually carry:

- `normalization_version`
- `exact_text_hash`
- `exact_text_cluster_id`
- `exact_text_cluster_size`
- `canonical_url_hash`
- `source_capture_count`
- `fuzzy_text_run_id`
- `fuzzy_text_cluster_id`
- `fuzzy_text_cluster_size`
- `image_exact_hashes`
- `multimodal_signature_version`

Separate side tables are also useful:

- exact duplicate membership table
- fuzzy duplicate membership table
- representative selection table
- cluster statistics table

## What We Should Implement First

## Phase 1. Exact Dedup Foundation

Implement first:

- stable text normalization
- exact text hash
- exact duplicate cluster statistics
- duplicate mapping manifest

This gives us stable metrics and reduces repeated downstream compute.

## Phase 2. Exact URL And Image Hashes

Implement next:

- canonical URL dedup signals
- exact image hash storage for fetched images

## Phase 3. Fuzzy Text Dedup

Implement after exact dedup is stable:

- MinHash + LSH
- connected components
- config-versioned outputs

Do not combine this immediately with deletion.

## Phase 4. Multimodal Document Dedup

Once image bytes and image hashes are reliable, add multimodal document signatures.

## What We Should Not Do

- Do not collapse exact and fuzzy dedup into one score.
- Do not delete duplicate-count information after exact dedup.
- Do not treat one fuzzy threshold as permanent truth.
- Do not decide final sampling policy inside the first dedup stage.

## How This Maps To The Current Prototype

Today the prototype only has a very small in-memory text deduplicator in [`src/cc_pipeline/dedup.py`](../src/cc_pipeline/dedup.py). It supports:

- exact text hash
- simple near-text Jaccard comparison

It does not yet support:

- persistent exact cluster metadata
- side manifests for duplicate membership
- URL dedup
- image dedup
- MinHash/LSH fuzzy dedup
- multimodal document dedup
- distribution rehydration metadata

So the next implementation target should be:

1. replace the current in-memory exact dedup with persistent exact cluster accounting
2. emit exact dedup metadata into record manifests
3. add a dedicated fuzzy dedup stage later instead of expanding the current ad hoc near-text check

## Summary

Our dedup design should follow one rule:

- compute exact duplicates as stable facts
- compute fuzzy duplicates as configurable policy

That gives us:

- reproducible dataset statistics
- cheaper downstream processing
- room to preserve or restore useful repetition later
- a clean path from text-only dedup to multimodal dedup

## References

- NeMo Curator dedup concepts: https://docs.nvidia.com/nemo/curator/latest/about/concepts/deduplication.html
- NeMo Curator exact dedup: https://docs.nvidia.com/nemo/curator/latest/curate-text/process-data/deduplication/exact.html
- NeMo Curator fuzzy dedup: https://docs.nvidia.com/nemo/curator/25.09/curate-text/process-data/deduplication/fuzzy.html
- DataTrove: https://github.com/huggingface/datatrove
- FineWeb 2: https://github.com/huggingface/fineweb-2
- Google deduplicate-text-datasets: https://github.com/google-research/deduplicate-text-datasets
- Google publication page: https://research.google/pubs/deduplicating-training-data-makes-language-models-better/
- Zyphra dclm-dedup: https://huggingface.co/datasets/Zyphra/dclm-dedup
- Zyda-2 note: https://www.zyphra.com/post/building-zyda-2

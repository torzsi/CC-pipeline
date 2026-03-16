from __future__ import annotations

import hashlib
import re


_SPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    return _SPACE_RE.sub(" ", text).strip()


def stable_text_hash(text: str) -> str:
    return hashlib.sha256(normalize_text(text).lower().encode("utf-8")).hexdigest()


def shingled_tokens(text: str, *, size: int = 3) -> set[tuple[str, ...]]:
    tokens = normalize_text(text).lower().split()
    if len(tokens) < size:
        return {tuple(tokens)} if tokens else set()
    return {tuple(tokens[idx : idx + size]) for idx in range(0, len(tokens) - size + 1)}


def jaccard_similarity(left: str, right: str) -> float:
    left_shingles = shingled_tokens(left)
    right_shingles = shingled_tokens(right)
    if not left_shingles and not right_shingles:
        return 1.0
    if not left_shingles or not right_shingles:
        return 0.0
    return len(left_shingles & right_shingles) / len(left_shingles | right_shingles)

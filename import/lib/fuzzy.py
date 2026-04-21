"""String normalization + fuzzy matching for entity resolution.

Inspired by cpp-server-dispatch-service-module/helper/fuzzy_matcher: remove
accents, uppercase, strip punctuation/separators, then score similarity with
a token-sort ratio so "TOUCHET MAXIME" matches "Maxime Touchet".

Uses difflib.SequenceMatcher (stdlib) instead of rapidfuzz to avoid a new dep.
"""

from __future__ import annotations

import re
import unicodedata
from difflib import SequenceMatcher


_PUNCT_RE = re.compile(r"[./()\\\-_,;:+*'\"]+")
_SPACE_RE = re.compile(r"\s+")


def normalize(s: str | None) -> str:
    """Upper + ASCII + punctuation-stripped, whitespace-collapsed."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.upper()
    s = _PUNCT_RE.sub(" ", s)
    s = _SPACE_RE.sub(" ", s).strip()
    return s


def _tokens_sorted(s: str) -> str:
    return " ".join(sorted(normalize(s).split()))


def similarity(a: str, b: str) -> float:
    """Token-sort ratio in [0, 1]. Order-independent."""
    ta = _tokens_sorted(a)
    tb = _tokens_sorted(b)
    if not ta or not tb:
        return 0.0
    return SequenceMatcher(None, ta, tb).ratio()


def best_match(
    query: str,
    candidates: list[tuple[str, object]],
    threshold: float = 0.85,
) -> tuple[object, float, str] | None:
    """Return (payload, score, matched_name) for the best candidate above threshold,
    or None. `candidates` is a list of (name, payload) tuples."""
    best: tuple[object, float, str] | None = None
    for name, payload in candidates:
        score = similarity(query, name)
        if score >= threshold and (best is None or score > best[1]):
            best = (payload, score, name)
    return best

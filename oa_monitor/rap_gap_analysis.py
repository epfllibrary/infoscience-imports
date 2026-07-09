"""Gap analysis: Read & Publish (R&P) tracking vs Infoscience OA status.

Joins the consolidated R&P tracking table (rap_consolidator.consolidate_rap_tracking)
against the OA Enricher's Infoscience-harvested + classified table ("enriched" —
see oa_enricher.apply_classification) to verify, for each article EPFL Library
committed to cover under a Read & Publish agreement, that it is:
  1. deposited in Infoscience, and
  2. open access with the published or accepted version attached (not just
     metadata-only, and not merely a preprint).

Matching is by normalized DOI. Rows without a DOI (still in the R&P
notification pipeline, not yet published) cannot be verified and are flagged
separately (GAP_NO_DOI) rather than conflated with a genuine "missing from
Infoscience" gap (GAP_NOT_IN_INFOSCIENCE).
"""

from __future__ import annotations

import pandas as pd

from oa_monitor.oa_enricher import normalize_doi
from utils import get_pipeline_logger

logger = get_pipeline_logger("rap_gap_analysis")

# Infoscience's own access level / resolved version must both indicate a real,
# citable open copy — matches the "Green" signal used in oa_classifier.classify_oa.
_OPEN_ACCESS_LEVELS = frozenset({"open access", "openaccess"})
_FULLTEXT_VERSIONS = frozenset({"publishedVersion", "acceptedVersion"})

GAP_NO_DOI = "no_doi"
GAP_NOT_IN_INFOSCIENCE = "not_in_infoscience"
GAP_OPEN_WITH_FULLTEXT = "open_with_fulltext"
GAP_NEEDS_ATTENTION = "needs_attention"

# Infoscience-side columns carried into the joined output, when present.
_ENRICHED_CONTEXT_COLUMNS = [
    "infoscience_uuid", "handle", "existing_access_level",
    "resolved_version", "oa_category_basic", "oa_category_advanced",
]


def analyze_rap_gaps(rap_df: pd.DataFrame, enriched_df: pd.DataFrame) -> pd.DataFrame:
    """Join R&P tracking rows against Infoscience-enriched items by DOI.

    Returns a copy of rap_df with Infoscience match context columns, a boolean
    `in_infoscience`, and a `gap_status` classifying each row into GAP_NO_DOI /
    GAP_NOT_IN_INFOSCIENCE / GAP_OPEN_WITH_FULLTEXT / GAP_NEEDS_ATTENTION.
    """
    enriched = enriched_df.copy()
    enriched["doi_norm"] = (
        enriched["doi"].map(normalize_doi) if "doi" in enriched.columns else pd.NA
    )

    # Only rows with a real DOI can ever serve as a join key. pandas.merge()
    # treats NaN == NaN as an equal key (unlike SQL NULL semantics) — keeping
    # DOI-less enriched rows here would let them match every rap_tracking row
    # that also lacks a DOI, exploding into a cross product instead of the
    # "unverifiable" rows they actually are. DOI-less rap rows are handled
    # separately below, without going through the merge at all.
    enriched = enriched[enriched["doi_norm"].notna()]

    dup_mask = enriched.duplicated("doi_norm", keep="first")
    if dup_mask.any():
        logger.warning(
            "  %d duplicate DOI rows in enriched data — keeping first occurrence",
            int(dup_mask.sum()),
        )
    enriched = enriched[~dup_mask]

    context_cols = [c for c in _ENRICHED_CONTEXT_COLUMNS if c in enriched.columns]
    enriched_slim = enriched[["doi_norm"] + context_cols]

    has_doi = rap_df["doi_norm"].notna()

    with_doi = rap_df[has_doi].merge(enriched_slim, on="doi_norm", how="left", indicator="_merge")
    with_doi["in_infoscience"] = with_doi["_merge"] == "both"
    with_doi = with_doi.drop(columns=["_merge"])

    without_doi = rap_df[~has_doi].copy()
    without_doi["in_infoscience"] = False
    for col in context_cols:
        without_doi[col] = pd.NA

    result = pd.concat([with_doi, without_doi]).sort_index().reset_index(drop=True)

    result["gap_status"] = result.apply(_classify_gap, axis=1)

    if len(result):
        counts = result["gap_status"].value_counts()
        logger.info(
            "  R&P gap analysis: %d rows — %s", len(result),
            "  |  ".join(f"{k}: {v}" for k, v in counts.items()),
        )

    return result


def _classify_gap(row: pd.Series) -> str:
    if pd.isna(row.get("doi_norm")):
        return GAP_NO_DOI
    if not row.get("in_infoscience"):
        return GAP_NOT_IN_INFOSCIENCE

    access = str(row.get("existing_access_level") or "").strip().lower()
    version = str(row.get("resolved_version") or "").strip()
    if access in _OPEN_ACCESS_LEVELS and version in _FULLTEXT_VERSIONS:
        return GAP_OPEN_WITH_FULLTEXT
    return GAP_NEEDS_ATTENTION

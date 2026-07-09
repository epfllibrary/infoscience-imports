"""Read & Publish (R&P) APC tracking consolidator.

Consolidates publisher R&P tracking workbooks — data/apc/{publisher}/{contract}/*.xls*
— into a single normalized table. This is the source-of-truth list of articles for
which EPFL Library committed to cover the OA cost under a Read & Publish agreement;
downstream OA Monitor matching (against Infoscience + OpenAlex/Unpaywall) verifies
that each one is actually deposited and open access with the published version.

Directory layout is itself metadata:
    data/apc/{publisher}/{contract_id}/{yearly tracking workbook}

The "year" in a workbook's filename is an operational tracking-round label, not a
strict partition of notification year — rows notified near year-end regularly spill
into the following year's workbook. It is kept as provenance (tracking_year), not
used to filter or bucket rows.

A merged multi-year archive (filename matching "_YYYY-YYYY") duplicates the
individual yearly workbooks and is intentionally excluded — see discover_rap_files().
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from oa_monitor.oa_classifier import LICENSE_MAP
from oa_monitor.oa_enricher import normalize_doi
from utils import get_pipeline_logger

logger = get_pipeline_logger("rap_consolidator")

# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

_LOCK_FILE_PREFIX = "~$"
_RANGE_FILENAME_RE = re.compile(r"_(\d{4})-(\d{4})\.xls[xm]?$", re.IGNORECASE)
_YEARLY_FILENAME_RE = re.compile(r"_(\d{4})\.xls[xm]?$", re.IGNORECASE)


def discover_rap_files(root: str | Path = "data/apc") -> list[dict]:
    """Walk {publisher}/{contract}/*.xls* and return yearly tracking file descriptors.

    Skips Excel lock files (~$...) and merged multi-year archives (e.g. a
    filename ending in "_2020-2022.xlsx") which duplicate the individual
    yearly workbooks.
    """
    root = Path(root)
    found: list[dict] = []
    if not root.exists():
        return found

    for publisher_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for contract_dir in sorted(p for p in publisher_dir.iterdir() if p.is_dir()):
            for file in sorted(contract_dir.glob("*.xls*")):
                if file.name.startswith(_LOCK_FILE_PREFIX):
                    continue
                if _RANGE_FILENAME_RE.search(file.name):
                    logger.info("  skipping merged archive: %s", file.name)
                    continue
                match = _YEARLY_FILENAME_RE.search(file.name)
                if not match:
                    logger.warning("  unrecognized filename pattern, skipping: %s", file.name)
                    continue
                found.append({
                    "path": file,
                    "publisher": publisher_dir.name,
                    "contract_id": contract_dir.name,
                    "tracking_year": int(match.group(1)),
                })
    return found


# ---------------------------------------------------------------------------
# Generic R&P workbook parsing
# ---------------------------------------------------------------------------
#
# In practice, R&P tracking workbooks are near-identical across publishers
# (Elsevier, AIP, Springer, Taylor&Francis all use the same base columns, some
# adding Actual APC / Currency / Comments). A single generic parser handles
# any publisher — new ones need only a matching data/apc/{publisher}/ folder,
# no dedicated code.

_NON_DATA_SHEETS = {"EmailDraft"}

# Canonical R&P tracking columns. Presence varies by publisher/year (BL and
# Funding come and go, Actual APC/Currency/Comments are publisher additions)
# — missing columns become NaN rather than raising.
_CORE_COLUMNS = [
    "Date notification", "Journal", "OA type", "Contact Author", "School",
    "Institute", "Lab", "PI", "BL", "Article Title", "Submission date",
    "Acceptance Date", "Publication date", "DOI", "Article Type", "License",
    "Funding", "Actual APC", "Currency", "Comments",
]

_COLUMN_ALIASES = {
    "Acceptance date": "Acceptance Date",  # Elsevier's 2020 workbook alone
    "Notes": "Comments",                   # Taylor&Francis uses "Notes"
}

# A sheet must carry at least one of these to be treated as R&P tracking
# data — filters out stray scratch sheets (e.g. a leftover "Feuil1") that
# would otherwise be ingested as a row of nulls.
_DATA_SHEET_SIGNAL_COLUMNS = frozenset({"DOI", "Journal"})

# Every primary tracking sheet observed across all publishers/years is named
# "Suivi_YYYY" / "YYYY_suivi" (the EPFL Library template). A secondary sheet
# a librarian creates for their own bookkeeping — an invoice checklist, a
# scratch pivot — can reuse the same DOI/Journal header row while duplicating
# the same articles (usually with the DOI left blank), which the column check
# alone cannot tell apart from real data. The sheet name is required too.
_SUIVI_NAME_MARKER = "suivi"

_DATE_COLUMNS = ("Date notification", "Submission date", "Acceptance Date", "Publication date")


def _coerce_consistent_types(df: pd.DataFrame) -> None:
    """Force a single dtype per raw column, in place.

    These workbooks are hand-edited: some rows carry a genuine Excel date
    while others carry a literal "?" placeholder in the same date column, and
    grant numbers in "Funding"/"Actual APC" are sometimes bare integers instead
    of text. pandas reads both as object dtype mixing Python types
    (datetime/str, int/str), which pyarrow/DuckDB cannot convert to a table.
    Date columns are parsed with errors="coerce" (unparsable placeholders
    become NaT); every other raw column is stringified.
    """
    for col in _CORE_COLUMNS:
        if col in _DATE_COLUMNS:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = df[col].astype("string")


def parse_rap_workbook(descriptor: dict) -> pd.DataFrame:
    """Parse one publisher R&P tracking workbook into a normalized DataFrame.

    Reads every sheet that looks like the primary tracking data — its name
    contains "suivi" AND it has a DOI or Journal column after alias
    resolution — skipping known non-data sheets (e.g. "EmailDraft" email
    templates), scratch sheets, and secondary bookkeeping sheets (e.g. an
    invoice-tracking checklist) that reuse the same header row. Maps columns
    by name (tolerant of missing/renamed columns) and adds provenance +
    normalized columns.
    """
    path = descriptor["path"]
    xl = pd.ExcelFile(path)

    frames = []
    for sheet in xl.sheet_names:
        if sheet in _NON_DATA_SHEETS:
            continue
        if _SUIVI_NAME_MARKER not in sheet.lower():
            logger.warning(
                "  sheet name has no '%s' marker in %s :: %s — skipping sheet",
                _SUIVI_NAME_MARKER, path.name, sheet,
            )
            continue
        df = xl.parse(sheet, header=0)
        df.columns = [str(c) for c in df.columns]
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        df = df.rename(columns=_COLUMN_ALIASES)

        if not _DATA_SHEET_SIGNAL_COLUMNS & set(df.columns):
            logger.warning(
                "  no recognizable tracking columns in %s :: %s — skipping sheet",
                path.name, sheet,
            )
            continue

        for col in _CORE_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        frames.append(df[_CORE_COLUMNS].copy())

    result = (
        pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=_CORE_COLUMNS)
    )

    _coerce_consistent_types(result)

    result["publisher"] = descriptor["publisher"]
    result["contract_id"] = descriptor["contract_id"]
    result["tracking_year"] = descriptor["tracking_year"]
    result["source_file"] = path.name

    result["doi_norm"] = result["DOI"].map(normalize_doi)

    oa_type_canonical, oa_type_flag = _normalize_categorical(result["OA type"], _OA_TYPE_MAP)
    result["oa_type_normalized"] = oa_type_canonical
    result["oa_type_needs_review"] = oa_type_flag

    article_type_canonical, article_type_flag = _normalize_categorical(
        result["Article Type"], _ARTICLE_TYPE_MAP
    )
    result["article_type_normalized"] = article_type_canonical
    result["article_type_needs_review"] = article_type_flag

    license_canonical, license_flag = _normalize_categorical(result["License"], LICENSE_MAP)
    result["license_normalized"] = license_canonical
    result["license_needs_review"] = license_flag

    return result


# ---------------------------------------------------------------------------
# Categorical normalization
# ---------------------------------------------------------------------------
#
# Unrecognized non-empty values are deliberately left as canonical=None with
# needs_review=True instead of being guessed at — these are librarian data-entry
# slips (typos, "?" / "check" placeholders) that need a human decision, not a
# silent reinterpretation.

_OA_TYPE_MAP: dict[str, str] = {
    "hybrid": "Hybrid",
    "hybrid open access": "Hybrid",
    "gold": "Gold",
    "gold oa": "Gold",
    "full open access": "Gold",
    "open access": "Gold",
    "oa": "Gold",  # Taylor&Francis shorthand
}

# Article-type labels vary by publisher for the same underlying concept:
# OriginalPaper (Springer) / Article (AIP) / Research Article (Taylor&Francis)
# / FLA / Full-length article / Journal (Elsevier) are all a standard primary
# research article.
_ARTICLE_TYPE_MAP: dict[str, str] = {
    "fla": "Full-length article",
    "full-length article": "Full-length article",
    "journal": "Full-length article",
    "originalpaper": "Full-length article",
    "article": "Full-length article",
    "research article": "Full-length article",
    "research-article": "Full-length article",  # OUP
    "review article": "Review article",
    "review": "Review article",
    "rev": "Review article",
    "review paper": "Review article",
    "review-article": "Review article",  # OUP
    "short review": "Short review",
    "short communication": "Short communication",
    "sco": "Short communication",
    "briefcommunication": "Short communication",
    "dat": "Data article",
    "original software publication": "Software publication",
    "osp": "Software publication",
    "case report": "Case report",
    "microarticle": "Microarticle",
    "perspective": "Perspective",
    "method": "Method",
}


def _normalize_categorical(
    series: pd.Series, mapping: dict[str, str]
) -> tuple[pd.Series, pd.Series]:
    """Map a raw categorical series via a lowercase-stripped lookup.

    Returns (canonical, needs_review): canonical is NaN for both missing values
    and unrecognized ones; needs_review is True only for non-empty values that
    failed to map (missing values are not a data-quality issue).
    """
    stripped = series.astype("string").str.strip()
    lower = stripped.str.lower()
    canonical = lower.map(mapping)
    is_present = stripped.notna() & (stripped != "")
    needs_review = is_present & canonical.isna()
    return canonical, needs_review


def normalize_oa_type(raw) -> tuple[str | None, bool]:
    """Normalize a single R&P 'OA type' value. See _normalize_categorical()."""
    canonical, needs_review = _normalize_categorical(pd.Series([raw]), _OA_TYPE_MAP)
    return (canonical.iloc[0] if pd.notna(canonical.iloc[0]) else None), bool(needs_review.iloc[0])


def normalize_article_type(raw) -> tuple[str | None, bool]:
    """Normalize a single R&P 'Article Type' value. See _normalize_categorical()."""
    canonical, needs_review = _normalize_categorical(pd.Series([raw]), _ARTICLE_TYPE_MAP)
    return (canonical.iloc[0] if pd.notna(canonical.iloc[0]) else None), bool(needs_review.iloc[0])


def normalize_license(raw) -> tuple[str | None, bool]:
    """Normalize a single R&P 'License' value against the shared LICENSE_MAP.

    Unlike oa_classifier.normalize_license() (which merges two sources and
    always returns a string), this flags unrecognized values instead of
    passing them through unchanged — the R&P sheet is hand-edited and can
    contain typos ("CC BY 4.1") or unresolved placeholders ("check", "?").
    """
    canonical, needs_review = _normalize_categorical(pd.Series([raw]), LICENSE_MAP)
    return (canonical.iloc[0] if pd.notna(canonical.iloc[0]) else None), bool(needs_review.iloc[0])


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def consolidate_rap_tracking(root: str | Path = "data/apc") -> pd.DataFrame:
    """Discover, parse, and deduplicate all R&P tracking workbooks under root.

    Every discovered workbook is parsed with the same generic parser
    (parse_rap_workbook) regardless of publisher — see module docstring.
    """
    descriptors = discover_rap_files(root)

    frames = [parse_rap_workbook(descriptor) for descriptor in descriptors]

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    dup_mask = result["doi_norm"].notna() & result.duplicated("doi_norm", keep="first")
    if dup_mask.any():
        logger.warning("  %d duplicate DOI rows dropped during consolidation", int(dup_mask.sum()))
    result = result[~dup_mask].reset_index(drop=True)

    logger.info(
        "  R&P consolidation: %d rows from %d workbook(s)", len(result), len(descriptors),
    )
    return result

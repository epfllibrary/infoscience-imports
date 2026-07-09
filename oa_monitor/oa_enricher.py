"""OA Enricher pipeline — Phase 1 (read-only).

Orchestrates the OA Monitor & NOAM export workflow on existing Infoscience items:
  M1  harvest_infoscience()     — query DSpace by year range / resource type
  M2  enrich_openalex()         — batch DOI fetch from OpenAlex (40 DOIs/request)
      enrich_unpaywall()        — parallel Unpaywall fetch (ThreadPoolExecutor)
      apply_classification()    — resolve version/licence, classify OA category
  M6  export_noam()             — produce per-year CSVs + Excel summary

Entry point: python oa_monitor/run_oa_enricher.py [--step all|harvest|classify|export-noam]
"""

from __future__ import annotations

import math
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from clients.openalex_client import OpenAlexClient
from clients.unpaywall_client import UnpaywallClient
from oa_monitor.oa_classifier import (
    classify_oa,
    normalize_license,
    resolve_version,
)
from oa_monitor.type_mapping import resolve_noam_type
from utils import get_pipeline_logger

logger = get_pipeline_logger("oa_enricher")

# Columns written by enrich_openalex() — all come from the OpenAlex digest format.
_OA_COLUMNS: list[str] = [
    "oa_status",
    "oa_is_oa",
    "oa_any_repository_has_fulltext",
    "best_oa_is_oa",
    "best_oa_is_in_doaj",
    "best_oa_version",
    "best_oa_license",
    "best_oa_pdf_url",
    "best_oa_host_org",
    "primary_source_type",
    "apc_list_value_usd",
    "apc_paid_value_usd",
    "has_content_pdf",
    "content_url_pdf",
]

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_str(val) -> str | None:
    """Convert a pandas scalar (including NaN/NA/None) to str or None."""
    if val is None:
        return None
    try:
        import pandas as _pd
        if _pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return s if s else None


# Infoscience is harvested independently in M1 (existing_access_level /
# existing_license); an OpenAlex location pointing back at Infoscience is not
# a corroborating external signal and must not feed the OA classification.
_INFOSCIENCE_OAI_PREFIX = "pmh:oai:infoscience.epfl.ch:"

_BEST_OA_LOCATION_FIELDS = (
    "best_oa_is_oa", "best_oa_is_in_doaj", "best_oa_version",
    "best_oa_license", "best_oa_pdf_url", "best_oa_host_org",
)
_PRIMARY_LOCATION_FIELDS = (
    "primary_source_type",
)


def _is_infoscience_location(location_id) -> bool:
    """True if an OpenAlex location id is an Infoscience OAI-PMH record."""
    if not location_id:
        return False
    return str(location_id).lower().startswith(_INFOSCIENCE_OAI_PREFIX)


def _discard_infoscience_self_reference(rec: dict) -> None:
    """Blank out location-derived fields when the location is Infoscience itself.

    Mutates rec in place. OpenAlex sometimes resolves best_oa_location (or
    primary_location) to the very Infoscience record the OA Enricher is
    already tracking (id starting with "pmh:oai:infoscience.epfl.ch:").
    Keeping those fields would let the classifier "confirm" an item's OA
    status using data that only mirrors what M1 already harvested from
    Infoscience, instead of an independent source.
    """
    if _is_infoscience_location(rec.get("best_oa_location_id")):
        for field in _BEST_OA_LOCATION_FIELDS:
            rec[field] = None
    if _is_infoscience_location(rec.get("primary_location_id")):
        for field in _PRIMARY_LOCATION_FIELDS:
            rec[field] = None


def _safe_bool(val) -> bool:
    """Convert a pandas scalar to bool, treating NA/None as False.

    Some OpenAlex-sourced fields (e.g. best_oa_is_oa, best_oa_is_in_doaj) are
    stored as the stringified Python bool ("True"/"False"/"") rather than a
    real bool — see clients/openalex_client.py. A naive bool(val) would treat
    the non-empty string "False" as truthy, so string values are parsed
    explicitly instead of falling through to Python's default truthiness.
    """
    if val is None:
        return False
    try:
        import pandas as _pd
        if _pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(val, str):
        return val.strip().lower() == "true"
    return bool(val)


# ---------------------------------------------------------------------------
# DOI normalisation
# ---------------------------------------------------------------------------

_DOI_PREFIXES = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi.org/",
)


def normalize_doi(doi) -> str | None:
    """Return a plain DOI string (10.xxx/yyy) from any URL-or-plain form.

    Returns None for non-DOI values (e.g. arXiv URLs, empty strings).
    """
    if doi is None:
        return None
    try:
        import pandas as _pd
        if _pd.isna(doi):
            return None
    except (TypeError, ValueError):
        pass
    s = str(doi).strip().lower()
    if not s:
        return None
    for prefix in _DOI_PREFIXES:
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    # Valid DOIs must start with the "10." prefix
    return s if s.startswith("10.") else None


# ---------------------------------------------------------------------------
# M2 — OpenAlex batch DOI fetch
# ---------------------------------------------------------------------------

def enrich_openalex(df: pd.DataFrame, batch_size: int = 100) -> pd.DataFrame:
    """Add OpenAlex OA metadata columns by batch-fetching DOIs.

    Sends DOIs to OpenAlex in batches of batch_size using the pipe-separated
    filter syntax (filter=doi:doi1|doi2|...).  Rows without a DOI or with no
    OpenAlex match receive NaN for all added columns.

    With a premium API key (OPENALEX_API_KEY), batch_size=100 is safe and
    efficient; the per_page parameter is set to match batch_size so a single
    page covers each batch.

    Returns a new DataFrame (does not mutate the input).
    """
    result = df.copy()

    # Initialise all OA columns with NaN so unmatched rows stay empty
    for col in _OA_COLUMNS:
        result[col] = pd.NA

    # Build list of valid DOIs from the DataFrame.
    # normalize_doi strips https://doi.org/ prefixes and rejects non-DOI values
    # (arXiv URLs, empty strings, etc.) that would cause OpenAlex 400 errors.
    doi_series = result["doi"] if "doi" in result.columns else pd.Series([], dtype=object)
    norm_dois = doi_series.map(normalize_doi)
    valid_dois = norm_dois.dropna().tolist()

    if not valid_dois:
        logger.info("  OpenAlex: no DOIs to enrich")
        return result

    n_batches = math.ceil(len(valid_dois) / batch_size)
    no_doi = len(df) - len(valid_dois)
    logger.info(
        "  OpenAlex: %d DOIs to fetch (%d rows without DOI) — %d batch(es) of %d",
        len(valid_dois), no_doi, n_batches, batch_size,
    )

    # Fetch in batches and build a DOI → record lookup
    lookup: dict[str, dict] = {}
    for i in range(0, len(valid_dois), batch_size):
        batch_num = i // batch_size + 1
        chunk = valid_dois[i : i + batch_size]
        logger.info("  batch %d/%d — %d DOIs…", batch_num, n_batches, len(chunk))
        doi_filter = "|".join(chunk)
        records = OpenAlexClient.fetch_records(
            filter=f"doi:{doi_filter}",
            per_page=batch_size,
        )
        for rec in records:
            doi_key = normalize_doi(rec.get("doi"))
            if doi_key:
                _discard_infoscience_self_reference(rec)
                lookup[doi_key] = rec
        logger.info("  batch %d/%d — %d/%d matched so far",
                    batch_num, n_batches, len(lookup), (i + len(chunk)))

    logger.info(
        "  OpenAlex: %d/%d DOIs matched (%.0f%%)",
        len(lookup), len(valid_dois),
        100 * len(lookup) / len(valid_dois) if valid_dois else 0,
    )

    # Map records back to DataFrame rows by normalised DOI
    for col in _OA_COLUMNS:
        result[col] = norm_dois.apply(
            lambda d, _col=col: _lookup_field(lookup, d, _col)
        )

    return result


def _lookup_field(lookup: dict[str, dict], doi, field: str):
    """Return record[field] for a normalised DOI key, or pd.NA if not found."""
    if doi is None:
        return pd.NA
    key = normalize_doi(doi)
    if key is None:
        return pd.NA
    rec = lookup.get(key)
    if rec is None:
        return pd.NA
    val = rec.get(field)
    return val if val is not None else pd.NA


# ---------------------------------------------------------------------------
# M2 — Unpaywall parallel fetch
# ---------------------------------------------------------------------------

# Fields derived from Unpaywall's best_oa_location — blanked out when that
# location is Infoscience itself (see _discard_infoscience_self_reference_upw).
_UPW_LOCATION_FIELDS = ("is_oa", "license", "version", "pdf_urls")


def _discard_infoscience_self_reference_upw(rec: dict) -> None:
    """Blank out location-derived Unpaywall fields when the OA copy is Infoscience.

    Mutates rec in place. Mirrors _discard_infoscience_self_reference() for
    OpenAlex: Unpaywall's best_oa_location can also resolve to the Infoscience
    record M1 already harvested (pmh_id containing "infoscience.epfl.ch"),
    which is not an independent OA signal.
    """
    pmh_id = str(rec.get("pmh_id") or "").lower()
    if "infoscience.epfl.ch" in pmh_id:
        for field in _UPW_LOCATION_FIELDS:
            rec[field] = None


_UPW_COLUMNS: list[str] = [
    "upw_is_oa",
    "upw_oa_status",
    "upw_license",
    "upw_version",
    "upw_pdf_urls",
    "upw_journal_is_oa",
    "upw_journal_is_in_doaj",
]

_UPW_KEY_MAP = {
    "is_oa":              "upw_is_oa",
    "oa_status":          "upw_oa_status",
    "license":            "upw_license",
    "version":            "upw_version",
    "pdf_urls":           "upw_pdf_urls",
    "journal_is_oa":      "upw_journal_is_oa",
    "journal_is_in_doaj": "upw_journal_is_in_doaj",
}


def enrich_unpaywall(df: pd.DataFrame, workers: int = 5) -> pd.DataFrame:
    """Add Unpaywall OA metadata columns via parallel DOI fetch.

    Uses skip_pdf=True — no PDF download, metadata only. This makes the
    function safe to call as a read-only enrichment step.

    Returns a new DataFrame (does not mutate the input).
    """
    result = df.copy()
    for col in _UPW_COLUMNS:
        result[col] = pd.NA

    doi_series = result["doi"] if "doi" in result.columns else pd.Series([], dtype=object)
    valid_mask = doi_series.notna() & (doi_series.astype(str).str.strip() != "") & (doi_series.astype(str) != "nan")
    index_doi_pairs = [(idx, doi) for idx, doi in doi_series[valid_mask].items()]

    if not index_doi_pairs:
        logger.info("  Unpaywall: no DOIs to fetch")
        return result

    total = len(index_doi_pairs)
    log_every = max(1, total // 10)  # log ~10 progress lines regardless of total
    logger.info("  Unpaywall: %d DOIs to fetch (%d workers)…", total, workers)

    def _fetch(idx_doi):
        idx, doi = idx_doi
        rec = UnpaywallClient.fetch_by_doi(doi, skip_pdf=True)
        if rec:
            _discard_infoscience_self_reference_upw(rec)
        return idx, rec

    lookup: dict[int, dict] = {}
    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch, pair): pair for pair in index_doi_pairs}
        for future in as_completed(futures):
            idx, rec = future.result()
            if rec:
                lookup[idx] = rec
            completed += 1
            if completed % log_every == 0 or completed == total:
                logger.info("  Unpaywall: %d/%d done (%d matched)…",
                            completed, total, len(lookup))

    logger.info(
        "  Unpaywall: %d/%d DOIs matched (%.0f%%)",
        len(lookup), total, 100 * len(lookup) / total if total else 0,
    )

    for src_key, col in _UPW_KEY_MAP.items():
        values = {idx: rec.get(src_key, pd.NA) for idx, rec in lookup.items()}
        result[col] = result.index.map(lambda i, v=values: v.get(i, pd.NA))

    return result


# ---------------------------------------------------------------------------
# M2 — classification step (pure, no API calls)
# ---------------------------------------------------------------------------

def apply_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Add resolved_version, resolved_license, oa_category_basic, oa_category_advanced.

    Operates on a DataFrame that contains columns produced by M1 (Infoscience harvest)
    and M2-Step1 (OpenAlex DOI fetch).  Missing columns are treated as None/empty.

    Returns a new DataFrame (does not mutate the input).
    """
    result = df.copy()

    resolved_versions: list[str | None] = []
    resolved_licenses: list[str] = []
    oa_basic: list[str] = []
    oa_advanced: list[str] = []
    oa_sources: list[str] = []

    for _, row in result.iterrows():
        # Determine effective OA signals.
        # OpenAlex is the primary source; Unpaywall is the fallback when OpenAlex
        # returned no match (oa_status absent or empty).
        oa_status = _safe_str(row.get("oa_status")) or ""
        best_oa_is_oa = _safe_bool(row.get("best_oa_is_oa"))
        best_oa_version = _safe_str(row.get("best_oa_version"))
        best_oa_license = _safe_str(row.get("best_oa_license"))
        primary_source = _safe_str(row.get("primary_source_type")) or ""
        oa_src = "openalex" if oa_status else "none"

        if not oa_status:
            upw_status = _safe_str(row.get("upw_oa_status")) or ""
            if upw_status:
                # Promote Unpaywall as effective source.
                # When there's no OpenAlex match, primary_source_type is undefined, so
                # we infer it from the Unpaywall OA colour (green → repository, else journal).
                oa_status = upw_status
                best_oa_is_oa = _safe_bool(row.get("upw_is_oa"))
                primary_source = "repository" if upw_status == "green" else "journal"
                oa_src = "unpaywall"

        # Supplement: fill version and license gaps from Unpaywall when OpenAlex lacks them
        eff_version = best_oa_version or _safe_str(row.get("upw_version"))
        eff_license = best_oa_license or _safe_str(row.get("upw_license"))

        # --- version resolution ---
        ver = resolve_version(
            existing_uri=_safe_str(row.get("existing_version")),
            legacy=_safe_str(row.get("legacy_version")),
            oa_version=eff_version,
        )
        resolved_versions.append(ver)

        # --- licence normalisation ---
        lic = normalize_license(
            existing=_safe_str(row.get("existing_license")),
            oa_license=eff_license,
        )
        resolved_licenses.append(lic)

        # DOAJ membership — OpenAlex primary, Unpaywall fallback (mirrors the
        # oa_status/version/license fallback pattern above).
        is_in_doaj = (
            _safe_bool(row.get("best_oa_is_in_doaj"))
            or _safe_bool(row.get("upw_journal_is_in_doaj"))
        )

        # --- OA classification ---
        row_dict = {
            "existing_access_level": _safe_str(row.get("existing_access_level")) or "",
            "best_oa_is_oa": best_oa_is_oa,
            "oa_status": oa_status,
            "dc_type": _safe_str(row.get("dc_type")) or "",
            "resolved_license": lic,
            "resolved_version": ver or "",
            "primary_source_type": primary_source,
            "publishedin": _safe_str(row.get("publishedin")) or "",
            "is_in_doaj": is_in_doaj,
        }
        basic, advanced = classify_oa(row_dict)
        oa_basic.append(basic)
        oa_advanced.append(advanced)
        oa_sources.append(oa_src)

    result["resolved_version"] = resolved_versions
    result["resolved_license"] = resolved_licenses
    result["oa_category_basic"] = oa_basic
    result["oa_category_advanced"] = oa_advanced
    result["resolved_oa_source"] = oa_sources

    counts = result["oa_category_advanced"].value_counts()
    breakdown = "  |  ".join(f"{cat}: {n}" for cat, n in counts.items())
    logger.info("  Classification: %d rows — %s", len(result), breakdown)
    open_n = int((result["oa_category_basic"] == "Open").sum())
    logger.info(
        "  Open Access: %d/%d (%.0f%%)",
        open_n, len(result), 100 * open_n / len(result) if len(result) else 0,
    )

    return result


# ---------------------------------------------------------------------------
# M6 — NOAM export
# ---------------------------------------------------------------------------

EPFL_AFFILIATION_ID = "02s376052"


def export_noam(
    df: pd.DataFrame,
    output_dir: str | Path,
    year_from: int,
    year_to: int,
    affiliation_id: str = EPFL_AFFILIATION_ID,
) -> dict:
    """Build NOAM per-year CSVs and one Excel summary file.

    Returns:
        {
            "csv_files": {year: Path, ...},
            "excel_file": Path,
        }
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.today().strftime("%Y-%m-%d")

    noam = _build_noam_df(df, affiliation_id)

    # Filter to requested year range
    noam = noam[
        noam["Issue Date"].notna()
        & (noam["Issue Date"] >= year_from)
        & (noam["Issue Date"] <= year_to)
    ]

    logger.info("  NOAM export: %d rows to write", len(noam))

    # Per-year CSVs
    csv_files: dict[int, Path] = {}
    for year in sorted(noam["Issue Date"].dropna().unique()):
        year_int = int(year)
        subset = noam[noam["Issue Date"] == year_int]
        path = output_dir / f"{year_int}_repo-data_dataset_epfl_{date_str}.csv"
        subset.to_csv(path, index=False)
        csv_files[year_int] = path
        logger.info("  CSV  %d : %d rows → %s", year_int, len(subset), path.name)

    # Excel summary
    excel_path = output_dir / f"{year_from}_{year_to}_NOAM_repository_survey_{date_str}.xlsx"
    basic = _summary_basic(noam, year_from, year_to)
    advanced = _summary_advanced(noam, year_from, year_to)
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        basic.to_excel(writer, sheet_name="Basic OA Summary", index=False)
        advanced.to_excel(writer, sheet_name="Advanced OA Summary", index=False)
    logger.info("  Excel: %s", excel_path.name)

    return {"csv_files": csv_files, "excel_file": excel_path}


def _build_noam_df(df: pd.DataFrame, affiliation_id: str) -> pd.DataFrame:
    """Map internal columns to NOAM output columns.

    Resource type is classified from dc_type_authority (the COAR code) rather
    than dc_type's free-text value — language-independent, see
    oa_monitor/type_mapping.py. Items whose authority doesn't resolve to a
    NOAM type (out of scope, e.g. conference poster, or unrecognized) are
    dropped from the export.
    """
    authority = df.get("dc_type_authority", pd.Series(index=df.index, dtype=object))
    resource_type = authority.map(resolve_noam_type)
    df = df[resource_type.notna()].copy()
    resource_type = resource_type[resource_type.notna()]

    out = pd.DataFrame(index=df.index)
    out["Creator affiliation"] = affiliation_id
    out["Creator"] = df.get("authors")
    out["Title"] = df.get("title")
    out["uuid"] = df.get("infoscience_uuid")
    out["Identifier: Repository DOI/Handle"] = df.get("handle")
    out["Identifier: Publisher's DOI"] = df.get("doi")
    out["ISBN or ISSN(s)"] = df.apply(_merge_isbn_issn, axis=1)
    out["Published in"] = df.get("publishedin")
    out["Issue Date"] = pd.to_numeric(df.get("pubyear"), errors="coerce").astype("float64")
    out["Resource type"] = resource_type
    out["OA category basic"] = df.get("oa_category_basic")
    out["OA category advanced"] = df.get("oa_category_advanced")
    out["License Condition"] = df.get("resolved_license")
    out["Resource version"] = df.get("resolved_version")
    out["Full-text: Embargo end date"] = _filter_embargo(df.get("embargo"))
    return out.reset_index(drop=True)


def _merge_isbn_issn(row) -> str | None:
    parts = []
    for field in ("issn", "isbn"):
        val = row.get(field)
        if val and str(val).strip() and str(val).strip().lower() not in ("nan", "none"):
            parts.append(str(val).strip())
    return "||".join(parts) if parts else None


def _filter_embargo(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype=object)
    dates = pd.to_datetime(series, errors="coerce")
    today = pd.Timestamp.today().normalize()
    dates[dates < today] = pd.NaT
    return dates


def _summary_basic(df: pd.DataFrame, year_from: int, year_to: int) -> pd.DataFrame:
    subset = df[df["Issue Date"].notna() & (df["Issue Date"] >= year_from) & (df["Issue Date"] <= year_to)]
    return (
        subset.groupby(["Creator affiliation", "Issue Date", "Resource type", "OA category basic"])
        .size()
        .reset_index(name="Count")
        .sort_values(["Issue Date", "Resource type", "Count"], ascending=[True, True, False])
    )


def _summary_advanced(df: pd.DataFrame, year_from: int, year_to: int) -> pd.DataFrame:
    subset = df[df["Issue Date"].notna() & (df["Issue Date"] >= year_from) & (df["Issue Date"] <= year_to)]
    return (
        subset.groupby(["Creator affiliation", "Issue Date", "Resource type", "OA category basic", "OA category advanced"])
        .size()
        .reset_index(name="Count")
        .sort_values(["Issue Date", "Resource type", "Count"], ascending=[True, True, False])
    )

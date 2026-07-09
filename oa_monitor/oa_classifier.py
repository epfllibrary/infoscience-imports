"""Pure OA classification functions for the OA Enricher pipeline.

All functions are side-effect-free and operate on plain dicts or strings,
making them straightforward to unit-test without any API or DataFrame dependency.

Business logic ported from docs/noam_survey.py (Marimo notebook, validated on
production data). Three differences vs the initial spec are intentional:
  1. existing_access_level == 'open access' is a first-class OA signal alongside
     best_oa_is_oa — items already open in Infoscience qualify without OpenAlex.
  2. Green OA requires resolved_version in {acceptedVersion, publishedVersion}
     to exclude preprints from Green classification.
  3. The CC licence set for Gold/Hybrid includes cc0 and public-domain (not just
     licences starting with 'cc-').
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Version resolution constants
# ---------------------------------------------------------------------------

COAR_VERSION_MAP: dict[str, str] = {
    "http://purl.org/coar/version/c_970fb48d4fbd8a85": "publishedVersion",
    "http://purl.org/coar/version/c_ab4af688f83e57aa": "acceptedVersion",
    "http://purl.org/coar/version/c_71e4c1898caa6e32": "submittedVersion",
    "http://purl.org/coar/version/c_be7fb7dd8ff6fe43": "copyright",
    "http://purl.org/coar/version/c_e19f295774971610": "correctedVersion",
}

_VERSION_PRIORITY: list[str] = [
    "publishedVersion",
    "acceptedVersion",
    "submittedVersion",
    "copyright",
    "correctedVersion",
]

# ---------------------------------------------------------------------------
# License normalisation constants
# ---------------------------------------------------------------------------

LICENSE_MAP: dict[str, str] = {
    "apache license": "other-oa",
    "cc by": "cc-by",
    "cc-by": "cc-by",
    "cc by 4.0": "cc-by",
    "creative commons attribution 4.0 international": "cc-by",
    "cc-by-nc": "cc-by-nc",
    "cc by nc": "cc-by-nc",
    "cc by-nc": "cc-by-nc",
    "cc by-nc 4.0": "cc-by-nc",
    "cc-by-nc 4.0": "cc-by-nc",
    "cc by nc nd": "cc-by-nc-nd",
    "cc-by-nc-nd": "cc-by-nc-nd",
    "cc by-nc-nd": "cc-by-nc-nd",
    "cc by-nc-nd 4.0": "cc-by-nc-nd",
    "cc-by-nc-nd 4.0": "cc-by-nc-nd",
    "cc by nc nd 4.0": "cc-by-nc-nd",
    "cc-by-nc-sa": "cc-by-nc-sa",
    "cc by nc sa": "cc-by-nc-sa",
    "cc by-nc-sa 4.0": "cc-by-nc-sa",
    "cc-by-nc-sa 4.0": "cc-by-nc-sa",
    "cc by-nd": "cc-by-nd",
    "cc-by-nd": "cc-by-nd",
    "cc by-nd 4.0": "cc-by-nd",
    "cc-by-nd 4.0": "cc-by-nd",
    "cc by-sa": "cc-by-sa",
    "cc-by-sa": "cc-by-sa",
    "cc by-sa 4.0": "cc-by-sa",
    "cc-by-sa 4.0": "cc-by-sa",
    "cc0": "cc0",
    "copyright": "copyright",
    "gnu-gpl": "gpl-v3",
    "gpl-v3": "gpl-v3",
    "mit": "mit",
    "mit license": "mit",
    "open access": "other-oa",
    "optica open access publishing agreement": "publisher-specific-oa",
    "optica publishing group under the terms of the optica open access publishing agreement": "publisher-specific-oa",
    "other-oa": "other-oa",
    "public-domain": "public-domain",
    "publisher-specific-oa": "publisher-specific-oa",
    "": "n/a",
    "none": "n/a",
    "null": "n/a",
}

_LICENSE_PRIORITY: list[str] = [
    "cc-by",
    "cc-by-nc",
    "cc-by-nc-nd",
    "cc-by-nc-sa",
    "cc-by-nd",
    "cc-by-sa",
    "cc0",
    "public-domain",
    "gpl-v3",
    "mit",
    "other-oa",
    "publisher-specific-oa",
    "n/a",
]

# Set used in OA classification rules (Gold / Hybrid conditions)
CC_LICENSES: frozenset[str] = frozenset({
    "cc-by", "cc-by-nc", "cc-by-nc-nd", "cc-by-nc-sa",
    "cc-by-nd", "cc-by-sa", "cc0", "public-domain",
})

# DSpace COAR types that qualify as book/proceedings contribution for Gold OA rule.
# NOAM: "Book part and CC licence → Gold, even if not in a fully Gold OA book."
# Conference poster, conference presentation are excluded per NOAM — not listed here.
# A single "conference paper" is deliberately NOT included: it behaves like a
# journal article (an individual contribution within a container) rather than
# a standalone book/monograph, and must go through the same Gold/Hybrid path
# as journal articles instead of an automatic type-based Gold shortcut. Only
# the proceedings VOLUME itself ("conference proceedings") stays here, since
# that is edited-book-like.
_BOOK_TYPES: frozenset[str] = frozenset({
    "text::book/monograph",
    "text::book/monograph::book part or chapter",
    "text::conference output::conference proceedings",
})

# Versions that qualify for Green OA
_GREEN_VERSIONS: frozenset[str] = frozenset({"publishedVersion", "acceptedVersion"})


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def resolve_version(
    existing_uri: str | None,
    legacy: str | None,
    oa_version: str | None,
) -> str | None:
    """Return the highest-priority version from three sources.

    Sources (in order of preference within equal priority):
      1. existing_uri  — Infoscience oaire.version (COAR URI, mapped to readable name)
      2. legacy        — Infoscience epfl.publication.version (freetext, already readable)
      3. oa_version    — OpenAlex best_oa_version (already readable)

    Priority across all resolved candidates:
      publishedVersion > acceptedVersion > submittedVersion > copyright > correctedVersion
    """
    def _clean(val: str | None) -> str | None:
        if not val or not val.strip():
            return None
        val = val.strip()
        return COAR_VERSION_MAP.get(val, val)

    candidates: list[str] = []
    seen: set[str] = set()
    for raw in (existing_uri, legacy, oa_version):
        resolved = _clean(raw)
        if resolved and resolved not in seen:
            candidates.append(resolved)
            seen.add(resolved)

    for priority_version in _VERSION_PRIORITY:
        if priority_version in seen:
            return priority_version

    return None


def normalize_license(
    existing: str | None,
    oa_license: str | None,
) -> str:
    """Return the highest-priority normalised licence from two sources.

    Sources: Infoscience oaire.licenseCondition and OpenAlex best_oa_license.
    Both are normalised via LICENSE_MAP (case-insensitive), then the highest-
    priority value from _LICENSE_PRIORITY is returned.
    """
    def _normalise(val: str | None) -> str:
        if not val:
            return "n/a"
        key = val.strip().lower()
        return LICENSE_MAP.get(key, key)

    candidates: list[str] = []
    seen: set[str] = set()
    for raw in (existing, oa_license):
        norm = _normalise(raw)
        if norm not in seen:
            candidates.append(norm)
            seen.add(norm)

    for priority_license in _LICENSE_PRIORITY:
        if priority_license in seen:
            return priority_license

    return "n/a"


def classify_oa(row: dict) -> tuple[str, str]:
    """Classify a publication into (oa_category_basic, oa_category_advanced).

    Expected keys in row (all optional — missing keys default to safe falsy values):
      existing_access_level  str   — 'open access' | 'openaccess' | 'metadata-only' | …
      best_oa_is_oa          bool  — OpenAlex best OA location is open
      oa_status              str   — OpenAlex colour: gold | green | diamond | hybrid | closed
      dc_type                str   — COAR type URI
      resolved_license       str   — output of normalize_license()
      resolved_version       str   — output of resolve_version()
      primary_source_type    str   — journal | repository | conference | …
      publishedin            str   — container title (journal, proceedings, series)
      is_in_doaj             bool  — journal listed in the Directory of Open
                                     Access Journals (OpenAlex best_oa_is_in_doaj
                                     or Unpaywall journal_is_in_doaj)

    Returns:
      ('Open', 'Diamond' | 'Gold' | 'Hybrid' | 'Green') or ('Closed', 'Closed')
    """
    access = (row.get("existing_access_level") or "").strip().lower()
    is_open_infoscience = access in {"open access", "openaccess"}
    is_open_oa = bool(row.get("best_oa_is_oa"))
    # NOAM: "Resource type is OA = true / Fully Gold Journal or Book, as
    # defined by Unpaywall or DOAJ." DOAJ membership is itself evidence the
    # article is open — it doesn't need existing_access_level/best_oa_is_oa
    # to also say so.
    is_in_doaj = bool(row.get("is_in_doaj"))
    has_oa_signal = is_open_infoscience or is_open_oa or is_in_doaj

    oa_status = (row.get("oa_status") or "").strip().lower()
    dc_type = (row.get("dc_type") or "").strip().lower()
    license_ = (row.get("resolved_license") or "n/a").strip().lower()
    version = (row.get("resolved_version") or "").strip()
    source_type = (row.get("primary_source_type") or "").strip().lower()
    has_container = bool((row.get("publishedin") or "").strip())

    # 1. Diamond
    if has_oa_signal and oa_status == "diamond":
        return ("Open", "Diamond")

    # 2. Gold
    gold_by_status = oa_status == "gold"
    # NOAM: "Book part + CC licence → Gold." Version must be at least acceptedVersion —
    # a submitted preprint with a CC licence on the repository copy is Closed per NOAM.
    gold_by_type = (
        dc_type in _BOOK_TYPES
        and license_ in CC_LICENSES
        and version in _GREEN_VERSIONS
    )
    if has_oa_signal and (gold_by_status or is_in_doaj or gold_by_type):
        return ("Open", "Gold")

    # 3. Hybrid — CC licence + publishedVersion + not a repository
    if (
        has_oa_signal
        and license_ in CC_LICENSES
        and version == "publishedVersion"
        and source_type not in ("repository", "")
        and source_type
    ):
        return ("Open", "Hybrid")

    # 3b. Fallback Hybrid — journal/report with no external source data, but in a venue
    # (has container) with CC licence + publishedVersion. Conference/book types are
    # already handled by rule 2 (Gold) via _BOOK_TYPES.
    if (
        has_oa_signal
        and not source_type
        and has_container
        and license_ in CC_LICENSES
        and version == "publishedVersion"
    ):
        return ("Open", "Hybrid")

    # 4. Green — OA in a repository OR Infoscience open access signal, with valid version
    green_oa_signal = (is_open_oa and source_type == "repository") or is_open_infoscience
    if green_oa_signal and version in _GREEN_VERSIONS:
        return ("Open", "Green")

    return ("Closed", "Closed")

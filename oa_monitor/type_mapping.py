"""Resource-type classification via DSpace-CRIS authority codes (COAR types).

dc.type's free-text `value` is locale-dependent — Infoscience holds both
English ("text::journal::journal article") and French
("text::revue::article de revue") labels for the same COAR resource type.
Matching on that text requires enumerating every language variant by hand
and silently misses anything not yet seen.

Every dc.type metadata entry also carries an `authority` (e.g.
"article-coar-types:c_6501") — the same COAR Resource Type Vocabulary code
regardless of display language (confirmed live against Infoscience). This
module classifies resource types from that authority value instead, reusing
the canonical mapping already maintained for the main import pipeline
(config/mappings/types_authority.yaml, loaded as mappings.types_authority_mapping)
— a single source of truth for the dc.type <-> COAR code correspondence.
"""

from __future__ import annotations

from mappings import types_authority_mapping

# authority value ("article-coar-types:c_6501") -> canonical (English) dc.type
# key ("text::journal::journal article"), inverted from types_authority_mapping.
_AUTHORITY_TO_DC_TYPE: dict[str, str] = {v: k for k, v in types_authority_mapping.items()}

# NOAM simplified resource type, keyed by the canonical dc.type. The COAR
# authority code is identical regardless of which language produced the
# item's display label, so this single English-keyed mapping now covers
# every language Infoscience stores dc.type in.
_NOAM_TYPE_BY_DC_TYPE: dict[str, str] = {
    "text::book/monograph": "Book",
    "text::book/monograph::book part or chapter": "Book part",
    "text::conference output::conference proceedings": "Book",
    "text::conference output::conference proceedings::conference paper": "Conference paper",
    "text::journal::journal article": "Journal article",
    "text::journal::journal article::data paper": "Journal article",
    "text::journal::journal article::research article": "Journal article",
    "text::journal::journal article::review article": "Journal article",
    "text::journal::journal article::software paper": "Journal article",
}

NOAM_TYPES: frozenset[str] = frozenset(_NOAM_TYPE_BY_DC_TYPE.values())

# NOAM type -> every authority value that resolves to it, for building Solr
# clauses that cover all COAR sub-types in one query.
_AUTHORITIES_BY_NOAM_TYPE: dict[str, list[str]] = {}
for _dc_type, _noam_type in _NOAM_TYPE_BY_DC_TYPE.items():
    _authority = types_authority_mapping.get(_dc_type)
    if _authority:
        _AUTHORITIES_BY_NOAM_TYPE.setdefault(_noam_type, []).append(_authority)


def resolve_noam_type(authority: str | None) -> str | None:
    """Return the NOAM simplified type for a dc.type authority value.

    Returns one of "Journal article" / "Book" / "Book part" / "Conference
    paper", or None if the authority is missing, unrecognized, or a valid
    COAR type that is out of NOAM scope (e.g. a conference poster).
    """
    if not authority or not authority.strip():
        return None
    dc_type = _AUTHORITY_TO_DC_TYPE.get(authority.strip())
    if dc_type is None:
        return None
    return _NOAM_TYPE_BY_DC_TYPE.get(dc_type)


def solr_types_authority_clause(noam_type: str) -> str:
    """Build a `types_authority:(*c_x OR *c_y ...)` Solr clause for a NOAM type.

    Covers every COAR sub-type that resolves to this NOAM bucket (e.g.
    "Journal article" includes research/review/data/software paper variants).
    Returns "" for an unrecognized NOAM type.
    """
    authorities = _AUTHORITIES_BY_NOAM_TYPE.get(noam_type, [])
    if not authorities:
        return ""
    codes = [f"*{auth.split(':')[-1]}" for auth in authorities]
    return "types_authority:(" + " OR ".join(codes) + ")"


# Every authority value considered in NOAM scope (any of the 4 buckets) — used
# to build both the "all NOAM types" Solr harvest filter and the DuckDB
# post-harvest filter over the already-extracted dc_type_authority column.
NOAM_AUTHORITY_VALUES: frozenset[str] = frozenset(
    auth for authorities in _AUTHORITIES_BY_NOAM_TYPE.values() for auth in authorities
)


def solr_all_noam_types_clause() -> str:
    """Build a `types_authority:(*c_x OR ...)` Solr clause covering all 4 NOAM buckets."""
    codes = [f"*{auth.split(':')[-1]}" for auth in NOAM_AUTHORITY_VALUES]
    return "types_authority:(" + " OR ".join(codes) + ")"


def label_for_authority(authority: str | None) -> str | None:
    """Return a short, human-readable, language-independent label for a dc.type authority.

    Uses the last "::"-segment of the canonical (English) dc.type string this
    authority maps to — e.g. "article-coar-types:c_2df8fbb1" -> "Research article",
    "book-coar-types:c_3248" -> "Book part or chapter". Unlike resolve_noam_type(),
    this covers every COAR type in types_authority_mapping (patents, datasets,
    theses, …), not just the 4 NOAM buckets — meant for a general "document type"
    filter/display rather than OA-monitor classification.

    Returns None if the authority is missing or unrecognized.
    """
    if not authority or not authority.strip():
        return None
    dc_type = _AUTHORITY_TO_DC_TYPE.get(authority.strip())
    if dc_type is None:
        return None
    segment = dc_type.split("::")[-1].strip()
    return segment[:1].upper() + segment[1:] if segment else None


def duckdb_authority_in_clause(column: str = "dc_type_authority") -> str:
    """Build a `{column} IN ('...', ...)` SQL clause covering all NOAM authorities.

    For filtering the already-harvested `enriched`/`harvest` DuckDB tables by
    NOAM scope (mirrors solr_all_noam_types_clause(), applied post-harvest
    instead of at query time).
    """
    values = ", ".join(f"'{auth}'" for auth in sorted(NOAM_AUTHORITY_VALUES))
    return f"{column} IN ({values})"

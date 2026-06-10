"""Publication business-logic helpers — OA status, author strength, source URLs."""

from __future__ import annotations

import re

import pandas as pd

_WEAK_STATUSES: frozenset[str] = frozenset({"hôte", "hors epfl", "étudiant"})
_WEAK_PERSONNEL_POSITIONS: frozenset[str] = frozenset(
    {
        "academic guest",
        "consultant",
        "doctoral assistant",
        "engineer",
        "external employee",
        "external student",
        "guest",
        "guest phd student",
        "lecturer",
        "postdoctoral researcher",
        "visiting professor",
    }
)
_NON_OPEN_LICENSES: frozenset[str] = frozenset({
    "elsevier-specific", "publisher-specific-oa", "implied-oa",
})


def is_weak(status: str | None, position: str | None) -> bool:
    """Return True when an EPFL author's affiliation is considered weak."""
    s = (status or "").strip().lower()
    p = (position or "").strip().lower()
    if not s or s in _WEAK_STATUSES:
        return True
    return s == "personnel" and (not p or p in _WEAK_PERSONNEL_POSITIONS)


def oa_text(row: dict) -> str:
    """Return a human-readable OA status string for a publication row."""
    is_oa = None if pd.isna(row.get("upw_is_oa")) else bool(row.get("upw_is_oa"))
    lic = str(row.get("upw_license") or "").lower().strip()
    if is_oa is None:
        return "—"
    if not is_oa:
        return "Non-OA"
    if lic in _NON_OPEN_LICENSES:
        return "Non-libre"
    return "OA"


def lic_text(lic) -> str:
    """Return a normalised licence label (e.g. 'CC-BY', 'Public Domain') or empty string."""
    l = str(lic or "").lower().strip()
    if l.startswith("cc-"):
        return l.upper()
    if l in ("public-domain", "pd"):
        return "Public Domain"
    return ""


def source_api_url(source: str, internal_id, doi) -> str | None:
    """Return a deep-link URL to the source record for a given source, internal ID, and DOI."""
    iid = str(internal_id).strip() if pd.notna(internal_id) and internal_id else None
    d_ = str(doi).strip() if pd.notna(doi) and doi else None

    if source == "crossref":
        key = iid or d_
        return f"https://api.crossref.org/works/{key}" if key else None
    if source == "openalex+crossref":
        key = iid or d_
        return f"https://api.openalex.org/works/https://doi.org/{key}" if key else None
    if source == "scopus":
        if iid:
            return f"https://www.scopus.com/record/display.uri?eid={iid}&origin=resultslist"
        if d_:
            doi_enc = d_.replace("/", "%2F")
            return f"https://www.scopus.com/results/results.url?s=DOI%28{doi_enc}%29&origin=searchbasic"
        return None
    if source == "wos":
        return f"https://www.webofscience.com/wos/woscc/full-record/{iid}" if iid else None
    if source == "zenodo":
        if iid and iid.isdigit():
            return f"https://zenodo.org/api/records/{iid}"
        if d_:
            m = re.search(r"zenodo\.(\d+)", d_)
            if m:
                return f"https://zenodo.org/api/records/{m.group(1)}"
        return None
    if source == "epo":
        return f"https://worldwide.espacenet.com/patent/search?q=pn%3D{iid}" if iid else None
    if source == "datacite":
        key = iid or d_
        return f"https://api.datacite.org/dois/{key}" if key else None
    if d_:
        return f"https://api.crossref.org/works/{d_}"
    return None

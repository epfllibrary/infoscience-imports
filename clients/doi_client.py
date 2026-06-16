"""Lightweight doi.org client for registration agency resolution."""

import requests
from utils import get_pipeline_logger

_logger = get_pipeline_logger("doi")

_RA_ENDPOINT = "https://doi.org/ra/"
_TIMEOUT = 10


def resolve_agencies(dois: list[str]) -> dict[str, str]:
    """Return a {doi: agency} mapping for a list of DOIs.

    Uses the doi.org/ra batch endpoint.  Unknown or failed DOIs are omitted.
    Agency values are normalised to lowercase ('crossref', 'datacite', ...).
    """
    if not dois:
        return {}

    clean = [d.strip().lstrip("https://doi.org/").lstrip("http://dx.doi.org/") for d in dois if d]
    clean = list(dict.fromkeys(clean))  # deduplicate, preserve order

    result: dict[str, str] = {}
    try:
        resp = requests.get(_RA_ENDPOINT + ",".join(clean), timeout=_TIMEOUT)
        resp.raise_for_status()
        for entry in resp.json() or []:
            doi = (entry.get("DOI") or "").lower()
            ra = (entry.get("RA") or "").lower()
            if doi and ra:
                result[doi] = ra
    except Exception as exc:
        _logger.warning("doi.org/ra batch lookup failed: %s", exc)

    return result


def resolve_agency(doi: str) -> str | None:
    """Return the registration agency for a single DOI, or None on failure."""
    mapping = resolve_agencies([doi])
    key = doi.strip().lower().lstrip("https://doi.org/").lstrip("http://dx.doi.org/")
    return mapping.get(key)

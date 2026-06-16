"""Gap analyzer: compare researcher's harvested publications against Infoscience.

For each researcher, this module:
  1. Fetches all Infoscience items linked to the researcher via CRIS relation
     (``RELATION.Person.researchoutputs`` + person UUID scope).
  2. Loads harvested publications from person_publications.
  3. Classifies each harvested publication using type-aware matching that mirrors
     data_pipeline/deduplicator.py rules:
       - DOI match (type-agnostic)        → ``in_infoscience``
       - preprint + published in IS (title/year) → ``superseded_preprint``
       - dataset vs non-dataset (title/year)     → different entities → ``missing_in_infoscience``
       - generic title+year match         → ``in_infoscience``
       - no match                         → ``missing_in_infoscience``
  4. Persists classifications to person_gaps and person_infoscience_outputs.
"""

import re
import string

from utils import get_pipeline_logger

logger = get_pipeline_logger("gap_analyzer")

_DOI_PREFIX_RE = re.compile(r"^https?://(?:dx\.)?doi\.org/", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NON_ALNUM_RE = re.compile(r"[^\w\s]")
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_TABLE = str.maketrans("", "", string.punctuation)


def _classify_type(dc_type: str | None) -> str:
    """Return 'preprint', 'dataset', or 'published' for a dc_type string.

    Mirrors the dc_type branch of mappings.classify_record_type
    (no ifs3_collection available for IS outputs or cached harvested pubs).
    """
    dc = str(dc_type or "")
    if dc == "text::preprint":
        return "preprint"
    if dc.startswith("dataset") or dc.startswith("software"):
        return "dataset"
    return "published"


def _normalize_doi(doi: str | None) -> str | None:
    """Return a bare, lowercase DOI, stripping any URL prefix.

    Mirrors the DOI normalisation used in the import pipeline's deduplicator
    and the ``itemidentifier_keyword`` index behaviour in the Infoscience API.
    Returns None for empty / None input.
    """
    if not doi or not doi.strip():
        return None
    bare = _DOI_PREFIX_RE.sub("", doi.strip()).lower()
    return bare or None


def _clean_title(title: str | None) -> str:
    """Normalise a publication title for fuzzy matching.

    Algorithm mirrors ``DataFrameProcessor.clean_title`` in
    ``data_pipeline/deduplicator.py``:
      1. Strip HTML tags
      2. Replace non-alphanumeric chars (except whitespace) with spaces
      3. Collapse runs of whitespace
      4. Lowercase
      5. Remove all punctuation
    """
    if not title:
        return ""
    t = _HTML_TAG_RE.sub("", title)
    t = _NON_ALNUM_RE.sub(" ", t)
    t = _WHITESPACE_RE.sub(" ", t).strip()
    t = t.lower()
    t = t.translate(_PUNCT_TABLE)
    return t


def _title_year_key(title: str | None, pub_year: str | None) -> str:
    """Composite matching key: clean_title + pub_year.

    Mirrors the ``title_pubyear_id`` key used by the pipeline deduplicator.
    Returns an empty string when both inputs are empty.
    """
    return _clean_title(title) + (pub_year or "")


class GapAnalyzer:
    """Compare a researcher's harvested publications against Infoscience outputs.

    Matching priority (same as the import pipeline):
      1. DOI match (normalised, case-insensitive, URL prefix stripped)
      2. Cleaned-title + pub-year match (fallback when DOI is absent)

    Args:
        db:             PipelineDB instance.
        dspace_client:  DSpaceClientWrapper instance (or None to use singleton).
    """

    def __init__(self, db, dspace_client=None):
        self._db = db

        if dspace_client is None:
            from clients.dspace_client_wrapper import DSpaceClientWrapper
            dspace_client = DSpaceClientWrapper()
        self._dspace = dspace_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_infoscience_outputs(
        self,
        sciper: str,
        person_uuid: str | None = None,
    ) -> list[dict]:
        """Fetch and cache Infoscience outputs linked to a researcher.

        Uses the ``RELATION.Person.researchoutputs`` discovery configuration
        (more precise than a free-text author-authority query — follows the
        explicit CRIS authorship link, not text metadata).

        If ``person_uuid`` is None, the DSpace person UUID is resolved first
        via ``find_person('epfl.sciperId:{sciper}')``.

        Returns the list of output dicts (uuid, doi, title, pub_year, dc_type, handle).
        Returns [] if the person is not found in DSpace or on any error.
        """
        if person_uuid is None:
            logger.debug("Resolving DSpace UUID for sciper %s …", sciper)
            person_record = self._dspace.find_person(f"epfl.sciperId:{sciper}")
            if not person_record:
                logger.warning("sciper %s — no DSpace person record found, skipping IS fetch", sciper)
                return []
            person_uuid = person_record["uuid"]
            logger.debug("sciper %s — DSpace UUID: %s", sciper, person_uuid)

        logger.info("sciper %s — fetching Infoscience publications (uuid %s) …", sciper, person_uuid)
        try:
            outputs = self._dspace.fetch_person_publications(person_uuid)
        except Exception as exc:
            logger.warning(
                "sciper %s (uuid %s) — error fetching Infoscience publications: %s",
                sciper, person_uuid, exc,
            )
            return []

        n_with_doi = sum(1 for o in outputs if o.get("doi"))
        logger.info(
            "sciper %s — %d Infoscience output(s) fetched (%d with DOI)",
            sciper, len(outputs), n_with_doi,
        )

        for out in outputs:
            self._db.upsert_person_infoscience_output(
                sciper=sciper,
                infoscience_uuid=out["uuid"],
                doi=out.get("doi"),
                title=out.get("title"),
                pub_year=out.get("pub_year"),
                dc_type=out.get("dc_type"),
                handle=out.get("handle"),
            )

        return outputs

    def analyze(
        self,
        sciper: str,
        start_year: int | None = None,
        end_year: int | None = None,
        use_cached: bool = False,
    ) -> dict:
        """Classify harvested publications as in_infoscience / missing_in_infoscience.

        Matching logic mirrors the import pipeline (data_pipeline/deduplicator.py):
          - Primary key: normalised DOI
          - Fallback key: cleaned_title + pub_year (same algorithm as pipeline)

        Args:
            sciper:     Researcher SCIPER.
            start_year: Filter person_publications to this start year (inclusive).
            end_year:   Filter person_publications to this end year (inclusive).
            use_cached: If True, skip fetching Infoscience outputs (use DB cache).

        Returns a summary dict: {sciper, total, in_infoscience, missing}
        """
        year_range = (
            f"{start_year or '…'}–{end_year or '…'}"
            if (start_year or end_year) else "all years"
        )
        logger.info(
            "sciper %s — analyze start [%s, use_cached=%s]",
            sciper, year_range, use_cached,
        )

        if not use_cached:
            self.fetch_infoscience_outputs(sciper)

        infoscience_outputs = self._db.get_person_infoscience_outputs(sciper)
        person_pubs = self._db.get_person_publications(sciper, start_year, end_year)

        logger.info(
            "sciper %s — %d harvested pub(s), %d Infoscience output(s) in cache",
            sciper, len(person_pubs), len(infoscience_outputs),
        )

        # DOI index (type-agnostic — same DOI = same work).
        infoscience_dois: set[str] = set()
        # Title+year index: key → set of type categories present in IS.
        # Preserves type multiplicity for the type-aware matching rules below.
        infoscience_title_year_types: dict[str, set[str]] = {}
        for out in infoscience_outputs:
            norm_doi = _normalize_doi(out.get("doi"))
            if norm_doi:
                infoscience_dois.add(norm_doi)
            key = _title_year_key(out.get("title"), out.get("pub_year"))
            if key:
                t = _classify_type(out.get("dc_type"))
                infoscience_title_year_types.setdefault(key, set()).add(t)

        count_doi = 0
        count_title = 0
        count_superseded = 0
        count_missing = 0

        for pub in person_pubs:
            norm_doi       = _normalize_doi(pub.get("doi"))
            norm_doi_canon = _normalize_doi(pub.get("doi_canonical"))
            title_key      = _title_year_key(pub.get("title"), pub.get("pub_year"))
            harvested_type = _classify_type(pub.get("dc_type"))

            # Match on either the stored DOI or its canonical (base) form so that
            # versioned DOIs (eLife *.2, F1000 *.1) match IS records holding the base.
            doi_matched = bool(
                (norm_doi and norm_doi in infoscience_dois)
                or (norm_doi_canon and norm_doi_canon in infoscience_dois)
            )
            is_types = infoscience_title_year_types.get(title_key) if title_key else None

            if doi_matched:
                # DOI match is type-agnostic (same DOI = same work regardless of type).
                gap_status = "in_infoscience"
                count_doi += 1

            elif is_types is not None:
                # Type-aware title+year matching — mirrors deduplicator._dedup_by_title_year:
                # • preprint × published → preprint superseded (keep published only)
                # • dataset × non-dataset → different entities (neither counts as match)
                if harvested_type == "preprint" and "published" in is_types:
                    gap_status = "superseded_preprint"
                    count_superseded += 1
                    logger.debug(
                        "sciper %s — superseded preprint: %r (%s)",
                        sciper, (pub.get("title") or "")[:60], pub.get("pub_year") or "?",
                    )
                elif harvested_type == "dataset" and "dataset" not in is_types:
                    # Dataset vs only non-datasets in IS: different entities — dataset still missing.
                    gap_status = "missing_in_infoscience"
                    count_missing += 1
                    logger.debug(
                        "sciper %s — missing (dataset/non-dataset entity split): %r (%s)",
                        sciper, (pub.get("title") or "")[:60], pub.get("pub_year") or "?",
                    )
                elif harvested_type != "dataset" and is_types == {"dataset"}:
                    # Non-dataset vs only datasets in IS: different entities — still missing.
                    gap_status = "missing_in_infoscience"
                    count_missing += 1
                    logger.debug(
                        "sciper %s — missing (non-dataset vs dataset-only in IS): %r (%s)",
                        sciper, (pub.get("title") or "")[:60], pub.get("pub_year") or "?",
                    )
                else:
                    gap_status = "in_infoscience"
                    count_title += 1

            else:
                gap_status = "missing_in_infoscience"
                count_missing += 1
                logger.debug(
                    "sciper %s — missing: %r (%s)",
                    sciper, (pub.get("title") or "")[:60], pub.get("pub_year") or "?",
                )

            self._db.upsert_person_gap(
                sciper=sciper,
                pub_id=pub["pub_id"],
                gap_status=gap_status,
                doi=pub.get("doi"),
                title=pub.get("title"),
                pub_year=pub.get("pub_year"),
                dc_type=pub.get("dc_type"),
            )

        count_in = count_doi + count_title
        self._db.update_researcher_last_gap_analysis(sciper)

        logger.info(
            "sciper %s — result: %d total | %d in IS (DOI: %d, title-year: %d) "
            "| %d superseded preprint | %d missing",
            sciper, len(person_pubs), count_in, count_doi, count_title,
            count_superseded, count_missing,
        )

        return {
            "sciper":              sciper,
            "total":               len(person_pubs),
            "in_infoscience":      count_in,
            "superseded_preprint": count_superseded,
            "missing":             count_missing,
        }

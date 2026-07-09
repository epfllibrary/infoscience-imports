"""Harvest publications for a single researcher from OpenAlex and ORCID.

Produces normalized records compatible with the person_publications schema.

Dedup pipeline (see _dedup for full detail):
  Pass 1+2 — DOI-base merge: normalise DOIs (strip .<N> version suffix for known
             publishers), prefer the canonical DOI as primary, backfill metadata.
  Pass 3   — Title+year fallback: no-DOI records merged into DOI-having records;
             records with a DOI always win as primary.
  Pass 4   — Preprint suppression: preprint dropped when published version shares
             title+year; surviving record gains has_preprint_version=True.
"""

import json
import re
import string

import mappings
from utils import get_pipeline_logger

logger = get_pipeline_logger("person_harvester")

_OPENALEX_DATE_FMT = "{year}-01-01"
_OPENALEX_DATE_FMT_END = "{year}-12-31"

# Loaded from config/mappings/doctypes.yaml at import time.
# Keys are doc-type strings; values are dicts with "dc.type" and "collection".
_ORCID_MAPPING: dict = mappings.doctypes_mapping_dict.get("source_orcid", {})
_OPENALEX_MAPPING: dict = mappings.doctypes_mapping_dict.get("source_openalex", {})


def _orcid_type_entry(raw_type: str) -> dict | None:
    """Return the YAML entry for a raw ORCID type, or None if unknown."""
    return _ORCID_MAPPING.get(raw_type.upper() if raw_type else "")


def _openalex_type_entry(oa_native_type: str) -> dict | None:
    """Return the YAML entry for a native OpenAlex type, or None if unknown."""
    return _OPENALEX_MAPPING.get((oa_native_type or "").lower())


# Batch size for DOI-based OpenAlex enrichment of ORCID records.
_ENRICH_BATCH_SIZE = 50

# ── DOI versioning ────────────────────────────────────────────────────────────
# Publishers that append a version number as .<N> to their DOIs.
# Allowlist prevents stripping legitimate publisher-ID suffixes that happen
# to end in 1–2 digits (e.g. 10.1097/01.hjh.0001196048.03502.12).
_VERSIONED_NUMERIC_PREFIXES: tuple[str, ...] = (
    "10.7554/",   # eLife: *.1, *.2, *.3
    "10.7490/",   # F1000Research: *.1
    "10.52843/",  # Cassyni: *.2
)
_VERSIONED_NUMERIC_RE = re.compile(r"^(.+)\.(\d{1,2})$")

# Figshare .vN: 10.6084/m9.figshare.X.v1 -> base 10.6084/m9.figshare.X, version 1
_VERSIONED_V_PREFIXES: tuple[str, ...] = ("10.6084/",)
_VERSIONED_V_RE = re.compile(r"^(.+)\.v(\d+)$")

# Wiley bilingual: 10.1002/ange.X (German) -> 10.1002/anie.X (English, canonical)
_WILEY_ANGE_RE = re.compile(r"10\.1002/ange\.", re.IGNORECASE)


def _doi_base(doi: str) -> str:
    """Return the normalised group key for dedup indexing (lowercase, no version suffix).

    Does NOT determine which DOI wins; use _doi_version() to pick the most recent.
    """
    low = doi.lower()
    if _WILEY_ANGE_RE.match(low):
        low = low.replace("/ange.", "/anie.", 1)
    m = _VERSIONED_NUMERIC_RE.match(low)
    if m and any(low.startswith(p) for p in _VERSIONED_NUMERIC_PREFIXES):
        return m.group(1)
    mv = _VERSIONED_V_RE.match(low)
    if mv and any(low.startswith(p) for p in _VERSIONED_V_PREFIXES):
        return mv.group(1)
    return low


def _doi_version(doi: str) -> int:
    """Return the version number embedded in a versioned DOI, or 0 if unversioned.

    Higher version = more recent = preferred when merging duplicates.
      eLife.X.2 -> 2,  figshare.X.v3 -> 3,  10.1234/art -> 0
    """
    low = doi.lower()
    m = _VERSIONED_NUMERIC_RE.match(low)
    if m and any(low.startswith(p) for p in _VERSIONED_NUMERIC_PREFIXES):
        return int(m.group(2))
    mv = _VERSIONED_V_RE.match(low)
    if mv and any(low.startswith(p) for p in _VERSIONED_V_PREFIXES):
        return int(mv.group(2))
    return 0


# Title normalisation (mirrors gap_analyzer)
_HTML_TAG_RE   = re.compile(r"<[^>]+")
_NON_ALNUM_RE  = re.compile(r"[^\w\s]")
_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_TABLE   = str.maketrans("", "", __import__("string").punctuation)


def _clean_title(title: str | None) -> str:
    """Normalise a title for exact-match dedup (mirrors gap_analyzer._clean_title)."""
    if not title:
        return ""
    t = _HTML_TAG_RE.sub("", title)
    t = _NON_ALNUM_RE.sub(" ", t)
    t = _WHITESPACE_RE.sub(" ", t).strip().lower()
    return t.translate(_PUNCT_TABLE)


def _title_year_key(title: str | None, pub_year: str | None) -> str:
    return _clean_title(title) + (pub_year or "")


def _classify_type(dc_type: str | None) -> str:
    """Return 'preprint', 'dataset', or 'published' (mirrors gap_analyzer)."""
    dc = str(dc_type or "")
    if dc == "text::preprint":
        return "preprint"
    if dc.startswith("dataset") or dc.startswith("software"):
        return "dataset"
    return "published"


def _merge_into(primary: dict, secondary: dict) -> None:
    """Merge secondary metadata into primary (in-place).

    Priority rules:
    - DOI: keep the shorter (base/canonical) one; prefer having any DOI over none.
    - Scalar fields (title, journal_title, dc_type, pub_year): backfill from
      secondary only when primary is absent.
    - Boolean flags: OR-merge (any True source → True).
    - sources_found: union, preserving insertion order.
    - metadata_quality: upgrade to 'full' if secondary is 'full'.
    """
    # DOI selection: prefer the record with a DOI; when both have one,
    # prefer the most recent version (highest _doi_version number).
    # eLife.X.2 supersedes eLife.X; figshare.X.v2 supersedes .v1.
    # Equal versions (both 0 or same N): primary keeps its DOI.
    sec_doi = secondary.get("doi")
    pri_doi = primary.get("doi")
    if sec_doi:
        if not pri_doi:
            primary["doi"]    = sec_doi
            primary["pub_id"] = sec_doi
        elif _doi_version(sec_doi) > _doi_version(pri_doi):
            primary["doi"]    = sec_doi
            primary["pub_id"] = sec_doi

    # Backfill absent scalar fields
    for field in ("title", "journal_title", "dc_type", "pub_year"):
        if not primary.get(field) and secondary.get(field):
            primary[field] = secondary[field]

    # OR-merge boolean flags
    for flag in ("orcid_infoscience_synced", "has_preprint_version"):
        if secondary.get(flag):
            primary[flag] = True

    # Union sources_found (preserves order, deduplicates)
    existing = json.loads(primary.get("sources_found") or "[]")
    incoming = json.loads(secondary.get("sources_found") or "[]")
    primary["sources_found"] = json.dumps(list(dict.fromkeys(existing + incoming)))

    # Upgrade quality
    if secondary.get("metadata_quality") == "full":
        primary["metadata_quality"] = "full"


class PersonHarvester:
    """Harvest publications for one researcher from OpenAlex and/or ORCID.

    Args:
        openalex_client: Instance of clients.openalex_client.Client.
                         If None, the module-level singleton is used.
        orcid_client:    Instance of clients.orcid_client.Client.
                         If None, the module-level singleton is used.
    """

    def __init__(self, openalex_client=None, orcid_client=None):
        if openalex_client is None:
            from clients.openalex_client import OpenAlexClient
            openalex_client = OpenAlexClient
        if orcid_client is None:
            from clients.orcid_client import OrcidClient
            orcid_client = OrcidClient
        self._openalex = openalex_client
        self._orcid = orcid_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def harvest_openalex(
        self,
        openalex_id: str | None = None,
        orcid_id: str | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[dict]:
        """Return normalized publication records for an OpenAlex author ID and/or ORCID.

        Makes one fetch_records call per available identifier:
          - author.id:{openalex_id}   when openalex_id is provided
          - author.orcid:{orcid_id}   when orcid_id is provided

        Results from both calls are combined before normalization; downstream
        dedup (in harvest()) handles any DOI-level overlap between the two.
        """
        year_filters: list[str] = []
        if start_year:
            year_filters.append(f"from_publication_date:{_OPENALEX_DATE_FMT.format(year=start_year)}")
        if end_year:
            year_filters.append(f"to_publication_date:{_OPENALEX_DATE_FMT_END.format(year=end_year)}")

        raw: list[dict] = []

        if openalex_id:
            logger.debug("OpenAlex harvest — author_id=%s year=%s–%s", openalex_id, start_year or "*", end_year or "*")
            fetched = self._openalex.fetch_records(
                format="digest-ifs3",
                filter=",".join([f"author.id:{openalex_id}"] + year_filters),
            )
            raw.extend(fetched or [])

        if orcid_id:
            logger.debug("OpenAlex harvest — author.orcid=%s year=%s–%s", orcid_id, start_year or "*", end_year or "*")
            fetched = self._openalex.fetch_records(
                format="digest-ifs3",
                filter=",".join([f"author.orcid:{orcid_id}"] + year_filters),
            )
            raw.extend(fetched or [])

        normalized = [self._normalize_openalex(r) for r in raw]
        valid = [r for r in normalized if r.get("pub_id")]
        rejected = [r for r in normalized if not r.get("pub_id")]

        type_counts: dict[str, int] = {}
        for r in valid:
            t = r.get("dc_type") or "unknown"
            type_counts[t] = type_counts.get(t, 0) + 1
        rej_counts: dict[str, int] = {}
        for r in rejected:
            t = r.get("_rejected_type") or "unknown"
            rej_counts[t] = rej_counts.get(t, 0) + 1
        logger.info(
            "OpenAlex harvest — author_id=%s orcid=%s: %d fetched → %d valid, %d rejected | "
            "types: %s | rejected: %s",
            openalex_id or "—", orcid_id or "—", len(normalized), len(valid), len(rejected),
            ", ".join(f"{t}×{n}" for t, n in sorted(type_counts.items())) or "—",
            ", ".join(f"{t}×{n}" for t, n in sorted(rej_counts.items())) or "—",
        )
        return valid

    def harvest_orcid(
        self,
        orcid_id: str,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[dict]:
        """Return normalized publication records for a single ORCID iD."""
        logger.debug(
            "ORCID harvest start — orcid=%s year=%s–%s",
            orcid_id, start_year or "*", end_year or "*",
        )
        works = self._orcid.fetch_works_by_orcid(
            orcid_id, start_year=start_year, end_year=end_year
        )
        normalized = [self._normalize_orcid(w) for w in (works or [])]
        valid = [r for r in normalized if r.get("pub_id")]
        rejected = [r for r in normalized if not r.get("pub_id")]

        synced_count = sum(1 for r in valid if r.get("orcid_infoscience_synced"))
        with_doi = sum(1 for r in valid if r.get("doi"))
        type_counts: dict[str, int] = {}
        for r in valid:
            t = r.get("dc_type") or "unknown"
            type_counts[t] = type_counts.get(t, 0) + 1
        rej_counts: dict[str, int] = {}
        for r in rejected:
            t = r.get("_rejected_type") or "unknown"
            rej_counts[t] = rej_counts.get(t, 0) + 1
        logger.info(
            "ORCID harvest — orcid=%s: %d fetched → %d valid (%d with DOI, %d IS-synced), "
            "%d rejected | types: %s | rejected: %s",
            orcid_id, len(normalized), len(valid), with_doi, synced_count, len(rejected),
            ", ".join(f"{t}×{n}" for t, n in sorted(type_counts.items())) or "—",
            ", ".join(f"{t}×{n}" for t, n in sorted(rej_counts.items())) or "—",
        )
        return valid

    def harvest(
        self,
        researcher: dict,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[dict]:
        """Harvest from all available sources for one researcher dict.

        researcher must have keys: 'openalex_id', 'orcid' (either may be None).
        OpenAlex is harvested before ORCID so it wins dedup as primary source.
        ORCID records with a DOI are enriched via OpenAlex DOI lookup when the
        OpenAlex client is available, upgrading type and journal metadata.
        """
        sciper = researcher.get("sciper", "?")
        openalex_id = researcher.get("openalex_id")
        orcid = researcher.get("orcid")

        logger.info(
            "Harvest start — sciper=%s openalex=%s orcid=%s year=%s–%s",
            sciper, openalex_id or "—", orcid or "—",
            start_year or "*", end_year or "*",
        )

        oa_records: list[dict] = []
        orcid_records: list[dict] = []

        if openalex_id or orcid:
            oa_records = self.harvest_openalex(
                openalex_id=openalex_id,
                orcid_id=orcid,
                start_year=start_year,
                end_year=end_year,
            )

        if orcid:
            orcid_records = self.harvest_orcid(orcid, start_year, end_year)
            if orcid_records:
                orcid_records = self._enrich_orcid_via_openalex(orcid_records)

        combined = oa_records + orcid_records
        deduped = self._dedup(combined)

        n_with_doi      = sum(1 for r in deduped if r.get("doi"))
        n_multi_source  = sum(1 for r in deduped if len(json.loads(r.get("sources_found") or "[]")) > 1)
        n_preprint_sup  = sum(1 for r in deduped if r.get("has_preprint_version"))
        logger.info(
            "Harvest done — sciper=%s: %d OA + %d ORCID = %d combined → %d after dedup "
            "(%d merged, %d multi-source, %d with DOI, %d preprint suppressed)",
            sciper, len(oa_records), len(orcid_records), len(combined), len(deduped),
            len(combined) - len(deduped), n_multi_source, n_with_doi, n_preprint_sup,
        )
        return deduped

    # ------------------------------------------------------------------
    # Normalisation
    # ------------------------------------------------------------------

    def _normalize_openalex(self, r: dict) -> dict:
        doi = r.get("doi") or None
        if isinstance(doi, str) and not doi.strip():
            doi = None
        openalex_id = r.get("openalex_id") or None
        pub_id = doi if doi else (f"openalex:{openalex_id}" if openalex_id else None)
        pub_year = r.get("pubyear")

        # Always resolve dc_type from the native OpenAlex type field.
        # The client also returns a pre-computed dc.type via the Crossref mapping when
        # type_crossref is present, but that mapping is deprecated by OpenAlex (2024).
        # Using openalex_type directly avoids the crossref/openalex divergence.
        oa_native = r.get("openalex_type") or ""
        dc_type = None
        if oa_native:
            entry = _openalex_type_entry(oa_native)
            if entry is None:
                logger.debug("OpenAlex: unknown type %r — skipping pub_id=%s", oa_native, pub_id)
                return {"pub_id": None, "_rejected_type": oa_native}
            if entry.get("rejected"):
                logger.debug("OpenAlex: rejected type %r — skipping pub_id=%s", oa_native, pub_id)
                return {"pub_id": None, "_rejected_type": oa_native}
            dc_type = entry.get("dc.type")

        return {
            "pub_id":                  pub_id,
            "doi":                     doi,
            "title":                   r.get("title") or None,
            "pub_year":                str(pub_year) if pub_year is not None else None,
            "dc_type":                 dc_type,
            "journal_title":           r.get("primary_container_title") or None,
            "sources_found":           json.dumps(["openalex"]),
            "primary_source":          "openalex",
            "metadata_quality":        "full" if doi else "partial",
            "openalex_id":             openalex_id,
            "orcid_infoscience_synced": False,
        }

    def _normalize_orcid(self, w: dict) -> dict:
        doi = w.get("doi") or None
        put_code = w.get("put_code")
        pub_id = doi if doi else (f"orcid:{put_code}" if put_code is not None else None)

        raw_type = w.get("type") or ""
        dc_type = None
        if raw_type:
            entry = _orcid_type_entry(raw_type)
            if entry is None:
                logger.debug("ORCID: unknown type %r — skipping put_code=%s", raw_type, put_code)
                return {"pub_id": None, "_rejected_type": raw_type}
            if entry.get("rejected"):
                logger.debug("ORCID: rejected type %r — skipping put_code=%s", raw_type, put_code)
                return {"pub_id": None, "_rejected_type": raw_type}
            dc_type = entry.get("dc.type")

        return {
            "pub_id":                  pub_id,
            "doi":                     doi,
            "title":                   w.get("title") or None,
            "pub_year":                w.get("pub_year") or None,
            "dc_type":                 dc_type,
            "journal_title":           w.get("journal") or None,
            "sources_found":           json.dumps(["orcid"]),
            "primary_source":          "orcid",
            "metadata_quality":        "full" if doi else "partial",
            "orcid_infoscience_synced": bool(w.get("infoscience_synced", False)),
            "source_type":             w.get("source_type"),
            "all_sources":             w.get("all_sources"),
            "_orcid_raw_type":         raw_type,
        }

    # ------------------------------------------------------------------
    # ORCID enrichment via OpenAlex DOI lookup
    # ------------------------------------------------------------------

    def _enrich_orcid_via_openalex(self, records: list[dict]) -> list[dict]:
        """Upgrade ORCID records that have a DOI with OpenAlex metadata.

        Batches DOI lookups to minimise API calls. Updates dc_type, journal_title,
        and title in-place when OpenAlex provides richer data.
        """
        doi_to_idx: dict[str, int] = {}
        for i, r in enumerate(records):
            doi = r.get("doi")
            if doi:
                doi_to_idx[doi] = i

        if not doi_to_idx:
            logger.debug("ORCID enrichment: no DOIs available, skipping OpenAlex lookup")
            return records

        dois = list(doi_to_idx.keys())
        oa_by_doi: dict[str, dict] = {}

        for i in range(0, len(dois), _ENRICH_BATCH_SIZE):
            chunk = dois[i: i + _ENRICH_BATCH_SIZE]
            try:
                fetched = self._openalex.fetch_records(
                    format="digest-ifs3",
                    filter=f"doi:{'|'.join(chunk)}",
                    per_page=len(chunk),
                ) or []
                for oa in fetched:
                    oa_doi = oa.get("doi")
                    if oa_doi:
                        oa_by_doi[oa_doi] = oa
                logger.debug(
                    "ORCID enrichment batch %d–%d: %d DOIs queried, %d found in OpenAlex",
                    i, i + len(chunk), len(chunk), len(oa_by_doi),
                )
            except Exception as exc:
                logger.warning("ORCID enrichment OpenAlex batch failed (offset %d): %s", i, exc)

        enriched = 0
        type_upgraded = 0
        for doi, idx in doi_to_idx.items():
            oa = oa_by_doi.get(doi)
            if not oa:
                continue
            rec = records[idx]

            oa_native = oa.get("openalex_type") or ""
            oa_dc_type = None
            if oa_native:
                oa_entry = _openalex_type_entry(oa_native)
                if oa_entry and not oa_entry.get("rejected"):
                    oa_dc_type = oa_entry.get("dc.type")
            if oa_dc_type:
                if rec.get("dc_type") != oa_dc_type:
                    type_upgraded += 1
                rec["dc_type"] = oa_dc_type

            if not rec.get("journal_title"):
                oa_journal = oa.get("primary_container_title")
                if oa_journal:
                    rec["journal_title"] = oa_journal

            if not rec.get("title"):
                oa_title = oa.get("title")
                if oa_title:
                    rec["title"] = oa_title

            rec["metadata_quality"] = "full"
            enriched += 1

        logger.info(
            "ORCID enrichment: %d/%d DOI records enriched via OpenAlex (%d type upgrades)",
            enriched, len(dois), type_upgraded,
        )
        return records

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _dedup(self, records: list[dict]) -> list[dict]:
        """Four-pass dedup: DOI-base merge → title+year fallback → preprint suppression.

        Pass 1+2 — DOI-base merge:
            Normalise each DOI to its canonical base (strip version suffix .<N> for
            known publishers).  When two records share a base DOI, call _merge_into
            so the record with the shorter/canonical DOI survives as primary.
            Records without a DOI proceed to Pass 3.

        Pass 3 — Title+year fallback:
            No-DOI records are merged into DOI-having records that share the same
            cleaned-title + pub_year key.  Remaining no-DOI records are deduped
            amongst themselves by the same key.  Datasets are excluded (same title
            ≠ same work across types).

        Pass 4 — Preprint suppression:
            When a preprint and a published record share the same title+year key,
            the preprint is dropped and the surviving record gains
            has_preprint_version=True.  orcid_infoscience_synced from the preprint
            is carried over via _merge_into so the flag is not lost.
        """
        # ── Pass 1+2: DOI-base merge ──────────────────────────────────────────
        doi_index: dict[str, int] = {}  # doi_base → index in doi_out
        doi_out: list[dict] = []

        for rec in records:
            doi = rec.get("doi")
            doi_key = _doi_base(doi) if doi else None

            if doi_key and doi_key in doi_index:
                idx = doi_index[doi_key]
                existing = doi_out[idx]
                # Prefer canonical (shorter) DOI as primary.
                # If incoming carries the base DOI while existing holds the versioned
                # form, swap so the base survives and versioned metadata backfills it.
                # Swap primary when incoming has a higher version number.
                # _merge_into will then promote the versioned DOI via _doi_version.
                if _doi_version(doi or "") > _doi_version(existing.get("doi") or ""):
                    _merge_into(rec, existing)
                    doi_out[idx] = rec
                else:
                    _merge_into(existing, rec)
            else:
                if doi_key:
                    doi_index[doi_key] = len(doi_out)
                # Store the canonical DOI alongside the original so gap_analyzer
                # can check both forms when matching against Infoscience.
                if doi and doi_key and doi_key != doi.lower():
                    rec["doi_canonical"] = doi_key
                doi_out.append(rec)

        # ── Pass 3: Title+year fallback for no-DOI records ───────────────────
        has_doi  = [r for r in doi_out if r.get("doi")]
        no_doi   = [r for r in doi_out if not r.get("doi")]

        # Index DOI records by title+year (non-datasets only)
        ty_doi_index: dict[str, int] = {}
        for i, r in enumerate(has_doi):
            if _classify_type(r.get("dc_type")) == "dataset":
                continue
            key = _title_year_key(r.get("title"), r.get("pub_year"))
            if key:
                ty_doi_index.setdefault(key, i)

        # Merge no-DOI records into matching DOI records; collect orphans.
        #
        # Special case — published (no-DOI) vs preprint (with DOI):
        #   The DOI-preference rule would normally keep the preprint as primary
        #   since it has a DOI, but published > preprint.  Swap: the published
        #   record becomes primary; the preprint DOI is saved as doi_canonical
        #   so gap_analyzer can still match it against IS.
        orphans: list[dict] = []
        for r in no_doi:
            if _classify_type(r.get("dc_type")) == "dataset":
                orphans.append(r)
                continue
            key = _title_year_key(r.get("title"), r.get("pub_year"))
            if not key or key not in ty_doi_index:
                orphans.append(r)
                continue

            idx = ty_doi_index[key]
            target = has_doi[idx]
            r_type = _classify_type(r.get("dc_type"))
            t_type = _classify_type(target.get("dc_type"))

            if r_type != "preprint" and t_type == "preprint":
                # Published (no-DOI) beats DOI-preprint: swap primary.
                # Carry the preprint DOI as doi_canonical so gap_analyzer can
                # still match IS records that hold the arxiv/preprint DOI.
                preprint_doi = target.get("doi")
                r["doi_canonical"] = preprint_doi
                r["has_preprint_version"] = True
                _merge_into(r, target)
                # _merge_into sets doi from secondary when primary has none;
                # undo that — the published record intentionally has no DOI.
                r["doi"] = None
                r["pub_id"] = r.get("pub_id") or preprint_doi
                has_doi[idx] = r
            else:
                _merge_into(target, r)
                if r_type == "preprint" and t_type != "preprint":
                    target["has_preprint_version"] = True

        # Dedup orphaned no-DOI records amongst themselves
        orphan_index: dict[str, int] = {}
        deduped_orphans: list[dict] = []
        for r in orphans:
            if _classify_type(r.get("dc_type")) == "dataset":
                deduped_orphans.append(r)
                continue
            key = _title_year_key(r.get("title"), r.get("pub_year"))
            if key and key in orphan_index:
                _merge_into(deduped_orphans[orphan_index[key]], r)
            else:
                if key:
                    orphan_index[key] = len(deduped_orphans)
                deduped_orphans.append(r)

        combined = has_doi + deduped_orphans

        # ── Pass 4: Preprint suppression ──────────────────────────────────────
        # Build title+year index of published records
        published_index: dict[str, int] = {}
        for i, r in enumerate(combined):
            if _classify_type(r.get("dc_type")) != "preprint":
                key = _title_year_key(r.get("title"), r.get("pub_year"))
                if key:
                    published_index.setdefault(key, i)

        to_remove: set[int] = set()
        for i, r in enumerate(combined):
            if _classify_type(r.get("dc_type")) == "preprint":
                key = _title_year_key(r.get("title"), r.get("pub_year"))
                if key and key in published_index:
                    j = published_index[key]
                    _merge_into(combined[j], r)
                    combined[j]["has_preprint_version"] = True
                    to_remove.add(i)

        return [r for i, r in enumerate(combined) if i not in to_remove]

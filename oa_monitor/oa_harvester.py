"""M1 — Infoscience OA Harvester.

Queries DSpace-CRIS via the discovery API for all publications in a given
year range and returns a normalised DataFrame ready for OpenAlex/Unpaywall
enrichment and OA classification.

Metadata priority
-----------------
When a "main document" bitstream exists in the ORIGINAL bundle, its metadata
(datacite.rights, oaire.licenseCondition, oaire.version) takes precedence over
the record-level equivalents. Record-level values are used as fallback when
no main document bitstream is found.
"""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from clients.dspace_client_wrapper import DSpaceClientWrapper
from utils import get_pipeline_logger

logger = get_pipeline_logger("oa_harvester")

# DSpace metadata field → output column name (record-level)
_FIELD_MAP = {
    "dc.identifier.doi":        "doi",
    "dc.title":                 "title",
    "dc.date.issued":           "issued_raw",
    "dc.type":                  "dc_type",
    "datacite.rights":          "existing_access_level",
    "oaire.version":            "existing_version",
    "epfl.publication.version": "legacy_version",
    "oaire.licenseCondition":   "existing_license",
    "datacite.available":       "embargo",
    "dc.relation.journal":        "_journal",
    "dc.relation.ispartof":       "_ispartof",
    "dc.relation.ispartofseries": "_series",
    "dc.relation.issn":           "_rel_issn",
    "dc.identifier.issn":         "_id_issn",
    "dc.relation.serieissn":      "_serie_issn",
    "dc.identifier.isbn":         "_id_isbn",
    "dc.relation.isbn":           "_rel_isbn",
}

# Same fields at bitstream level → column to override
_BS_FIELD_MAP = {
    "datacite.rights":        "existing_access_level",
    "oaire.licenseCondition": "existing_license",
    "oaire.version":          "existing_version",
}

_ENTITY_FILTER = (
    "(entityType:(Publication) OR entityType:(Product) OR entityType:(Patent))"
)

_EMPTY_COLUMNS = [
    "infoscience_uuid", "handle", "doi", "title", "pubyear",
    "dc_type", "dc_type_authority", "existing_access_level", "existing_version",
    "legacy_version", "existing_license", "embargo",
    "journal", "publishedin", "series", "issn", "isbn", "authors",
]


class InfoscienceOAHarvester:
    """Harvest OA-relevant metadata for existing Infoscience items.

    Args:
        dspace: Optional DSpaceClientWrapper instance (injected for tests).
    """

    def __init__(self, dspace: DSpaceClientWrapper | None = None):
        self.dspace = dspace or DSpaceClientWrapper()

    def harvest(
        self,
        year_from: int,
        year_to: int,
        page_size: int = 100,
        extra_filter: str | None = None,
        max_items: int | None = None,
        with_bitstream_metadata: bool = True,
        bitstream_workers: int = 10,
    ) -> pd.DataFrame:
        """Return a DataFrame of Infoscience items published in [year_from, year_to].

        Output columns: infoscience_uuid, handle, doi, title, pubyear, dc_type,
        dc_type_authority, existing_access_level, existing_version, legacy_version,
        existing_license, embargo, journal, publishedin, series, issn, isbn, authors.
        publishedin is coalesced from dc.relation.journal > dc.relation.ispartof >
        dc.relation.ispartofseries; journal and series expose the raw source fields.
        dc_type_authority is the dc.type entry's COAR authority code (e.g.
        "article-coar-types:c_6501") — language-independent, unlike dc_type's
        free-text value. See oa_monitor/type_mapping.py for classification.

        Args:
            extra_filter: Optional Solr clause ANDed into the base query.
            max_items: Cap the result to N items (0 or None = no limit). Reduces
                the number of API pages fetched when set.
            with_bitstream_metadata: When True (default), fetch per-item bitstream
                metadata and override record-level fields for items that have a
                "main document" bitstream in their ORIGINAL bundle.
            bitstream_workers: Number of parallel threads for bitstream API calls.
        """
        effective_limit = int(max_items) if max_items else None
        max_pages = math.ceil(effective_limit / page_size) if effective_limit else 500

        query = (
            f"(dateIssued.year:[{year_from} TO {year_to}])"
            f" AND {_ENTITY_FILTER}"
        )
        if extra_filter and extra_filter.strip():
            query += f" AND ({extra_filter.strip()})"
        logger.info("  query : %s", query)
        logger.info("  config: researchoutputs  page_size=%d  max_pages=%d", page_size, max_pages)
        dsos = self.dspace._search_objects(
            query=query,
            page=0,
            size=page_size,
            dso_type="item",
            configuration="researchoutputs",
            max_pages=max_pages,
        )

        if effective_limit and len(dsos) > effective_limit:
            logger.info("  capped at %d items (--max-items)", effective_limit)
            dsos = dsos[:effective_limit]

        logger.info("  fetched %d items for %d–%d", len(dsos), year_from, year_to)

        rows = [self._extract(dso) for dso in dsos]

        if not rows:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        # Enrich with bitstream-level metadata (overrides record-level when main doc found)
        if with_bitstream_metadata:
            uuids = [r["infoscience_uuid"] for r in rows if r.get("infoscience_uuid")]
            bs_data = self._fetch_all_bitstream_metadata(uuids, workers=bitstream_workers)
            enriched = 0
            for row in rows:
                uuid = row.get("infoscience_uuid")
                bs = bs_data.get(uuid, {})
                if not bs.get("bs_has_main_doc"):
                    continue
                for col in _BS_FIELD_MAP.values():
                    # Only override when the bitstream provides a non-null value
                    bs_val = bs.get(f"bs_{col.removeprefix('existing_')}" if col.startswith("existing_") else f"bs_{col}")
                    if bs_val:
                        row[col] = bs_val
                enriched += 1
            logger.info("  bitstream metadata applied to %d/%d items", enriched, len(rows))

        df = pd.DataFrame(rows)
        df["pubyear"] = df.pop("issued_raw").apply(_parse_year)

        # Coalesce container fields — priority per field type:
        # publishedin: journal > book/proceedings > series
        # issn: journal ISSN > own ISSN > series ISSN
        # isbn: own ISBN > parent book ISBN
        df["journal"] = df.pop("_journal")
        df["series"] = df.pop("_series")
        df["publishedin"] = (
            df["journal"]
            .combine_first(df.pop("_ispartof"))
            .combine_first(df["series"])
        )
        df["issn"] = (
            df.pop("_rel_issn")
            .combine_first(df.pop("_id_issn"))
            .combine_first(df.pop("_serie_issn"))
        )
        df["isbn"] = df.pop("_id_isbn").combine_first(df.pop("_rel_isbn"))
        return df

    # ── Bitstream metadata ────────────────────────────────────────────────────

    def _fetch_all_bitstream_metadata(
        self, uuids: list[str], workers: int = 10
    ) -> dict[str, dict]:
        """Fetch main-document bitstream metadata for multiple items in parallel.

        Returns a dict mapping uuid → bitstream metadata dict as returned by
        _extract_main_doc_metadata().
        """
        results: dict[str, dict] = {}
        total = len(uuids)
        log_every = max(1, total // 10)  # ~10 progress lines regardless of total

        def _fetch_one(uuid: str) -> tuple[str, dict]:
            try:
                endpoint = self.dspace.client.API_ENDPOINT
                url = (
                    f"{endpoint}/core/items/{uuid}"
                    "?embed=bundles&embed=bundles/bitstreams"
                    "&embed=bundles/bitstreams/metadata"
                )
                r = self.dspace.client.api_get(url)
                if r.status_code != 200:
                    return uuid, {"bs_has_main_doc": False}
                return uuid, self._extract_main_doc_metadata(r.json())
            except Exception:
                return uuid, {"bs_has_main_doc": False}

        logger.info("  bitstream metadata: %d items to fetch (%d workers)…", total, workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_one, uuid): uuid for uuid in uuids}
            completed = 0
            for future in as_completed(futures):
                uuid, data = future.result()
                results[uuid] = data
                completed += 1
                if completed % log_every == 0 or completed == total:
                    logger.info("  bitstream metadata: %d/%d done…", completed, total)

        return results

    def _extract_main_doc_metadata(self, item_data: dict) -> dict:
        """Extract access/license/version from the main document bitstream in ORIGINAL bundle.

        Returns a dict with keys:
          bs_has_main_doc (bool)
          bs_access_level, bs_license, bs_version (str | None) — present when has_main_doc=True
        """
        for bundle in (
            item_data.get("_embedded", {})
                     .get("bundles", {})
                     .get("_embedded", {})
                     .get("bundles", [])
        ):
            if bundle.get("name") != "ORIGINAL":
                continue
            for bs in (
                bundle.get("_embedded", {})
                      .get("bitstreams", {})
                      .get("_embedded", {})
                      .get("bitstreams", [])
            ):
                bs_meta = bs.get("metadata") or {}
                is_main = any(
                    "main document" in str(v.get("value", "")).lower()
                    for v in bs_meta.get("dc.type", [])
                    if isinstance(v, dict)
                )
                if not is_main:
                    continue

                def _first(field: str) -> str | None:
                    entries = bs_meta.get(field, [])
                    v = entries[0].get("value", "").strip() if entries else ""
                    return v or None

                return {
                    "bs_has_main_doc": True,
                    "bs_access_level":  _first("datacite.rights"),
                    "bs_license":       _first("oaire.licenseCondition"),
                    "bs_version":       _first("oaire.version"),
                }

        return {"bs_has_main_doc": False}

    # ── Record-level extraction ───────────────────────────────────────────────

    def _extract(self, dso) -> dict:
        md: dict = getattr(dso, "metadata", {}) or {}

        row: dict = {
            "infoscience_uuid": getattr(dso, "uuid", None),
            "handle":           getattr(dso, "handle", None),
        }

        for field, col in _FIELD_MAP.items():
            entries = md.get(field, [])
            row[col] = entries[0]["value"].strip() if entries else None

        dc_type_entries = md.get("dc.type", [])
        row["dc_type_authority"] = (
            (dc_type_entries[0].get("authority") or "").strip() or None
            if dc_type_entries else None
        )

        author_entries = md.get("dc.contributor.author", [])
        row["authors"] = (
            " || ".join(e["value"].strip() for e in author_entries if e.get("value"))
            if author_entries else None
        )

        return row


def _parse_year(raw: str | None) -> str | None:
    """Extract 4-digit year from dc.date.issued values like '2023', '2023-05', '2023-05-15'."""
    if not raw:
        return None
    return str(raw).strip()[:4] or None

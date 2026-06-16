"""Sync the researcher registry from EPFL People API.

Builds and maintains researcher_registry in PipelineDB by:
  1. Fetching active accreditations from the EPFL API (class_id / position_id scope)
  2. Filtering affiliations to keep only allowed unit types
  3. Merging per-SCIPER (preferred unit = lowest unit_order)
  4. Batch-fetching ORCIDs from the persons?ids= endpoint
  5. Enriching external IDs and name variants from the ORCID API
  6. Enriching DSpace/Infoscience profile data (uuid, openalex_id, etc.)
  7. Upserting each researcher and their unit rows into the DB
  8. Offboarding researchers no longer present in the API response
"""

import json
import os
from datetime import datetime

from utils import get_pipeline_logger

logger = get_pipeline_logger("registry_sync")

_DEFAULT_CLASS_ID = os.environ.get("EPFL_ACCRED_CLASS_IDS", "5,6,10")
_DEFAULT_POSITION_ID = os.environ.get(
    "EPFL_ACCRED_POSITION_ID", "65,70,123,146,181,184,182,186,187,189,1456,256,208"
)
# EPFL accred status filter: 1 = internal staff, 2 = hosted researchers.
# Leave unset to return all statuses.
_DEFAULT_STATUS_ID = os.environ.get("EPFL_ACCRED_STATUS_ID", "1")

# Manual overrides: take precedence over all inferred / Infoscience-sourced IDs.
# Map sciper (str) → canonical OpenAlex URL.
_MANUAL_OPENALEX_CORRECTIONS: dict[str, str] = {
    "317488": "https://openalex.org/A5025196309",   # Mackenzie Mathis
    "318514": "https://openalex.org/A5013861606",   # Alexander Mathis
    "103938": "https://openalex.org/A5111484611",   # Maryline Andersen
    "357164": "https://openalex.org/A5027599933",   # Sara Bonetti
    "365328": "https://openalex.org/A5021953983",   # Andrea Cavalli
}


class RegistrySync:
    """Sync researcher_registry from EPFL People API.

    Args:
        db:            PipelineDB instance.
        epfl_client:   clients.api_epfl_client.Client instance (or singleton).
        orcid_client:  clients.orcid_client.Client instance (or singleton).
        dspace_client: clients.dspace_client_wrapper.DSpaceClientWrapper (optional).
    """

    def __init__(
        self,
        db,
        epfl_client=None,
        orcid_client=None,
        dspace_client=None,
        openalex_client=None,
    ):
        self._db = db

        if epfl_client is None:
            from clients.api_epfl_client import ApiEpflClient
            epfl_client = ApiEpflClient
        if orcid_client is None:
            from clients.orcid_client import OrcidClient
            orcid_client = OrcidClient
        if openalex_client is None:
            from clients.openalex_client import OpenAlexClient
            openalex_client = OpenAlexClient

        self._epfl = epfl_client
        self._orcid = orcid_client
        self._dspace = dspace_client
        self._openalex = openalex_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sync_all(
        self,
        class_id: str = _DEFAULT_CLASS_ID,
        position_id: str = _DEFAULT_POSITION_ID,
        status_id: str | None = _DEFAULT_STATUS_ID,
        enrich_orcid: bool = True,
        enrich_dspace: bool = True,
        enrich_openalex: bool = True,
    ) -> list[str]:
        """Sync all researchers matching class_id / position_id from EPFL API.

        Batch-fetches ORCIDs from the persons?ids= endpoint for all scipers at once.
        Returns the list of SCIPERs upserted (active set after sync).
        """
        logger.info(
            "sync_all started — class_id=%s position_id=%s status_id=%s "
            "orcid=%s dspace=%s openalex=%s",
            class_id, position_id, status_id, enrich_orcid, enrich_dspace, enrich_openalex,
        )

        rows, filtered = self._fetch_researchers(class_id, position_id, status_id)
        total = len(rows)
        logger.info("EPFL API: %d researchers fetched (%d unit rows)", total, len(filtered))

        scipers_active = {
            str(r.get("sciper") or "").strip()
            for r in rows if r.get("sciper")
        }

        units_by_sciper = self._build_unit_rows(filtered)

        orcid_map: dict[str, str] = {}
        if enrich_orcid and scipers_active:
            orcid_map = self._fetch_orcid_map(list(scipers_active))

        for idx, row in enumerate(rows, start=1):
            sciper = str(row.get("sciper") or "").strip()
            if not sciper:
                continue

            name = row.get("display") or sciper
            logger.info("[%d/%d] %s (%s)", idx, total, name, sciper)

            researcher = self._map_accred_to_registry(row)

            if enrich_orcid:
                orcid = orcid_map.get(sciper)
                if orcid:
                    researcher["orcid"] = orcid
                    researcher["orcid_epfl_linked"] = True
                    logger.info("  ORCID        : %s (persons batch)", orcid)
                else:
                    logger.info("  ORCID        : not found")

            if researcher.get("orcid"):
                ext = self._enrich_orcid_externals(researcher["orcid"])
                researcher.update(ext)
                parts = []
                if ext.get("scopus_author_id"):
                    parts.append(f"Scopus={ext['scopus_author_id']}")
                if ext.get("researcher_id"):
                    parts.append(f"WoS={ext['researcher_id']}")
                if ext.get("name_variants"):
                    parts.append("name_variants=yes")
                logger.info(
                    "  ORCID ext    : %s",
                    ", ".join(parts) if parts else "nothing new",
                )

            researcher["last_people_sync"] = datetime.now()
            self._db.upsert_researcher(**researcher)

            n_units = len(units_by_sciper.get(sciper, []))
            if sciper in units_by_sciper:
                self._db.upsert_researcher_units(sciper, units_by_sciper[sciper])
            logger.info("  Units        : %d unit row(s) upserted", n_units)

            if enrich_dspace:
                dspace_data = self._enrich_dspace_profile(sciper)
                if dspace_data:
                    self._db.patch_researcher(sciper, **dspace_data)
                    keys = [k for k, v in dspace_data.items() if v]
                    logger.info("  Infoscience  : profile found — %s", ", ".join(keys))
                else:
                    logger.info("  Infoscience  : no profile")

            if enrich_openalex:
                existing_oa, oa_in_is = self._db.get_researcher_openalex_data(sciper)
                _fn = (row.get("firstname") or "").strip()
                _ln = (row.get("lastname") or "").strip()
                _display = f"{_fn} {_ln}".strip() or row.get("display") or ""
                oa_data = self._enrich_openalex_id(
                    sciper,
                    _display,
                    existing_openalex_id=existing_oa,
                    openalex_in_infoscience=oa_in_is,
                    orcid=researcher.get("orcid"),
                )
                if oa_data:
                    oa_data["openalex_in_infoscience"] = False
                    self._db.patch_researcher(sciper, **oa_data)
                    logger.info("  OpenAlex     : %s (inferred)", oa_data.get("openalex_id"))
                else:
                    logger.info(
                        "  OpenAlex     : %s%s",
                        existing_oa if existing_oa else "not found",
                        " (from IS)" if oa_in_is else "",
                    )
                final_oa_id = oa_data.get("openalex_id") or existing_oa
                if final_oa_id:
                    oa_profile = self._enrich_openalex_author_profile(sciper, final_oa_id)
                    if oa_profile:
                        self._db.patch_researcher_if_null(sciper, **oa_profile)
                        logger.info(
                            "  OpenAlex profile: supplemented %s",
                            ", ".join(oa_profile.keys()),
                        )

        self._offboard_inactive(scipers_active)
        logger.info(
            "sync_all complete — %d active researchers, offboarding checked",
            len(scipers_active),
        )
        return list(scipers_active)

    def sync_one(
        self,
        sciper: str,
        enrich_orcid: bool = True,
        enrich_dspace: bool = True,
        enrich_openalex: bool = True,
    ) -> dict | None:
        """Sync a single researcher by SCIPER.

        Fetches ORCID from the persons?ids= endpoint when enrich_orcid=True.
        Returns the normalized researcher dict that was upserted, or None if
        the EPFL API returned no records for this SCIPER.
        """
        logger.info("sync_one started — sciper=%s orcid=%s dspace=%s openalex=%s",
                    sciper, enrich_orcid, enrich_dspace, enrich_openalex)

        records = self._epfl.fetch_accred_by_unique_id(sciper, format="person")
        if not records:
            logger.warning("No accred records for sciper %s", sciper)
            return None

        filtered = self._epfl.dedupe_people_keep_allowed_affiliations(records)
        merged = self._epfl.merge_records_by_sciper_with_preferred_unit(filtered)
        if not merged:
            logger.warning("No merged rows for sciper %s after affiliation filter", sciper)
            return None

        row = merged[0]
        name = row.get("display") or sciper
        logger.info("  EPFL People  : %s — unit=%s position=%s",
                    name, row.get("preferred_unit_name"), row.get("position"))
        researcher = self._map_accred_to_registry(row)

        if enrich_orcid:
            orcid_map = self._fetch_orcid_map([sciper])
            orcid = orcid_map.get(sciper)
            if orcid:
                researcher["orcid"] = orcid
                researcher["orcid_epfl_linked"] = True
                logger.info("  ORCID        : %s", orcid)
            else:
                logger.info("  ORCID        : not found")

        if researcher.get("orcid"):
            ext = self._enrich_orcid_externals(researcher["orcid"])
            researcher.update(ext)
            parts = []
            if ext.get("scopus_author_id"):
                parts.append(f"Scopus={ext['scopus_author_id']}")
            if ext.get("researcher_id"):
                parts.append(f"WoS={ext['researcher_id']}")
            if ext.get("name_variants"):
                parts.append("name_variants=yes")
            logger.info("  ORCID ext    : %s", ", ".join(parts) if parts else "nothing new")

        researcher["last_people_sync"] = datetime.now()
        self._db.upsert_researcher(**researcher)

        units_by_sciper = self._build_unit_rows(filtered)
        n_units = len(units_by_sciper.get(sciper, []))
        if sciper in units_by_sciper:
            self._db.upsert_researcher_units(sciper, units_by_sciper[sciper])
        logger.info("  Units        : %d unit row(s) upserted", n_units)

        if enrich_dspace:
            dspace_data = self._enrich_dspace_profile(sciper)
            if dspace_data:
                self._db.patch_researcher(sciper=sciper, **dspace_data)
                keys = [k for k, v in dspace_data.items() if v]
                logger.info("  Infoscience  : profile found (%s)", ", ".join(keys))
            else:
                logger.info("  Infoscience  : no profile")

        if enrich_openalex:
            existing_oa, oa_in_is = self._db.get_researcher_openalex_data(sciper)
            _fn = (row.get("firstname") or "").strip()
            _ln = (row.get("lastname") or "").strip()
            _display = f"{_fn} {_ln}".strip() or row.get("display") or ""
            oa_data = self._enrich_openalex_id(
                sciper,
                _display,
                existing_openalex_id=existing_oa,
                openalex_in_infoscience=oa_in_is,
                orcid=researcher.get("orcid"),
            )
            if oa_data:
                oa_data["openalex_in_infoscience"] = False
                self._db.patch_researcher(sciper=sciper, **oa_data)
                logger.info("  OpenAlex     : %s (inferred)", oa_data.get("openalex_id"))
            else:
                logger.info(
                    "  OpenAlex     : %s%s",
                    existing_oa if existing_oa else "not found",
                    " (from IS)" if oa_in_is else "",
                )
            final_oa_id = oa_data.get("openalex_id") or existing_oa
            if final_oa_id:
                oa_profile = self._enrich_openalex_author_profile(sciper, final_oa_id)
                if oa_profile:
                    self._db.patch_researcher_if_null(sciper, **oa_profile)
                    logger.info(
                        "  OpenAlex profile: supplemented %s",
                        ", ".join(oa_profile.keys()),
                    )

        logger.info("sync_one complete — %s (%s)", name, sciper)
        return researcher

    # ------------------------------------------------------------------
    # ORCID batch fetch via persons?ids=
    # ------------------------------------------------------------------

    def _fetch_orcid_map(self, scipers: list[str]) -> dict[str, str]:
        """Fetch ORCIDs for a list of scipers via the persons?ids= endpoint.

        Returns {sciper: bare_orcid_id}.  Logs a warning and returns {} on
        failure so a network error never aborts the full sync.
        """
        try:
            orcid_map = self._epfl.fetch_persons_orcid_batch(scipers)
            logger.info(
                "Persons batch: %d ORCIDs resolved from %d scipers",
                len(orcid_map), len(scipers),
            )
            return orcid_map
        except Exception as exc:
            logger.warning("Could not fetch persons batch for ORCIDs: %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Fetch + transform
    # ------------------------------------------------------------------

    def _fetch_researchers(
        self, class_id: str, position_id: str, status_id: str | None = None
    ) -> tuple[list[dict], list[dict]]:
        """Return (merged_rows, filtered_rows).

        filtered_rows are pre-merge per-unit accred records; merged_rows are
        the per-SCIPER merged rows used to populate researcher_registry.
        """
        records = self._epfl.fetch_accred_by_class_position(
            class_id, position_id, status_id=status_id, format="person"
        )
        if not records:
            return [], []
        filtered = self._epfl.dedupe_people_keep_allowed_affiliations(records)
        merged = self._epfl.merge_records_by_sciper_with_preferred_unit(filtered)
        return merged, filtered

    def _map_accred_to_registry(self, row: dict) -> dict:
        """Map a merged accred row to researcher_registry columns."""
        raw_units = str(row.get("unit_name") or "")
        all_unit_names = [u.strip() for u in raw_units.split("||") if u.strip()]

        return {
            "sciper": str(row.get("sciper") or "").strip(),
            "first_name": row.get("firstname"),
            "last_name": row.get("lastname"),
            "full_name": row.get("display"),
            "email": row.get("email"),
            "epfl_status": row.get("status"),
            "epfl_position": row.get("preferred_position") or row.get("position"),
            "epfl_class": row.get("preferred_class") or row.get("class"),
            "is_active": True,
            "enrollment_date": row.get("preferred_validfrom"),
            "main_unit": row.get("preferred_unit_name"),
            "all_units": json.dumps(all_unit_names) if all_unit_names else None,
        }

    @staticmethod
    def _build_unit_rows(filtered_records: list[dict]) -> dict[str, list[dict]]:
        """Group pre-merge accred records into unit-row dicts per sciper.

        Returns {sciper: [unit_dict, ...]} where each unit_dict has all columns
        needed by upsert_researcher_units.  The unit with the lowest unit_order
        is flagged is_primary=True.
        """
        by_sciper: dict[str, list[dict]] = {}
        for rec in filtered_records:
            sciper = str(rec.get("sciper") or "").strip()
            unit_id = rec.get("unit_id")
            if not sciper or not unit_id:
                continue
            by_sciper.setdefault(sciper, []).append({
                "unit_id": str(unit_id),
                "unit_name": rec.get("unit_name"),
                "unit_label": rec.get("unit_label"),
                "unit_type": rec.get("unit_type"),
                "unit_path": rec.get("unit_path"),
                "unit_cf": rec.get("unit_cf"),
                "unit_level_2": rec.get("unit_level_2"),
                "unit_level_3": rec.get("unit_level_3"),
                "unit_order": rec.get("unit_order"),
                "position": rec.get("position"),
                "epfl_class": rec.get("class"),
                "valid_from": rec.get("validfrom"),
                "valid_to": rec.get("validto"),
            })

        def _int_or_none(v):
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        for units in by_sciper.values():
            orders = [_int_or_none(u["unit_order"]) for u in units]
            valid_orders = [o for o in orders if o is not None]
            min_order = min(valid_orders) if valid_orders else None
            for u in units:
                order = _int_or_none(u["unit_order"])
                u["is_primary"] = (order == min_order) if min_order is not None else (len(units) == 1)

        return by_sciper

    # ------------------------------------------------------------------
    # ORCID external IDs enrichment
    # ------------------------------------------------------------------

    def _enrich_orcid_externals(self, orcid_id: str) -> dict:
        """Fetch external IDs and name variants from the ORCID API.

        Returns a partial dict with any of: scopus_author_id, researcher_id,
        name_variants (JSON array string).  Never raises.
        """
        result: dict = {}

        try:
            ext = self._orcid.fetch_external_identifiers(orcid_id)
            if ext.get("Scopus Author ID"):
                result["scopus_author_id"] = ext["Scopus Author ID"]
            if ext.get("ResearcherID"):
                result["researcher_id"] = ext["ResearcherID"]
        except Exception as exc:
            logger.warning(
                "Could not fetch external identifiers for ORCID %s: %s", orcid_id, exc
            )

        try:
            variants = self._orcid.fetch_name_variants(orcid_id)
            if variants:
                result["name_variants"] = json.dumps(variants)
        except Exception as exc:
            logger.warning(
                "Could not fetch name variants for ORCID %s: %s", orcid_id, exc
            )

        return result

    # ------------------------------------------------------------------
    # DSpace/Infoscience profile enrichment
    # ------------------------------------------------------------------

    def _enrich_dspace_profile(self, sciper: str) -> dict:
        """Fetch DSpace Infoscience profile metadata for a researcher.

        Returns a partial dict with any of: dspace_uuid, infoscience_profile_url,
        openalex_id, scopus_author_id, researcher_id.  Never raises.
        Returns {} when no DSpace client is configured or the profile is not found.
        """
        if self._dspace is None:
            return {}
        try:
            profile = self._dspace.fetch_person_profile(sciper)
            if not profile:
                return {}
            result = {k: v for k, v in profile.items() if v is not None}
            result["openalex_in_infoscience"] = "openalex_id" in result
            return result
        except Exception as exc:
            logger.warning("DSpace profile fetch failed for sciper %s: %s", sciper, exc)
            return {}

    # ------------------------------------------------------------------
    # OpenAlex ID enrichment (DOI inference + manual corrections)
    # ------------------------------------------------------------------

    def _enrich_openalex_id(
        self,
        sciper: str,
        display_name: str,
        existing_openalex_id: str | None = None,
        openalex_in_infoscience: bool | None = None,
        orcid: str | None = None,
    ) -> dict:
        """Infer / correct the OpenAlex author ID for a researcher.

        Priority (highest wins):
          1. Manual corrections (_MANUAL_OPENALEX_CORRECTIONS) — always applied.
          2. openalex_in_infoscience=True — ID is authoritative from IS, skip inference.
          3. ORCID lookup via /authors?filter=orcid (most reliable — no DOIs needed).
          4a. DOI inference using Infoscience outputs only (curated, directly attributed
              to the researcher in Infoscience).
          4b. Fallback: DOI inference using all known sources (harvest + pipeline
              co-authorship) — used when neither ORCID nor gap analysis is available.

        Returns {} when no update is warranted (IS-sourced ID kept, no match found,
        no OpenAlex client configured, or an error occurs).
        """
        # Priority 1 — manual correction always wins
        if sciper in _MANUAL_OPENALEX_CORRECTIONS:
            corrected = _MANUAL_OPENALEX_CORRECTIONS[sciper]
            if corrected != existing_openalex_id:
                return {"openalex_id": corrected}
            return {}

        # Priority 2 — skip inference when the ID is confirmed present in Infoscience
        if openalex_in_infoscience:
            return {}

        if self._openalex is None:
            return {}

        # Priority 3 — ORCID lookup (most reliable: no DOIs required)
        if orcid:
            try:
                oa_id = self._openalex.fetch_author_id_by_orcid(orcid)
                if oa_id:
                    logger.debug(
                        "OpenAlex inference (%s): match via ORCID %s → %s",
                        sciper, orcid, oa_id,
                    )
                    return {"openalex_id": oa_id}
            except Exception as exc:
                logger.warning(
                    "OpenAlex ORCID lookup failed for sciper %s: %s", sciper, exc
                )

        # Priority 4a — Infoscience DOIs: journal articles last 5 years first, then all IS
        infoscience_dois = self._db.get_person_infoscience_dois_for_inference(sciper)
        if infoscience_dois:
            try:
                matches = self._openalex.fetch_author_ids_from_dois(
                    infoscience_dois, display_name
                )
                if matches:
                    logger.debug(
                        "OpenAlex inference (%s): match via %d Infoscience DOI(s)",
                        sciper, len(infoscience_dois),
                    )
                    return {"openalex_id": matches[0]}
            except Exception as exc:
                logger.warning(
                    "OpenAlex DOI inference (Infoscience) failed for sciper %s: %s",
                    sciper, exc,
                )

        # Priority 4b — fallback: full pool (harvest + pipeline co-authorship + IS)
        all_dois = self._db.get_person_publication_dois(sciper)
        if not all_dois:
            return {}
        try:
            matches = self._openalex.fetch_author_ids_from_dois(all_dois, display_name)
        except Exception as exc:
            logger.warning(
                "OpenAlex DOI inference (full pool) failed for sciper %s: %s", sciper, exc
            )
            return {}

        if matches:
            logger.debug(
                "OpenAlex inference (%s): match via full pool (%d DOIs)",
                sciper, len(all_dois),
            )
            return {"openalex_id": matches[0]}
        return {}

    # ------------------------------------------------------------------
    # OpenAlex author profile enrichment (identifiers from /authors/{id})
    # ------------------------------------------------------------------

    def _enrich_openalex_author_profile(self, sciper: str, openalex_id: str) -> dict:
        """Fetch /authors/{id} from OpenAlex and return supplementary identifiers.

        Only returns fields not already populated in the DB (via patch_researcher_if_null).
        Returns {} when no OpenAlex client is configured or an error occurs.
        """
        if self._openalex is None:
            return {}
        try:
            return self._openalex.fetch_author_by_id(openalex_id)
        except Exception as exc:
            logger.warning(
                "OpenAlex author profile enrichment failed for sciper %s: %s", sciper, exc
            )
            return {}

    # ------------------------------------------------------------------
    # Offboarding
    # ------------------------------------------------------------------

    def _offboard_inactive(self, active_scipers: set[str]) -> None:
        """Mark researchers in the DB but absent from active_scipers as inactive."""
        db_active = set(self._db.get_active_scipers())
        to_offboard = db_active - active_scipers
        for sciper in to_offboard:
            logger.info("Offboarding researcher %s (no longer in API scope)", sciper)
            self._db.mark_researcher_inactive(sciper)

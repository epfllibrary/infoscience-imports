"""Wrapper for Dspace client"""

import os
import re
import pandas as pd
from dotenv import load_dotenv
from dspace.dspace_rest_client.client import DSpaceClient
from utils import get_pipeline_logger

load_dotenv(os.path.join(os.getcwd(), ".env"))
ds_api_endpoint = os.environ.get("DS_API_ENDPOINT")

class DSpaceClientWrapper:
    """Wrapper for Dspace client"""
    def __init__(self):
        self.logger = get_pipeline_logger(self.__class__.__name__.lower())

        self.client = DSpaceClient()

        authenticated = self.client.authenticate()
        if authenticated is not True:
            self.logger.info("DSpace API Authentication failed.")

    def _get_item(self, uuid):
        return self.client.get_item(uuid)

    def _search_objects(
        self,
        query,
        filters=None,
        page=0,
        size=1,
        sort=None,
        configuration="researchoutputs",
        scope=None,
        dso_type=None,
        max_pages=None,
    ):
        return self.client.search_objects(
            query=query,
            filters=filters,
            page=page,
            size=size,
            sort=sort,
            configuration=configuration,
            scope=scope,
            dso_type=dso_type,
            max_pages=max_pages,
        )

    def _create_object(self, data):
        return self.client.create_dso(self, ds_api_endpoint, None, data)

    def _update_object(self, uuid, data):
        dso = self.client.get_dso(self, ds_api_endpoint, uuid)
        return self.client.update_dso(self, dso, params=data)

    def find_publication_duplicate(self, x):

        identifier_type = x["source"]
        cleaned_title = clean_title(x["title"])
        pubyear = x["pubyear"]
        if pd.notna(pubyear):
            try:
                pubyear = int(float(pubyear))
            except (ValueError, TypeError):
                pubyear = None
        else:
            pubyear = None

        # Define previous and next years if pubyear is valid
        previous_year = pubyear - 1 if pubyear is not None else None
        next_year = pubyear + 1 if pubyear is not None else None


        # Construct the item ID based on identifier type
        handlers = {
            "wos": lambda x: str(x["internal_id"]).replace("WOS:", "").strip(),
            "scopus": lambda x: str(x["internal_id"]).replace("SCOPUS_ID:", "").strip(),
            "crossref": lambda x: str(x["internal_id"]).strip(),
            "openalex+crossref": lambda x: str(x["internal_id"]).strip(),
            "openalex": lambda x: str(x["doi"]).strip(),
            "zenodo": lambda x: x["internal_id"].strip(),
            "datacite": lambda x: str(x["internal_id"]).strip(),
            "epo": lambda x: str(x["family_id"]).strip(),
            "orcidWorks": lambda x: None,
        }

        if identifier_type in handlers:
            item_id = handlers[identifier_type](x)
        else:
            raise ValueError(
                f"{identifier_type} : identifier_type must be one of {list(handlers.keys())}"
            )
        # Combine all criteria into a single query
        query_parts = []

        if item_id:
            query_parts.append(f'(itemidentifier_keyword:"{item_id}")')

        if pubyear is not None:
            query_parts.append(
                    f"(title:({cleaned_title}) AND (dateIssued.year:{pubyear} "
                    f"OR dateIssued.year:{previous_year} OR dateIssued.year:{next_year}))"
                )
        else:
            query_parts.append(f"(title:({cleaned_title}))")

        if "doi" in x and x["doi"] not in ["", None]:
            query_parts.append(f"(itemidentifier_keyword:\"{str(x['doi']).strip()}\")")

        # Combine all query parts using OR
        final_query = " OR ".join(query_parts)
        final_query = f"({final_query}) AND (entityType:(Publication) OR entityType:(Product) OR entityType:(Patent))"

        final_query_workflow = f"({final_query}) AND (search.resourcetype:(XmlWorkflowItem) OR ((search.resourcetype:(WorkspaceItem) AND submitter_authority:(4e8d183f-1309-470c-955e-c45a99c6f1b8)) OR (search.resourcetype:(WorkspaceItem) AND epfl.workflow.rejected:(true))))"

        # Search for duplicates using the combined query
        self.logger.info(
            f"Searching Infoscience researchoutput with query: {final_query}..."
        )

        # Check the researchoutput configuration
        dsos_researchoutputs = self._search_objects(
            query=final_query,
            page=0,
            size=1,
            dso_type="item",
            configuration="administrativeView",
            max_pages=1,
        )
        num_items_researchoutputs = len(dsos_researchoutputs)

        self.logger.info(
            f"Searching workflow items with query: {final_query_workflow}..."
        )

        # Check the supervision configuration
        dsos_supervision = self._search_objects(
            query=final_query_workflow,
            page=0,
            size=1,
            configuration="supervision",
            max_pages=1,
        )
        num_items_supervision = len(dsos_supervision)

        # Determine if the item is a duplicate in either configuration
        is_duplicate = (num_items_researchoutputs > 0) or (num_items_supervision > 0)

        if is_duplicate:
            self.logger.info(
                f"Publication searched with id:{item_id} found in Infoscience."
            )
            return True  # Duplicate found

        self.logger.info(
            f"Publication searched with id:{item_id} not found in Infoscience."
        )
        return False  # No duplicates found

    # ── Type-aware duplicate check ────────────────────────────────────────

    _ENTITY_FILTER  = "(entityType:(Publication) OR entityType:(Product) OR entityType:(Patent))"
    _TYPE_PREPRINT  = "types:(*preprint*)"
    _TYPE_DATASET   = "entityType:(Product) AND types:(*dataset*)"
    _TYPE_SOFTWARE  = "entityType:(Product) AND types:(*software*)"
    _TYPE_PUBLISHED = "entityType:(Publication) AND -types:(*preprint*)"
    _WORKFLOW_FILTER = (
        "(search.resourcetype:(XmlWorkflowItem) OR "
        "((search.resourcetype:(WorkspaceItem) AND submitter_authority:(4e8d183f-1309-470c-955e-c45a99c6f1b8)) OR "
        "(search.resourcetype:(WorkspaceItem) AND epfl.workflow.rejected:(true))))"
    )

    def _count_items(self, base_query: str, scope: str = None) -> int:
        """Return the true count of archived items matching base_query.

        Uses count_results (reads totalElements) rather than len(search_objects)
        so that scoped comparisons like doi_preprint == doi_total are accurate
        when multiple items share the same DOI (e.g. a preprint and a published
        version both present in Infoscience).
        """
        full_q = f"({base_query}) AND {self._ENTITY_FILTER}"
        return self.client.count_results(
            query=full_q,
            dso_type="item",
            configuration="administrativeView",
            scope=scope,
        ) or 0

    def _fetch_item_info(self, base_query: str, scope: str = None, max_items: int = 5) -> list:
        """Return a list of {uuid, doi, dc_type} dicts for items matching base_query.

        Up to ``max_items`` results are returned so that all known duplicates are
        captured rather than silently dropping matches beyond the first one.
        Returns an empty list when nothing is found.
        """
        full_q = f"({base_query}) AND {self._ENTITY_FILTER}"
        dsos = self._search_objects(
            query=full_q, page=0, size=max_items, dso_type="item",
            configuration="administrativeView", scope=scope, max_pages=1,
        )
        results = []
        for item in dsos:
            meta = getattr(item, "metadata", {}) or {}
            doi_list  = meta.get("dc.identifier.doi", [])
            type_list = meta.get("dc.type", [])
            results.append({
                "uuid":    getattr(item, "uuid", None),
                "doi":     doi_list[0]["value"]  if doi_list  else None,
                "dc_type": type_list[0]["value"] if type_list else None,
            })
        return results

    def _count_workflow_items(self, base_query: str) -> int:
        """Count workflow/workspace items matching base_query."""
        full_q = f"({base_query}) AND {self._WORKFLOW_FILTER}"
        dsos = self._search_objects(
            query=full_q, page=0, size=1, configuration="supervision", max_pages=1,
        )
        return len(dsos)

    def find_publication_duplicate_typed(self, x: dict) -> tuple:
        """Type-aware duplicate check against Infoscience.

        Returns ``(is_duplicate: bool, dedup_note: str | None, flagged_info: dict | None)``.

        dedup_note values:
        - ``"supersedes_preprint"``: published record passed; preprint with same
          title+year already in Infoscience. ``flagged_info`` holds the preprint.
        - ``"cross_type_doi"``: published record passed; same DOI exists only as
          a preprint in Infoscience. ``flagged_info`` holds that item.
        - ``"dataset_in_other_collection"``: dataset passed; title+year match found
          outside "Datasets and Code". ``flagged_info`` holds that item.
        """
        from mappings import (
            classify_record_type,
            PREPRINT_COLLECTION_UUID,
            DATASET_COLLECTION_UUID,
        )

        rec_type = classify_record_type(x)

        identifier_type = x["source"]
        cleaned_title = clean_title(x["title"])
        pubyear = x.get("pubyear")
        if pd.notna(pubyear):
            try:
                pubyear = int(float(pubyear))
            except (ValueError, TypeError):
                pubyear = None
        else:
            pubyear = None

        prev_year = pubyear - 1 if pubyear else None
        next_year = pubyear + 1 if pubyear else None

        handlers = {
            "wos":               lambda r: str(r["internal_id"]).replace("WOS:", "").strip(),
            "scopus":            lambda r: str(r["internal_id"]).replace("SCOPUS_ID:", "").strip(),
            "crossref":          lambda r: str(r["internal_id"]).strip(),
            "openalex+crossref": lambda r: str(r["internal_id"]).strip(),
            "openalex":          lambda r: str(r["doi"]).strip(),
            "zenodo":            lambda r: r["internal_id"].strip(),
            "datacite":          lambda r: str(r["internal_id"]).strip(),
            "epo":               lambda r: str(r["family_id"]).strip(),
            "orcidWorks":        lambda r: None,
        }
        item_id = handlers.get(identifier_type, lambda r: None)(x)
        doi = str(x.get("doi") or "").strip()

        # Title+year query
        if pubyear is not None:
            ty_q = (
                f"(title:({cleaned_title}) AND "
                f"(dateIssued.year:{pubyear} OR dateIssued.year:{prev_year} OR dateIssued.year:{next_year}))"
            )
        else:
            ty_q = f"(title:({cleaned_title}))"

        self.logger.info(
            "Type-aware dedup [%s] — rec_type=%s doi=%s",
            cleaned_title[:60], rec_type, doi or "(none)",
        )

        # ── DOI check ─────────────────────────────────────────────────
        if doi:
            doi_q = f'(itemidentifier_keyword:"{doi}")'
            doi_total = self._count_items(doi_q)
            if doi_total > 0:
                if rec_type == "published":
                    doi_preprint_q = f"{doi_q} AND {self._TYPE_PREPRINT}"
                    doi_preprint = self._count_items(doi_preprint_q, scope=PREPRINT_COLLECTION_UUID)
                    if doi_preprint == doi_total:
                        items = self._fetch_item_info(doi_preprint_q, scope=PREPRINT_COLLECTION_UUID)
                        return False, "cross_type_doi", items or None
                return True, None, None
            if self._count_workflow_items(doi_q) > 0:
                return True, None, None

        # ── Item identifier check ──────────────────────────────────────
        if item_id:
            id_q = f'(itemidentifier_keyword:"{item_id}")'
            if self._count_items(id_q) > 0 or self._count_workflow_items(id_q) > 0:
                return True, None, None

        # ── Title+year check (type-scoped) ────────────────────────────
        if rec_type == "dataset":
            dc_type = str(x.get("dc.type") or "")
            product_type_filter = (
                self._TYPE_SOFTWARE if dc_type.startswith("software")
                else self._TYPE_DATASET
            )
            if self._count_items(f"{ty_q} AND {product_type_filter}", scope=DATASET_COLLECTION_UUID) > 0:
                return True, None, None
            # Title+year exists outside "Datasets and Code" — different entity, flag it
            ty_total = self._count_items(ty_q)
            if ty_total > 0:
                items = self._fetch_item_info(ty_q)
                return False, "dataset_in_other_collection", items or None
            return False, None, None

        elif rec_type == "preprint":
            ty_preprint_q = f"{ty_q} AND {self._TYPE_PREPRINT}"
            if self._count_items(ty_preprint_q, scope=PREPRINT_COLLECTION_UUID) > 0:
                return True, None, None
            if self._count_workflow_items(ty_q) > 0:
                return True, None, None
            # Check if a published version exists
            ty_total   = self._count_items(ty_q)
            ty_preprint = self._count_items(ty_preprint_q, scope=PREPRINT_COLLECTION_UUID)
            if ty_total > ty_preprint:
                all_items = self._fetch_item_info(ty_q, max_items=5)
                published_items = [i for i in all_items if i.get("dc_type") != "text::preprint"]
                return False, "published_version_exists", published_items or all_items or None
            return False, None, None

        else:  # published
            ty_total = self._count_items(ty_q)
            if ty_total > 0:
                ty_preprint_q = f"{ty_q} AND {self._TYPE_PREPRINT}"
                ty_preprint   = self._count_items(ty_preprint_q, scope=PREPRINT_COLLECTION_UUID)
                ty_published  = self._count_items(f"{ty_q} AND {self._TYPE_PUBLISHED}")
                if ty_published > 0:
                    return True, None, None
                if ty_preprint > 0:
                    items = self._fetch_item_info(ty_preprint_q, scope=PREPRINT_COLLECTION_UUID)
                    return False, "supersedes_preprint", items or None
            if self._count_workflow_items(ty_q) > 0:
                return True, None, None
            return False, None, None

    def find_duplicate_enhanced(self, x):
        identifier_type = x.get("source")
        cleaned_title = clean_title(x.get("title", ""))
        pubyear = x.get("pubyear")
        if pd.notna(pubyear):
            try:
                pubyear = int(float(pubyear))
            except (ValueError, TypeError):
                pubyear = None
        else:
            pubyear = None

        previous_year = pubyear - 1 if pubyear else None
        next_year = pubyear + 1 if pubyear else None

        handlers = {
            "wos": lambda x: str(x["internal_id"]).replace("WOS:", "").strip(),
            "scopus": lambda x: str(x["internal_id"]).replace("SCOPUS_ID:", "").strip(),
            "crossref": lambda x: str(x["internal_id"]).strip(),
            "openalex+crossref": lambda x: str(x["internal_id"]).strip(),
            "openalex": lambda x: str(x["doi"]).strip(),
            "zenodo": lambda x: str(x["internal_id"]).strip(),
            "datacite": lambda x: str(x["internal_id"]).strip(),
            "epo": lambda x: str(x["family_id"]).strip(),
            "orcidWorks": lambda x: None,
        }

        if identifier_type not in handlers:
            raise ValueError(
                f"{identifier_type} : identifier_type must be one of {list(handlers.keys())}"
            )

        item_id = handlers[identifier_type](x)

        query_parts = []
        if item_id:
            query_parts.append(f'(itemidentifier_keyword:"{item_id}")')
        if pubyear:
            query_parts.append(
                f"(title:({cleaned_title}) AND (dateIssued.year:{pubyear} OR dateIssued.year:{previous_year} OR dateIssued.year:{next_year}))"
            )
        else:
            query_parts.append(f"(title:({cleaned_title}))")
        doi = x.get("doi")
        if isinstance(doi, str) and doi.strip():
            query_parts.append(f'(itemidentifier_keyword:"{doi.strip()}")')

        final_query = " OR ".join(query_parts)
        final_query = f"({final_query}) AND (entityType:(Publication) OR entityType:(Product) OR entityType:(Patent))"

        self.logger.info(f"Searching Infoscience researchoutput with query: {final_query}")

        results = self._search_objects(
            query=final_query,
            page=0,
            size=1,
            dso_type="item",
            configuration="researchoutputs",
            max_pages=1,
        )

        if results:
            result = results[0]
            uuid = getattr(result, "uuid", None)
            handle = getattr(result, "handle", None)

            metadata = getattr(result, "metadata", {}) or {}
            dc_type_list = metadata.get("dc.type", [])
            dc_title_list = metadata.get("dc.title", [])
            dc_date_list = metadata.get("dc.date.issued", [])
            dc_identifier_doi_list = metadata.get("dc.identifier.doi", [])
            epfl_reviewed_list = metadata.get("epfl.peerreviewed", [])
            epfl_writtenat_list = metadata.get("epfl.writtenAt", [])

            dc_type = dc_type_list[0]["value"] if dc_type_list else None
            dc_title = dc_title_list[0]["value"] if dc_title_list else None
            dc_date_issued = dc_date_list[0]["value"] if dc_date_list else None
            dc_identifier_doi = dc_identifier_doi_list[0]["value"] if dc_identifier_doi_list else None
            epfl_peerreviewed = (epfl_reviewed_list[0]["value"] if epfl_reviewed_list else None)
            epfl_writtenat = (epfl_writtenat_list[0]["value"] if epfl_writtenat_list else None)

            self.logger.info(f"Duplicate found for doi={doi}: uuid={uuid}, handle={handle}, title={dc_title}, type={dc_type}, date_issued={dc_date_issued}, dc_identifier_doi={dc_identifier_doi}, epfl_peerreviewed={epfl_peerreviewed}, written_at={epfl_writtenat}")

            return {
                "is_duplicate": True,
                "uuid": uuid,
                "handle": handle,
                "dc_title": dc_title,
                "dc_type": dc_type,
                "dc_identifier_doi": dc_identifier_doi,
                "dc_date_issued": dc_date_issued,
                "epfl_peerreviewed": epfl_peerreviewed,
                "epfl_writtenat": epfl_writtenat,
            }

        self.logger.info("No duplicate found in research outputs.")
        return {
            "is_duplicate": False,
            "uuid": None,
            "handle": None,
            "dc_title": None,
            "dc_type": None,
            "dc_identifier_doi": None,
            "dc_date_issued": None,
            "epfl_peerreviewed": None,
            "epfl_writtenat": None,
        }

    @staticmethod
    def _extract_person_lastnames(metadata: dict) -> list:
        """Return all last-name candidates for a DSpace person item.

        Collects: person.familyName (primary), dc.title parsed before the comma
        (fallback), and the pre-comma token from every crisrp.name.variant entry.
        Duplicates are removed while preserving order.
        """
        lastnames = []

        def _add(raw):
            name = (raw or "").strip()
            if name and name not in lastnames:
                lastnames.append(name)

        family_meta = metadata.get("person.familyName", [])
        if family_meta:
            _add(family_meta[0]["value"])
        else:
            title_meta = metadata.get("dc.title", [])
            if title_meta:
                _add(title_meta[0]["value"].split(",")[0])

        for entry in metadata.get("crisrp.name.variant", []):
            raw = entry.get("value", "")
            if "," in raw:
                _add(raw.split(",")[0])

        return lastnames

    def find_person(self, query):
        """
        param query: format (index:value), for example (title:Scolaro A.)
        """
        dsos_persons = self._search_objects(
            query=query,
            size=10,
            configuration="person",
        )
        num_items_persons = len(dsos_persons)
        if num_items_persons == 1:
            self.logger.debug(
                "Single record found for %s in DspaceCris. Processing record.", query
            )
            sciper_metadata = dsos_persons[0].metadata.get("epfl.sciperId")
            sciper_id = (
                sciper_metadata[0]["value"] if sciper_metadata and len(sciper_metadata) > 0 else ""
            )
            matched_lastnames = self._extract_person_lastnames(dsos_persons[0].metadata)
            return {
                "uuid": dsos_persons[0].uuid,
                "sciper_id": sciper_id,
                "matched_lastnames": matched_lastnames,
            }
        elif num_items_persons == 0:
            self.logger.warning(
                f"No record found for {query} in DspaceCris: {num_items_persons} results."
            )
            return None
        elif num_items_persons > 1:
            self.logger.warning(
                f"Multiple records found for {query} in DspaceCris: {num_items_persons} results."
            )
            return None

    def fetch_person_profile(self, sciper: str) -> dict | None:
        """Fetch the Infoscience person profile for a SCIPER.

        Returns a dict with dspace_uuid, infoscience_profile_url, openalex_id,
        scopus_author_id, and researcher_id extracted from the DSpace item
        metadata.  Returns None when no profile is found for the SCIPER.
        """
        items = self._search_objects(
            query=f"epfl.sciperId:{sciper}",
            page=0,
            size=1,
            sort="dc.date.accessioned,DESC",
            configuration="person",
        )
        if not items:
            return None

        item = items[0]
        md = item.metadata

        def _first_value(key: str) -> str | None:
            arr = md.get(key)
            if not isinstance(arr, list) or not arr:
                return None
            v = (arr[0] or {}).get("value")
            return str(v).strip() if v else None

        def _all_values(key: str) -> list[str]:
            arr = md.get(key)
            if not isinstance(arr, list):
                return []
            return [str(e["value"]).strip() for e in arr if e and e.get("value")]

        handle = getattr(item, "handle", None)
        url = (
            f"https://infoscience.epfl.ch/handle/{handle}"
            if handle
            else _first_value("dc.identifier.uri")
        )

        import json as _json
        raw_variants = _all_values("crisrp.name.variant")
        name_variants = _json.dumps(raw_variants) if raw_variants else None

        return {
            "dspace_uuid": item.uuid,
            "infoscience_profile_url": url,
            "openalex_id": _first_value("person.identifier.openalex"),
            "scopus_author_id": _first_value("person.identifier.scopus-author-id"),
            "researcher_id": _first_value("person.identifier.rid"),
            "name_variants": name_variants,
        }

    def push_publication(self, source, wos_id, collection_id):
        try:
            # Attempt to create a workspace item from the external source
            response = self.client.create_workspaceitem_from_external_source(
                source, wos_id, collection_id
            )

            # Check if the response contains the expected data
            if response and "id" in response:
                workspace_id = response["id"]
                self.logger.info(
                    f"Successfully created workspace item with ID: {workspace_id}"
                )
                return response
            else:
                self.logger.error(
                    "Failed to create workspace item: Response does not contain 'id'."
                )
                return None
        except Exception as e:
            self.logger.error(
                f"An error occurred while pushing the publication: {str(e)}"
            )
            return None

    def update_workspace(self, workspace_id, patch_operations):
        return self.client.update_workspaceitem(workspace_id, patch_operations)

    def create_workflowitem(self, workspace_id):
        return self.client.create_workflowitem(workspace_id)

    def upload_file_to_workspace(self, workspace_id, file_path):
        return self.client.upload_file_to_workspace(workspace_id, file_path)

    def update_adminitem(self, uuid, patch_operations):
        return self.client.update_adminitem(uuid, patch_operations)

    def add_file_adminitem(self, uuid, file_path):
        return self.client.add_file_adminitem(uuid, file_path)

    def delete_workspace(self, workspace_id):
        return self.client.delete_workspace_item(workspace_id)

    def delete_workflow(self, workflow_id):
        return self.client.delete_workflow_item(workflow_id)

    def find_workspaceitem_by_item_uuid(self, item_uuid: str) -> int | None:
        """Return the workspace item ID for the given DSpace item UUID, or None if not found.

        Uses the supervision search configuration with the query:
            namedresourcetype_authority:workspace "Item-{item_uuid}"

        This is the same search strategy used for Infoscience deduplication and is the
        only reliable way to find a workspace item after a workflow rejection, since
        DSpace assigns a new workspace_id that differs from the original stored value.
        """
        query = f'namedresourcetype_authority:workspace "Item-{item_uuid}"'
        try:
            results = self._search_objects(
                query=query,
                page=0,
                size=1,
                configuration="supervision",
                max_pages=1,
            )
            if results:
                ws_id = getattr(results[0], "id", None)
                if ws_id is not None:
                    self.logger.info(
                        "Found workspace item %s for item %s via supervision search.",
                        ws_id, item_uuid,
                    )
                    return int(ws_id)
        except Exception as exc:
            self.logger.warning("find_workspaceitem_by_item_uuid failed: %s", exc)
        return None

    def delete_item(
        self,
        workspace_id: str | None,
        workflow_id: str | None,
        item_uuid: str | None = None,
    ) -> tuple[bool, str]:
        """Delete an imported item from DSpace, handling the two-step sequence for workflow items.

        For workflow items: calls DELETE with ?expunge=true — DSpace rejects the item
        back to workspace as a new draft with a NEW workspace_id returned in the response
        body. That new workspace_id is used to permanently delete the workspace item.
        Falls back to find_workspaceitem_by_item_uuid if the response contains no body.
        For workspace-only items: deletes directly.

        Returns (success, message).
        """
        workflow_done = False
        try:
            if workflow_id:
                url = f"{self.client.API_ENDPOINT}/workflow/workflowitems/{int(float(workflow_id))}?expunge=true"
                response = self.client.api_delete(url)
                workflow_done = True
                self.logger.info("Workflow item %s rejected back to workspace.", workflow_id)

                # DSpace returns the new workspace item in the response body.
                # Parse its id to use for the workspace deletion step.
                new_ws_id = self._parse_workspace_id_from_response(response)
                if new_ws_id is not None:
                    workspace_id = str(new_ws_id)
                    self.logger.info("New workspace item %s found in response.", new_ws_id)
                else:
                    # Fallback: search by item UUID if the response had no body.
                    self.logger.warning(
                        "No workspace item in rejection response — searching by UUID."
                    )
                    if item_uuid:
                        found = self.find_workspaceitem_by_item_uuid(item_uuid)
                        if found is not None:
                            workspace_id = str(found)
                            self.logger.info("Found workspace item %s via UUID search.", found)

            if workspace_id:
                self.delete_workspace(int(float(str(workspace_id))))
                self.logger.info("Workspace item %s deleted.", workspace_id)

            return True, "Item supprimé avec succès."
        except Exception as exc:
            self.logger.error("delete_item failed (workflow_done=%s): %s", workflow_done, exc)
            if workflow_done:
                return False, (
                    f"Le workflow a été rejeté mais la suppression du workspace a échoué : {exc}. "
                    "Relancez la suppression — l'item est maintenant en workspace."
                )
            return False, f"Échec de la suppression : {exc}"

    def _parse_workspace_id_from_response(self, response) -> int | None:
        """Extract the workspace item id from a DSpace rejection response.

        DSpace returns the new workspace item in the response body when a workflow
        item is rejected with ?expunge=true. Returns None if the body is absent or
        unparseable (204 No Content, network error, etc.).
        """
        if response is None:
            return None
        try:
            if response.status_code in (200, 201) and response.content:
                ws_id = response.json().get("id")
                return int(ws_id) if ws_id is not None else None
        except Exception as exc:
            self.logger.warning("Could not parse workspace id from response: %s", exc)
        return None

    @staticmethod
    def _extract_item_quality(data: dict) -> dict:
        """Extract quality indicators from a fully-embedded DSpace item response.

        Checks performed:
          abstract_ok — dc.description.abstract present and non-empty.
          pdf_ok      — at least one bitstream in the ORIGINAL bundle has
                        dc.type = "main document" with an openaccess resource
                        policy (falls back to True if policy embed is absent).
        """
        meta = data.get("metadata") or {}
        abstract_ok = any(
            str(v.get("value", "")).strip()
            for v in meta.get("dc.description.abstract", [])
            if isinstance(v, dict)
        )

        pdf_ok = False
        for bundle in (
            data.get("_embedded", {})
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
                # Verify openaccess resource policy when embedded; trust main
                # document type alone if the embed is not supported.
                policies = (
                    bs.get("_embedded", {})
                      .get("accessConditions", {})
                      .get("_embedded", {})
                      .get("resourcepolicies", [])
                )
                is_open = (
                    any(
                        str(p.get("name", "")).lower() == "openaccess"
                        for p in policies if isinstance(p, dict)
                    )
                    if policies else True
                )
                if is_open:
                    pdf_ok = True
                    break
            if pdf_ok:
                break

        return {"abstract_ok": abstract_ok, "pdf_ok": pdf_ok}

    def check_item_infoscience_status(
        self,
        dspace_item_uuid: "str | None",
        workspace_id: "str | None",
        workflow_id: "str | None",
        check_quality: bool = False,
    ) -> "tuple[str, str | None, dict | None]":
        """Check the current Infoscience status of a previously imported item.

        Returns (status, handle, quality) where:
          status  — one of: 'published', 'withdrawn', 'deleted', 'rejected',
                    'still_pending'
          handle  — Infoscience handle (e.g. "20.500.14299/12345") when
                    status='published', None otherwise
          quality — dict {"abstract_ok": bool, "pdf_ok": bool} when
                    status='published' and check_quality=True, None otherwise

        When check_quality=True the published-item call embeds bundles and
        bitstreams so that status, abstract, and PDF presence are resolved in
        a single round-trip.
        """
        # Step 1 — try the canonical item endpoint via dspace_item_uuid
        if dspace_item_uuid:
            url = f"{self.client.API_ENDPOINT}/core/items/{dspace_item_uuid}"
            if check_quality:
                url += (
                    "?embed=bundles"
                    "&embed=bundles/bitstreams"
                    "&embed=bundles/bitstreams/accessConditions"
                )
            r = self.client.api_get(url)
            if r.status_code == 200:
                try:
                    data = r.json()
                except Exception:
                    data = {}
                if data.get("inArchive"):
                    handle = data.get("handle") or None
                    quality = self._extract_item_quality(data) if check_quality else None
                    return "published", handle, quality
                if data.get("withdrawn"):
                    return "withdrawn", None, None
                # Item exists but is neither published nor withdrawn — could be
                # still_pending or rejected (workspace). Fall through to the
                # workspace check to detect the rejection flag.
            elif r.status_code == 404:
                # Item UUID not found — fall through to workspace/workflow checks.
                pass
            else:
                self.logger.warning(
                    "Unexpected status %s fetching item %s",
                    r.status_code, dspace_item_uuid,
                )
                return "still_pending", None, None

        # Step 2 — resolve the live workspace item.
        # After a workflow rejection DSpace assigns a NEW workspace id, so the
        # stored workspace_id is stale.  Search by item UUID via the supervision
        # configuration first, then fall back to the stored id.
        live_ws_id = None
        if dspace_item_uuid:
            live_ws_id = self.find_workspaceitem_by_item_uuid(dspace_item_uuid)
        if live_ws_id is None and workspace_id:
            try:
                live_ws_id = int(float(str(workspace_id)))
            except (ValueError, TypeError):
                pass

        if live_ws_id is not None:
            try:
                url = (
                    f"{self.client.API_ENDPOINT}/submission/workspaceitems/{live_ws_id}"
                    "?embed=item&embed=collection"
                )
                r = self.client.api_get(url)
                if r.status_code == 200:
                    try:
                        ws_data = r.json()
                    except Exception:
                        ws_data = {}
                    item_meta = (
                        ws_data.get("_embedded", {})
                        .get("item", {})
                        .get("metadata", {})
                    )
                    rejected = item_meta.get("epfl.workflow.rejected", [])
                    if any(
                        str(entry.get("value", "")).lower() == "true"
                        for entry in rejected
                    ):
                        return "rejected", None, None
                    return "still_pending", None, None
                elif r.status_code == 404:
                    pass  # workspace item gone — fall through
            except Exception as exc:
                self.logger.warning("Workspace check failed for %s: %s", live_ws_id, exc)

        # Step 3 — check workflow item
        if workflow_id:
            try:
                wf_int = int(float(str(workflow_id)))
                url = f"{self.client.API_ENDPOINT}/workflow/workflowitems/{wf_int}"
                r = self.client.api_get(url)
                if r.status_code == 200:
                    return "still_pending", None, None
                # 404 → workflow item gone too
            except Exception as exc:
                self.logger.warning("Workflow check failed for %s: %s", workflow_id, exc)

        # Nothing found — item has been fully deleted or never existed
        if dspace_item_uuid or workspace_id or workflow_id:
            return "deleted", None, None

        # No identifiers at all — cannot determine status
        return "still_pending", None, None

    def fetch_person_publications(
        self,
        person_uuid: str,
        page_size: int = 100,
    ) -> list[dict]:
        """Return all Infoscience publications linked to a person UUID.

        Uses the ``RELATION.Person.researchoutputs`` discovery configuration
        with ``scope={person_uuid}`` — follows the explicit CRIS authorship
        link rather than matching free-text metadata, so it captures all
        linked records including those without a stored DOI or author-authority
        field value.  (Per the Infoscience REST API documentation.)

        Each returned dict has: uuid, doi, title, pub_year, dc_type, handle.
        """
        dsos = self._search_objects(
            query=None,
            page=0,
            size=page_size,
            dso_type="item",
            configuration="RELATION.Person.researchoutputs",
            scope=person_uuid,
            max_pages=200,
        )

        results: list[dict] = []
        for dso in dsos:
            md = dso.metadata
            doi_entries = md.get("dc.identifier.doi", [])
            doi = doi_entries[0].get("value", "").strip() if doi_entries else None
            title_entries = md.get("dc.title", [])
            title = title_entries[0].get("value", "").strip() if title_entries else None
            year_entries = md.get("dc.date.issued", [])
            pub_year = None
            if year_entries:
                raw_year = year_entries[0].get("value", "")
                pub_year = raw_year[:4] if raw_year else None
            handle_entries = md.get("dc.identifier.uri", [])
            handle = handle_entries[0].get("value", "").strip() if handle_entries else None
            type_entries = md.get("dc.type", [])
            dc_type = type_entries[0].get("value", "").strip() if type_entries else None
            results.append(
                {
                    "uuid": dso.uuid,
                    "doi": doi or None,
                    "title": title,
                    "pub_year": pub_year,
                    "dc_type": dc_type,
                    "handle": handle,
                }
            )
        return results

    def search_authority(
        self,
        authority_type="AuthorAuthority",
        metadata="dc.contributor.author",
        filter_text="",
        exact=False,
    ):
        return self.client.get_authority(authority_type, metadata, filter_text, exact)

    def get_sciper_from_authority(self, response):
        """
        Extracts a SCIPER-ID from an authority response.
        If multiple entries exist but all have the same 'authority' value,
        only the first entry is processed.

        Args:
            response (dict): The JSON response containing authority data.

        Returns:
            int | None: The extracted SCIPER-ID or None if not found.
        """
        # Retrieve entries from the response
        entries = response.get("_embedded", {}).get("entries", [])
        if not entries:
            return None

        # Check if all entries have the same 'authority' value
        authorities = {entry.get("authority") for entry in entries}
        if len(authorities) > 1:
            # If multiple different 'authority' values are found, do not process
            return None

        # Process the first entry if all authorities are identical
        first_entry = entries[0]
        auth_id = first_entry.get("authority")
        if not auth_id:
            return None

        # Check if auth_id starts with "will be generated"
        if auth_id.startswith("will be generated"):
            match = re.search(r"::(\d+)$", auth_id)
            if match:
                return int(match.group(1))  # Return the ID as an integer
        else:
            # Use an external method to retrieve metadata for the given authority
            authority = self._get_item(auth_id)
            if authority:
                sciper_data = authority.metadata.get("epfl.sciperId", [])
                if (
                    sciper_data
                    and isinstance(sciper_data, list)
                    and "value" in sciper_data[0]
                ):
                    return sciper_data[0]["value"]

        # Return None if no valid SCIPER-ID is found
        return None


def clean_title(title):
    title = re.sub(r"<[^>]+>", "", title)
    title = re.sub(r"[^\w\s-]", " ", title)
    title = re.sub(r"(?<!\w)-|-(?!\w)", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

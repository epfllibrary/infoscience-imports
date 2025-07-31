"""Wrapper for Dspace client"""

import os
import re
import pandas as pd
from dotenv import load_dotenv
from config import logs_dir
from dspace.dspace_rest_client.client import DSpaceClient
from utils import manage_logger

load_dotenv(os.path.join(os.getcwd(), ".env"))
ds_api_endpoint = os.environ.get("DS_API_ENDPOINT")

class DSpaceClientWrapper:
    """Wrapper for Dspace client"""
    def __init__(self):
        log_file_path = os.path.join(logs_dir, "logging.log")
        self.logger = manage_logger(log_file_path)

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
            # affiliation_metadata = dsos_persons[0].metadata.get("person.affiliation.name", "")

            # self.logger.debug("affiliation_metadata: %s", affiliation_metadata)

            # main_affiliation = (
            #     affiliation_metadata[0]["value"]
            #     if affiliation_metadata else ""
            # )
            return {
                "uuid": dsos_persons[0].uuid,
                "sciper_id": sciper_id,
                # "main_affiliation": main_affiliation,
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

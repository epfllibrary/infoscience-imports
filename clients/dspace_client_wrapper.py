from dspace.client import DSpaceClient
import logging
import os, re
from dotenv import load_dotenv
from utils import manage_logger


load_dotenv(os.path.join(os.getcwd(), ".env"))
ds_api_endpoint = os.environ.get("DS_API_ENDPOINT")


class DSpaceClientWrapper:
    def __init__(self):
        self.client = DSpaceClient()
        self.logger = manage_logger("./logs/dspace_client.log")

        authenticated = self.client.authenticate()
        self.logger.info(f"Authentication status {authenticated}.")

    def _search_objects(
        self, query, page=0, size=1, configuration="researchoutputs", dso_type=None
    ):
        return self.client.search_objects(
            query=query,
            page=page,
            size=size,
            dso_type=dso_type,
            configuration=configuration,
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
        if isinstance(pubyear, str) and pubyear.isdigit():
            pubyear = int(pubyear)
        elif not isinstance(pubyear, int):
            raise ValueError("pubyear must be an integer")
        previous_year = pubyear - 1
        next_year = pubyear + 1

        # Build queries for each matching rule
        print("identifier_type is ", identifier_type)
        if identifier_type == "wos":
            item_id = str(x["internal_id"]).replace("WOS:", "").strip()
        elif identifier_type == "scopus":
            item_id = str(x["internal_id"]).replace("SCOPUS_ID:", "").strip()
        elif identifier_type == "zenodo":
            item_id = x["doi"].strip()
        else:
            raise ValueError("identifier_type must be 'wos', 'scopus' or 'zenodo'")

        query = f'(itemidentifier:"*{item_id}*")'
        title_query = f"(title:({cleaned_title}) AND (dateIssued:{pubyear} OR dateIssued:{previous_year} OR dateIssued:{next_year}))"
        doi_query = (
            f"(itemidentifier:\"*{str(x['doi']).strip()}*\")" if "doi" in x else None
        )

        # Check each identifier for duplicates
        for query in [query, title_query, doi_query]:
            if query is None:
                continue
            self.logger.debug(
                f"Searching archived researchoutput with query:{query}..."
            )
            # Check the researchoutput configuration
            dsos_researchoutputs = self._search_objects(
                query=query,
                page=0,
                size=1,
                dso_type="item",
                configuration="researchoutputs",
            )
            num_items_researchoutputs = len(dsos_researchoutputs)
            self.logger.debug(f"Searching workflow items with query:{query}...")
            # Check the supervision configuration
            dsos_supervision = self._search_objects(
                query=query,
                page=0,
                size=1,
                configuration="supervision",
            )
            num_items_supervision = len(dsos_supervision)

            # Determine if the item is a duplicate in either configuration
            is_duplicate = (num_items_researchoutputs > 0) or (
                num_items_supervision > 0
            )

            if is_duplicate:
                self.logger.info(
                    f"Publication searched with id:{item_id} found in Infoscience."
                )
                return True  # Duplicate found

        self.logger.debug(
            f"Publication searched with id:{item_id} not found in Infoscience."
        )
        return False  # No duplicates found

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
                f"Single record found for {query} in DspaceCris. Processing record."
            )
            # return {
            #    "uuid": dsos_persons[0].uuid,
            #    "name": dsos_persons[0].metadata.get("dc.title")[0]["value"]
            # }
            return dsos_persons[0].uuid
        elif num_items_persons == 0:
            self.logger.warning(
                f"No record found for {query} in DspaceCris: {num_items_persons} results."
            )
            return "0 result on Infoscience"
        elif num_items_persons > 1:
            self.logger.warning(
                f"Multiple records found for {query} in DspaceCris: {num_items_persons} results."
            )
            return "At least 1 result on Infoscience"

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
                return workspace_id
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


def clean_title(title):
    title = re.sub(r"<[^>]+>", "", title)
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

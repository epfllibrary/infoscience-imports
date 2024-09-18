from dspace.client import DSpaceClient
import logging
import os, re
from dotenv import load_dotenv
from utils import manage_logger

logger = manage_logger("./logs/dspace_client.log")

load_dotenv(os.path.join(os.getcwd(), ".env"))
ds_api_endpoint = os.environ.get("DS_API_ENDPOINT")

class DSpaceClientWrapper:
    def __init__(self):
        self.client = DSpaceClient()
        authenticated = self.client.authenticate()
        logging.info(f"Authentication status {authenticated}.")

    def _search_objects(self, query, page=0, size=1, configuration="researchoutputs", dso_type=None):
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
            raise ValueError("pubyear doit être numérique")
        previous_year = pubyear - 1
        next_year = pubyear + 1

        # Build queries for each matching rule
        if identifier_type == 'wos':
            item_id = str(x['internal_id']).replace("WOS:","").strip()
        elif identifier_type == 'scopus':
            item_id = str(x['internal_id']).replace("SCOPUS_ID:","").strip() 
        else:
            raise ValueError("identifier_type must be 'wos' or 'scopus'")

        query = f"(itemidentifier:\"*{item_id}*\")"
        title_query = f"(title:({cleaned_title}) AND (dateIssued:{pubyear} OR dateIssued:{previous_year} OR dateIssued:{next_year}))"
        doi_query = (
            f"(itemidentifier:\"*{str(x['doi']).strip()}*\")" if "doi" in x else None
        )

        # Check each identifier for duplicates
        for query in [query, title_query, doi_query]:
            if query is None:
                continue
            logging.info(f"Searching archived researchoutput with query:{query}...")
            # Check the researchoutput configuration
            dsos_researchoutputs = self._search_objects(
                query=query,
                page=0,
                size=1,
                dso_type="item",
                configuration="researchoutputs",
            )
            num_items_researchoutputs = len(dsos_researchoutputs)
            logging.info(f"Searching workflow items with query:{query}...")
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
                logging.info(
                    f"Publication searched with id:{item_id} founded in Infoscience."
                )
                return True  # Duplicate found

        logging.info(
            f"Publication searched with id:{item_id} not founded in Infoscience."
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
            logger.info(f"Single record found for {query} in DspaceCris. Processing record.")
            #return {
            #    "uuid": dsos_persons[0].uuid,
            #    "name": dsos_persons[0].metadata.get("dc.title")[0]["value"]
            #}
            return dsos_persons[0].uuid
        elif num_items_persons == 0:
            logger.warning(f"No record found for {query} in DspaceCris: {num_items_persons} results.")
            return "0 résultat dans Dspace"
        elif num_items_persons > 1:
            logger.warning(f"Multiple records found for {query} in DspaceCris: {num_items_persons} results.")
            return "Plus de 1 résultat dans Dspace"
    
    def push_publication(self, source, wos_id, collection_id):
        try:
            # Attempt to create a workspace item from the external source
            response = self.client.create_workspaceitem_from_external_source(source, wos_id, collection_id)
            
            # Check if the response contains the expected data
            if response and "id" in response:
                workspace_id = response["id"]
                logger.info(f"Successfully created workspace item with ID: {workspace_id}")
                return workspace_id
            else:
                logger.error("Failed to create workspace item: Response does not contain 'id'.")
                return None
        except Exception as e:
            logger.error(f"An error occurred while pushing the publication: {str(e)}")
            return None
        
    def update_workspace(self, workspace_id, patch_operations):
        try:
            # Attempt to update the workspace item
            update_response = self.client.update_workspaceitem(workspace_id, patch_operations)
            if update_response:
                logger.info(f"Successfully updated workspace item with ID: {workspace_id}")

                # Attempt to import Unpaywall fulltext
                ft = self.client.import_unpaywall_fulltext(workspace_id)
                if ft:
                    logger.info("Import Unpaywall successful.")
                else:
                    logger.warning("Failed to import Unpaywall fulltext.")

                # Pass draft to workflow
                wf_response = self.client.create_workflowitem(workspace_id)
                if wf_response:
                    logger.info(f"Successfully created workflow item for workspace ID: {workspace_id}")
                else:
                    logger.error(f"Failed to create workflow item for workspace ID: {workspace_id}")
            else:
                logger.error(f"Failed to update workspace item with ID: {workspace_id}. No response received.")
        except Exception as e:
            logger.error(f"An error occurred while updating the workspace: {str(e)}")

        
        
def clean_title(title):
    title = re.sub(r"<[^>]+>", "", title)
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title   

"""ORCID client for Infoscience imports"""

import os
import json
import tenacity
from apiclient import (
    APIClient,
    endpoint,
    HeaderAuthentication,
    JsonResponseHandler,
)
from apiclient.retrying import retry_if_api_request_error
from apiclient.request_formatters import BaseRequestFormatter
from apiclient.utils.typing import OptionalJsonType, OptionalStr
from dotenv import load_dotenv
from config import logs_dir
from utils import manage_logger

log_file_path = os.path.join(logs_dir, "orcid_client.log")
logger = manage_logger(log_file_path)

orcid_prod_public_base_url = "https://pub.orcid.org/v3.0"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
orcid_api_token = os.environ.get("ORCID_API_TOKEN")

orcid_authentication_method = HeaderAuthentication(token=orcid_api_token)

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

class OrcidJsonRequestFormatter(BaseRequestFormatter):
    """Format the outgoing data for ORCID requests."""

    content_type = "application/vnd.orcid+json"

    @classmethod
    def format(cls, data: OptionalJsonType) -> OptionalStr:
        if data:
            return json.dumps(data)

@endpoint(base_url=orcid_prod_public_base_url)
class Endpoint:
    base = ""
    recordId = "{orcidId}/record"
    employmentsId = "{orcidId}/employments"

class Client(APIClient):
        
    @retry_decorator
    def fetch_record_by_unique_id(self, orcid_id, format="digest"):
        logger.info(f"Fetching record for ORCID ID: {orcid_id} with format: {format}")
        result = self.get(Endpoint.recordId.format(orcidId=orcid_id))
        if result:
            logger.debug(f"Record fetched successfully for ORCID ID: {orcid_id}")
            return self._process_record(result, orcid_id, format)
        logger.warning(f"No result found for ORCID ID: {orcid_id}")
        return None
    
    @retry_decorator
    def fetch_employments_by_unique_id(self, orcid_id):
        logger.info(f"Fetching employments for ORCID ID: {orcid_id}")
        result = self.get(Endpoint.employmentsId.format(orcidId=orcid_id))
        if result:
            logger.debug(f"Employments fetched successfully for ORCID ID: {orcid_id}")
            return result
        logger.warning(f"No employments found for ORCID ID: {orcid_id}")
        return None
       
    def _process_record(self, record, orcid_id, format):
        logger.info(f"Processing record for ORCID ID: {orcid_id} with format: {format}")
        # Fetch employments data
        employments = self.fetch_employments_by_unique_id(orcid_id)
        
        if format == "digest":
            logger.debug(f"Extracting digest record info for ORCID ID: {orcid_id}")
            return self._extract_digest_record_info(record, employments)
        elif format == "orcid":
            logger.debug(f"Returning full record for ORCID ID: {orcid_id}")
            return record
        
    def _extract_digest_record_info(self, x, employments):
        """
        Returns
        A list of records dict containing the fields :  wos_id, title, DOI, doctype, pubyear
        """
        logger.info("Extracting digest record information.")
        search_strings = ["EPFL", "École Polytechnique Fédérale de Lausanne"]
        epfl_verif_affiliation = search_json(employments, search_strings)
        record = {
            "orcid_id": x["orcid-identifier"]["path"],
            "firstname": x["person"]["name"]["given-names"]["value"],
            "lastname": x["person"]["name"]["family-name"]["value"],
            "epfl_verif_affiliation": epfl_verif_affiliation
        }
        logger.debug(f"Extracted record: {record}")
        return record

def replace_nulls(json_data):
    """
    Recursively replaces null values in a JSON-like structure with None.

    Args:
        json_data (dict or list): The JSON data object to process.

    Returns:
        dict or list: The processed JSON data with null values replaced by None.
    """
    if isinstance(json_data, dict):
        return {key: replace_nulls(value) for key, value in json_data.items()}
    elif isinstance(json_data, list):
        return [replace_nulls(item) for item in json_data]
    elif json_data is None:
        return None
    return json_data

def search_json(json_data, search_strings):
    """
    Search for multiple strings within a JSON data object.

    Args:
        json_data (dict): The JSON data object to search.
        search_strings (list of str): The list of strings to search for in the JSON data.

    Returns:
        bool: True if any of the search strings are found, False otherwise.
    """
    # Replace null values with None
    json_data = replace_nulls(json_data)

    # Function to recursively search for strings in the JSON data
    def contains_search_string(data):
        if isinstance(data, dict):
            for value in data.values():
                if contains_search_string(value):
                    return True
        elif isinstance(data, list):
            for item in data:
                if contains_search_string(item):
                    return True
        elif isinstance(data, str):
            # Check if any search string is in the current string
            for search_string in search_strings:
                if search_string in data:
                    return True
        return False

    # Perform the search
    return contains_search_string(json_data)

OrcidClient = Client(
    authentication_method=orcid_authentication_method,
    response_handler=JsonResponseHandler,
    request_formatter=OrcidJsonRequestFormatter
)

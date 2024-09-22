from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    paginated,
    BasicAuthentication,
    JsonResponseHandler,
    exceptions,
)
import tenacity
from apiclient.retrying import retry_if_api_request_error
from apiclient.error_handlers import BaseErrorHandler, ErrorHandler
from apiclient.response import Response
from typing import List, Dict
from collections import defaultdict
import ast
import os
from dotenv import load_dotenv
from utils import manage_logger

logger = manage_logger("./logs/api_epfl_client.log")

api_epfl_base_url = "https://api.epfl.ch/v1"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
api_epfl_user = os.environ.get("API_EPFL_USER")
api_epfl_pwd = os.environ.get("API_EPFL_PWD")

api_epfl_authentication_method = BasicAuthentication(username=api_epfl_user, password=api_epfl_pwd)

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

@endpoint(base_url=api_epfl_base_url)
class Endpoint:
    base = ""
    personsQuery = "persons?query={query}"
    personsFirstnameLastname = "persons?firstname={firstname}&lastname={lastname}"
    accredsId = "accreds?persid={sciperID}"
    unitsId = "units/{unitID}"

class Client(APIClient):
    @retry_decorator
    def query_person(self, query, firstname=None, lastname=None, format="sciper", use_firstname_lastname=False):
        logger.info(f"Initiating search for person with query: '{query}' using format: '{format}'")       
        # Initialize results
        result1 = result2 = None

        # If using firstname/lastname
        if use_firstname_lastname:
            logger.info(f"Attempting personsFirstnameLastname for {firstname} {lastname}.")
            result1 = self.get(Endpoint.personsFirstnameLastname.format(firstname=firstname, lastname=lastname))
            logger.debug(f"Received response for {firstname} {lastname} from personsFirstnameLastname: {result1}")

            logger.info(f"Attempting personsFirstnameLastname for {lastname} {firstname}.")
            result2 = self.get(Endpoint.personsFirstnameLastname.format(firstname=lastname, lastname=firstname))
            logger.debug(f"Received response for {lastname} {firstname} from personsFirstnameLastname: {result2}")

        # Always attempt personsQuery
        logger.info(f"Attempting personsQuery for {query}.")
        result_query = self.get(Endpoint.personsQuery.format(query=query))
        logger.debug(f"Received response for {query} from personsQuery: {result_query}")

        # Process results based on the count
        if use_firstname_lastname:
            if result1 and result1["count"] == 1:
                logger.info(f"Single record found for {query} in personsFirstnameLastname (first request). Processing record.")
                return self._process_person_record(result1, query, format)
            elif result2 and result2["count"] == 1:
                logger.info(f"Single record found for {query} in personsFirstnameLastname (second request). Processing record.")
                return self._process_person_record(result2, query, format)

        if result_query and result_query["count"] == 1:
            logger.info(f"Single record found for {query} in personsQuery. Processing record.")
            return self._process_person_record(result_query, query, format)
        elif (result1 and result1["count"] > 1) or (result2 and result2["count"] > 1):
            logger.warning(f"Multiple records found for {query}.")
            return "Plus de 1 résultat dans api.epfl.ch"
        else:
            logger.warning(f"No valid record found for {query}.")
            return "0 résultat dans api.epfl.ch"
    
    @retry_decorator
    def fetch_accred_by_unique_id(self, sciper_id: str, format="digest"):
        logger.info(f"Fetching accreditation for sciper_id: '{sciper_id}' using format: '{format}'")
        result = self.get(Endpoint.accredsId.format(sciperID=sciper_id))
        logger.debug(f"Received response for {sciper_id}: {result}")
        return self._process_accred_record(result, sciper_id, format)
    
    @retry_decorator
    def fetch_unit_by_unique_id(self, unit_id: str, format="digest"):
        logger.info(f"Fetching units for unit_id: '{unit_id}' using format: '{format}'")
        result = self.get(Endpoint.unitsId.format(unitID=unit_id))
        logger.debug(f"Received response for {unit_id}: {result}")
        return self._process_unit_record(result, unit_id, format)
    
    def _process_person_record(self, record, query, format):
        logger.info(f"Processing person record for query: '{query}' with format: '{format}'")
        if format == "sciper":
            logger.debug(f"Extracting sciperId information for {query}.")
            return self._extract_sciper_person_info(record)
        elif format == "digest":
            logger.debug(f"Extracting sciperId and unitIds information for {query}.")
            return self._extract_digest_person_info(record)
        elif format == "epfl":
            logger.debug(f"Returning full record for {query}.")
            return record
        
    def _process_accred_record(self, record, sciper_id, format):
        logger.info(f"Processing accred record for sciper_id: '{sciper_id}' with format: '{format}'")
        
         # Check if 'accreds' is present and not empty
        if 'accreds' not in record or not record['accreds']:
            logger.warning(f"No accreditation records found for sciper_id: '{sciper_id}'.")
            return None  # or return an empty list or dict as needed
        
        if format == "digest":
            logger.debug(f"Extracting sciperId information for {sciper_id}.")
            return [self._extract_accred_units_info(x) for x in record.get("accreds", [])] # to keep the order of units
        elif format == "mainUnit":
            logger.debug(f"Extracting main unit information for {sciper_id}.")
            return self._extract_accred_units_info(record["accreds"][0])
        elif format == "epfl":
            logger.debug(f"Returning full record for {sciper_id}.")
            return record
        
    def _extract_sciper_person_info(self, x):
        logger.info("Extracting sciper person information from the record.")
        sciper_id = x["persons"][0]["id"]
        logger.debug(f"Extracted sciper_id from record: {sciper_id}")
        return sciper_id
    
    def _extract_digest_person_info(self, x):
        logger.info("Extracting digest person information from the record.")
        record = {
            "sciper_id": x["persons"][0]["id"],
            "unitsIds": "|".join([unit["unitid"] for unit in x["persons"][0]["rooms"]])
        }
        logger.debug(f"Extracted digest record: {record}")
        return record
    
    def _extract_accred_units_info(self, x):
        logger.info("Extracting units information from the accred record.")
        unit_type = self.fetch_unit_by_unique_id(str(x["unit"]["id"]))
        record = {
            "unit_id": str(x["unit"]["id"]),
            "unit_name": x["unit"]["name"],
            "unit_type": unit_type
        }
        logger.debug(f"Extracted units from accred record: {record}")
        return record
    
    def _process_unit_record(self, record, unit_id, format):       
        if format == "digest":
            logger.debug(f"Extracting unit type information for {unit_id}.")
            return self._extract_unittype_info(record)
        elif format == "epfl":
            logger.debug(f"Returning full record for {unit_id}.")
            return record
        
    def _extract_unittype_info(self, x):
        logger.info("Extracting unit type information from the unit record.")
        unit_type = None  # Default value
        try:
            unit_type = x.get("unittype", {}).get("label")
            logger.debug(f"Extracted unit type from unit record: {unit_type}")
        except (AttributeError, TypeError):
            logger.warning("Missing 'unittype' or 'label' in the record.")       
        return unit_type

    
ApiEpflClient = Client(
    authentication_method=api_epfl_authentication_method,
    response_handler=JsonResponseHandler,
    error_handler=ErrorHandler
)
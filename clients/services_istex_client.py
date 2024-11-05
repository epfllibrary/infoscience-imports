from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    paginated,
    HeaderAuthentication,
    JsonResponseHandler,
    exceptions,
)
import tenacity
from apiclient.retrying import retry_if_api_request_error
from typing import List, Dict
from collections import defaultdict
import os
import requests
import json
from utils import manage_logger
from config import logs_dir

log_file_path = os.path.join(logs_dir, "services_istex_client.log")
logger = manage_logger(log_file_path)

authors_tools_base_url = "https://authors-tools.services.istex.fr/v1"

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

@endpoint(base_url=authors_tools_base_url)
class Endpoint:
    base = ""
    orcid_disambiguation = "orcid-disambiguation/orcidDisambiguation"

class Client(APIClient):
    @retry_request
    def get_orcid_id(self, **param_kwargs): 
        """
        Retrieves the ORCID ID based on the provided author information.

        Parameters:
        -----------
        id : int
            The ID of the author (default is 1).
        firstname : str
            The first name of the author.
        lastname : str
            The last name of the author.

        Returns:
        --------
        response : Response
            The response from the ORCID API.

        Usage Example:
        ---------------
        response = ServicesIstexClient.get_orcid_id(firstname="S", lastname="Forrer", id=1)
        """
        logger.info("Starting ORCID ID retrieval process.")
        
        param_kwargs.setdefault('id', 1)
        param_kwargs.setdefault('affiliation', 'EPFL')
        self.params = {**param_kwargs}
        
        logger.debug(f"Parameters for ORCID ID request: {self.params}")
        
        payload = json.dumps([
            {
                "id": self.params["id"],
                "value": [
                    {
                        "firstName": self.params["firstname"],
                        "lastName": self.params["lastname"],
                        "affiliations": [
                            self.params["affiliation"]
                        ]
                    }
                ]
            }
        ])
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        logger.debug(f"Payload for ORCID ID request: {payload}")
        
        response = self.post(Endpoint.orcid_disambiguation, headers=headers, data=payload)
        
        logger.info("Received response from ORCID API.")
        
        # Process the response
        if isinstance(response, list):
            logger.debug(f"Response is a list with length: {len(response)}")
            if len(response) == 1:
                logger.info("Single ORCID ID found.")
                return response[0].get('value')  # Return the 'value' key
            elif len(response) > 1:
                logger.info("Multiple ORCID IDs found, concatenating values.")
                return '|'.join(item.get('value') for item in response)  # Concatenate values
        
        logger.warning("No valid ORCID ID found in the response.")
        return None  # Return None for other cases


ServicesIstexClient = Client(
    response_handler=JsonResponseHandler,
)

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
import numpy as np
import os
import requests
import json
from config import LICENSE_CONDITIONS
from utils import manage_logger

logger = manage_logger("./logs/unpaywall_client.log")

unpaywall_base_url = "https://api.unpaywall.org/v2"
email = "air.bib@groupes.epfl.ch"

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)

@endpoint(base_url=unpaywall_base_url)
class Endpoint:
    base = ""
    doi = "/{doi}"
    
class Client(APIClient):
    @retry_request
    def fetch_by_doi(self, doi, format="best-oa-location", **param_kwargs): 

        logger.info("Starting upw DOI retrieval process.")
        
        param_kwargs.setdefault('email', email)
        self.params = {**param_kwargs}
        
        logger.debug(f"Parameters for upw request: {self.params}")
        
        result = self.get(Endpoint.doi.format(doi=doi), params=self.params)
        if result:
            return self._process_fetch_record(result, format)
        return None
  
    def _process_fetch_record(self, x, format):
        if format == "oa":
            return self._extract_oa_infos(x)
        elif format == "best-oa-location":
            return self._extract_best_oa_location_infos(x)
        elif format == "upw":
            return record

    def _extract_oa_infos(self, x):
        rec = {}
        rec["is_oa"] = x["is_oa"]
        rec["oa_status"] = x["oa_status"]
        return rec
    
    def _extract_best_oa_location_infos(self, x):
        rec = self._extract_oa_infos(x)
        logger.info("Extracting OA location infos.")
        
        if rec.get("is_oa") and rec.get("oa_status") in LICENSE_CONDITIONS["allowed_oa_statuses"]: 
            best_oa_location = x.get("best_oa_location")
            license_type = best_oa_location["license"]
            
            result = None
            logger.debug(f"License type: {license_type}")
            
            # Check the license condition using the config
            if license_type is not None and license_type is not np.nan and any(allowed in license_type for allowed in LICENSE_CONDITIONS["allowed_licenses"]):
                # Concatenate non-null URLs
                urls = [
                    best_oa_location["url_for_pdf"],
                    best_oa_location["url_for_landing_page"],
                    best_oa_location["url"]
                ]
                # Filter out None values and concatenate
                result = '|'.join(filter(None, urls))
                logger.info("URLs concatenated successfully.")
            else:
                logger.warning("License type is invalid or not allowed.")
            
            rec["pdf_urls"] = result
        return rec
            
        
        
        
                            
UnpaywallClient = Client(
    response_handler=JsonResponseHandler,
)
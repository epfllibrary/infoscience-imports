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
    def fetch_by_doi(self, doi, format="oa-locations", **param_kwargs): 

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
        elif format == "oa-locations":
            return self._extract_oa_locations_infos(x)
        elif format == "upw":
            return record

    def _extract_oa_infos(self, x):
        rec = {}
        rec["is_oa"] = x["is_oa"]
        rec["oa_status"] = x["oa_status"]
        return rec
    
    def _extract_oa_locations_infos(self, x):
        rec = self._extract_oa_infos(x)
        if rec.get("is_oa") and rec.get("oa_status") in ["gold", "hybrid"]: # add green ?
            # Filter oa_locations for url_for_pdf where version is "acceptedVersion"
            pdf_urls = [
                location["url_for_pdf"] 
                for location in x.get("oa_locations", []) 
                if location.get("version") == "publishedVersion" and "cc-by" in (location.get("license") or "")
            ]
            rec["pdf_urls"] = "|".join([pdf for pdf in pdf_urls])
        return rec
            
        
        
        
                            
UnpaywallClient = Client(
    response_handler=JsonResponseHandler,
)
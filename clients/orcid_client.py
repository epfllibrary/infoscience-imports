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
from apiclient.utils.typing import JsonType
from dotenv import load_dotenv
from utils import get_pipeline_logger

logger = get_pipeline_logger('orcid_client')

orcid_prod_public_base_url = "https://pub.orcid.org/v3.0"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
orcid_api_token = os.environ.get("ORCID_API_TOKEN")
REPOSITORY_SOURCE_NAME = os.environ.get(
    "ORCID_REPOSITORY_SOURCE_NAME",
    "Ecole Polytechnique Fédérale de Lausanne - Infoscience",
)

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
    def format(cls, data: JsonType | None) -> str | None:
        if data:
            return json.dumps(data)

@endpoint(base_url=orcid_prod_public_base_url)
class Endpoint:
    base = ""
    recordId = "{orcidId}/record"
    employmentsId = "{orcidId}/employments"
    worksId = "{orcidId}/works"

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
       
    @retry_decorator
    def fetch_works_by_orcid(
        self,
        orcid_id: str,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[dict]:
        """Return normalized works from /v3.0/{orcid}/works.

        ORCID groups deduplicate across sources natively — one group = one unique
        work, with potentially N summaries (one per claiming source). The first
        summary in each group is used for metadata; all source names are collected
        in ``all_sources``.

        Year filter is applied client-side (the /works endpoint has no server-side
        year parameter). Works with a missing or non-numeric pub_year are kept.

        Each returned dict contains:
            put_code, title, type, pub_year, pub_month, doi, journal,
            source_name, source_type ('institutional'|'self'|'unknown'),
            all_sources (pipe-separated), n_sources, url
        """
        raw = self.get(Endpoint.worksId.format(orcidId=orcid_id))
        if not raw:
            return []
        return self._extract_works(raw, start_year=start_year, end_year=end_year)

    def _extract_works(
        self,
        works_data: dict,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[dict]:
        groups = (works_data or {}).get("group", [])
        results = []
        for group in groups:
            summaries = group.get("work-summary", [])
            if not summaries:
                continue
            ws = summaries[0]

            pub_date = ws.get("publication-date") or {}
            year_raw = (pub_date.get("year") or {}).get("value")
            month_raw = (pub_date.get("month") or {}).get("value")

            try:
                year_int = int(year_raw) if year_raw else None
            except (TypeError, ValueError):
                year_int = None

            if year_int is not None:
                if start_year is not None and year_int < start_year:
                    continue
                if end_year is not None and year_int > end_year:
                    continue

            ext_ids = (ws.get("external-ids") or {}).get("external-id", [])
            doi = next(
                (e["external-id-value"] for e in ext_ids
                 if e.get("external-id-type") == "doi"),
                None,
            )

            title_obj = ws.get("title") or {}
            title = (title_obj.get("title") or {}).get("value")

            journal_obj = ws.get("journal-title") or {}
            journal = journal_obj.get("value")

            src_type, src_name = self._classify_source(ws.get("source"))

            all_sources = list(dict.fromkeys(
                (s.get("source") or {}).get("source-name", {}).get("value", "")
                for s in summaries
                if (s.get("source") or {}).get("source-name", {}).get("value")
            ))

            infoscience_synced = any(REPOSITORY_SOURCE_NAME in s for s in all_sources)

            url_obj = ws.get("url") or {}
            results.append({
                "put_code": ws.get("put-code"),
                "title": title,
                "type": ws.get("type"),
                "pub_year": year_raw,
                "pub_month": month_raw,
                "doi": doi,
                "journal": journal,
                "source_name": src_name,
                "source_type": src_type,
                "all_sources": " | ".join(all_sources),
                "n_sources": len(all_sources),
                "url": url_obj.get("value"),
                "infoscience_synced": infoscience_synced,
            })
        return results

    @staticmethod
    def _classify_source(source_block: dict | None) -> tuple[str, str]:
        """Return (source_type, source_name) from an ORCID source block.

        source_type values:
          'institutional' — pushed by an API client (source-client-id present)
          'self'          — manually added by the researcher (source-orcid present)
          'unknown'       — source block absent or unrecognised
        """
        if not source_block:
            return "unknown", ""
        src_name = (source_block.get("source-name") or {}).get("value", "")
        if source_block.get("source-client-id"):
            return "institutional", src_name
        if source_block.get("source-orcid"):
            return "self", src_name
        return "unknown", src_name

    @retry_decorator
    def fetch_name_variants(self, orcid_id: str) -> list[str]:
        """Return other-name values from /record as a list of strings.

        Used by researcher_monitor/registry.py to populate name_variants in
        researcher_registry for fuzzy matching during gap analysis.
        """
        raw = self.get(Endpoint.recordId.format(orcidId=orcid_id))
        if not raw:
            return []
        entries = (
            (raw.get("person") or {})
            .get("other-names", {})
            .get("other-name", [])
        ) or []
        return [
            e["content"].strip()
            for e in entries
            if e.get("content") and str(e["content"]).strip()
        ]

    @retry_decorator
    def fetch_external_identifiers(self, orcid_id: str) -> dict[str, str]:
        """Return external identifiers from /record as {type: value}.

        E.g. {"Scopus Author ID": "7003899513", "ResearcherID": "A-1234-2010"}

        Used by researcher_monitor/registry.py to seed scopus_author_id and
        researcher_id columns in researcher_registry without an extra API call.
        """
        raw = self.get(Endpoint.recordId.format(orcidId=orcid_id))
        if not raw:
            return {}
        entries = (
            (raw.get("person") or {})
            .get("external-identifiers", {})
            .get("external-identifier", [])
        ) or []
        result = {}
        for e in entries:
            id_type = (e.get("external-id-type") or "").strip()
            id_value = (e.get("external-id-value") or "").strip()
            if id_type and id_value:
                result[id_type] = id_value
        return result

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

import os
from typing import List
import tenacity
from apiclient import APIClient, endpoint, retry_request, JsonResponseHandler
from apiclient.retrying import retry_if_api_request_error
from dotenv import load_dotenv
from utils import manage_logger
from config import logs_dir
import mappings

# Base URL for CrossRef API
crossref_api_base_url = "https://api.crossref.org"

# Load environment variables
load_dotenv(os.path.join(os.getcwd(), ".env"))
crossref_email = os.environ.get("CROSSREF_EMAIL")  # Required for polite API usage

accepted_doctypes = [
    key for key in mappings.doctypes_mapping_dict["source_crossref"].keys()
]

# Retry decorator to handle request retries on specific status codes
retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=crossref_api_base_url)
class CrossRefEndpoint:
    works = "works"
    work_id = "works/{doi}"


class CrossRefClient(APIClient):
    """Client for interacting with the CrossRef API."""

    log_file_path = os.path.join(os.getcwd(), "logs", "crossref_client.log")
    logger = manage_logger(log_file_path)

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Basic search query in the CrossRef API.

        Example request:
        https://api.crossref.org/works?query=climate+change&rows=5

        Usage:
        CrossRefClient.search_query(query="climate change", rows=5)

        Args:
            param_kwargs: Query parameters such as 'query', 'rows', etc.

        Returns:
            A JSON object containing search results from CrossRef.
        """
        param_kwargs.setdefault("mailto", crossref_email)
        self.params = {**param_kwargs}
        return self.get(CrossRefEndpoint.works, params=self.params)

    @retry_request
    def count_results(self, **param_kwargs) -> int:
        """
        Counts the number of results for a given query.

        Example request:
        https://api.crossref.org/works?query=climate+change&rows=1

        Usage:
        CrossRefClient.count_results(query="climate change")

        Args:
            param_kwargs: Query parameters such as 'query'.

        Returns:
            The total count of results for the query.
        """
        param_kwargs.setdefault("rows", 1)
        response = self.search_query(**param_kwargs)
        return response.get("message", {}).get("total-results", 0)

    @retry_decorator
    def fetch_ids(self, **param_kwargs) -> List[str]:
        """
        Retrieves a list of DOIs for a specified query.

        Example request:
        https://api.crossref.org/works?query=climate+change&rows=10

        Usage:
        CrossRefClient.fetch_ids(query="climate change", rows=10)

        Args:
            param_kwargs: Query parameters such as 'query', 'rows', etc.

        Returns:
            A list of DOIs from CrossRef.
        """
        param_kwargs.setdefault("rows", 10)
        response = self.search_query(**param_kwargs)
        items = response.get("message", {}).get("items", [])

        return [item.get("DOI") for item in items if "DOI" in item]

    @retry_decorator
    def fetch_records(self, format="digest", **param_kwargs):
        """
        Fetch records from CrossRef API, processing them into the specified format.

        Args:
            format (str): Desired format for output records. Options are 'digest' or 'raw'.
            **param_kwargs: Additional parameters for querying CrossRef.

        Returns:
            list or None: Processed records in the specified format, or None if no records are found.
        """
        param_kwargs.setdefault("rows", 10)

        result = self.search_query(**param_kwargs)

        if result["message"]["total-results"] > 0:
            return self._process_fetch_records(format, **result["message"])

        return None

    @retry_decorator
    def fetch_record_by_doi(self, doi: str):
        """
        Retrieves a specific record by its unique DOI.

        Example request:
        https://api.crossref.org/works/{doi}

        Usage:
        CrossRefClient.fetch_record_by_doi("10.1000/xyz123")

        Args:
            doi: The DOI of the record to fetch.

        Returns:
            A dictionary containing detailed record information.
        """
        if not doi:
            self.logger.warning("No valid DOI provided. Aborting the request.")
            return None

        try:
            url = CrossRefEndpoint.work_id.format(doi=doi)
            result = self.get(url, headers={"Accept": "application/json"})
            return result.get("message", {})

        except Exception as e:
            self.logger.error(f"Error fetching record by DOI {doi}: {e}")
            return None

    def _process_fetch_records(self, format, **data):
        """
        Process fetched records into the desired output format.

        Args:
            format (str): Output format ('digest' or 'raw').
            **data: Data returned from the search query.

        Returns:
            list: Processed records in the requested format.
        """

        if format == "digest":
            return [
                self._extract_digest_record_info(record)
                for record in data.get("items", [])
            ]

        elif format == "raw":
            return data.get("items", [])

    def _extract_digest_record_info(self, record):
        """
        Extract minimal information for digest format from a single CrossRef record.

        Args:
            record (dict): A single CrossRef record.

        Returns:
            dict: Extracted information in digest format.
        """

        return {
            "source": "crossref",
            "doi": record.get("DOI"),
            "title": record.get("title")[0] if record.get("title") else None,
            "type": record.get("type"),
            "published_year": record.get("published-print", {}).get(
                "date-parts", [[None]]
            )[0][0],
            "authors": [
                author.get("given") + " " + author.get("family")
                for author in record.get("author", [])
            ],
            "publisher": record.get("publisher"),
            "journal": (
                record.get("container-title")[0]
                if record.get("container-title")
                else None
            ),
            "issn": "|".join(record.get("ISSN", [])),
            "license": (
                record.get("license")[0].get("URL") if record.get("license") else None
            ),
            "abstract": record.get("abstract"),
        }

from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    paginated,
    JsonResponseHandler,
    exceptions,
)
import tenacity
from apiclient.retrying import retry_if_api_request_error
from typing import List, Dict
import os
import traceback
from dotenv import load_dotenv
from utils import manage_logger
import mappings
from config import logs_dir

# Base URL for OpenAlex API
openalex_api_base_url = "https://api.openalex.org"

# Load environment variables
load_dotenv(os.path.join(os.getcwd(), ".env"))
openalex_email = os.environ.get("OPENALEX_EMAIL")

accepted_doctypes = [
    key for key in mappings.doctypes_mapping_dict["source_openalex"].keys()
]

# Retry decorator to handle request retries on specific status codes
retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=openalex_api_base_url)
class OpenAlexEndpoint:
    works = "works"
    work_id = "works/{openalexId}"


class OpenAlexClient(APIClient):
    log_file_path = os.path.join(logs_dir, "openalex_client.log")
    logger = manage_logger(log_file_path)

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Basic search query in the OpenAlex API.

        Example request:
        https://api.openalex.org/works?filter=title.search:cadmium&per_page=5&page=1

        Usage:
        OpenAlexClient.search_query(filter="title.search:cadmium", per_page=5, page=1)

        Returns:
        A JSON object containing search results from OpenAlex.
        """
        param_kwargs.setdefault("email", openalex_email)
        self.params = {**param_kwargs}
        return self.get(OpenAlexEndpoint.works, params=self.params)

    @retry_request
    def count_results(self, **param_kwargs) -> int:
        """
        Counts the number of results for a given query.

        Example request:
        https://api.openalex.org/works?filter=title.search:cadmium&per_page=1&page=1

        Usage:
        OpenAlexClient.count_results(filter="title.search:cadmium")

        Returns:
        The total count of results for the query.
        """
        param_kwargs.setdefault("email", openalex_email)
        param_kwargs.setdefault("per_page", 1)
        param_kwargs.setdefault("page", 1)
        self.params = {**param_kwargs}
        return self.search_query(**self.params)["meta"]["count"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs) -> List[str]:
        """
        Retrieves a list of OpenAlex IDs for a specified query.

        Example request:
        https://api.openalex.org/works?filter=title.search:cadmium&per_page=10&page=1

        Usage:
        OpenAlexClient.fetch_ids(filter="title.search:cadmium", per_page=10)

        Returns:
        A list of IDs from OpenAlex.
        """
        param_kwargs.setdefault("email", openalex_email)
        param_kwargs.setdefault("per_page", 10)
        param_kwargs.setdefault("page", 1)
        self.params = {**param_kwargs}
        return [x["id"] for x in self.search_query(**self.params)["results"]]

    @retry_decorator
    def fetch_records(self, format="digest", **param_kwargs):
        """
        Fetch records from OpenAlex API, processing them into the specified format.

        Args:
            format (str): Desired format for output records. Options are 'digest', 'digest-ifs3', 'ifs3', or 'openalex'.
            **param_kwargs: Additional parameters for querying OpenAlex.

        Returns:
            list or None: Processed records in the specified format, or None if no records are found.
        """
        param_kwargs.setdefault("email", openalex_email)
        self.params = param_kwargs
        result = self.search_query(**self.params)
        if result["meta"]["count"] > 0:
            return self._process_fetch_records(format, **self.params)
        return None

    @retry_decorator
    def fetch_record_by_unique_id(self, openalex_id):
        """
        Retrieves a specific record by its unique OpenAlex ID.

        Example request:
        https://api.openalex.org/works/W2762925973

        Usage:
        OpenAlexClient.fetch_record_by_unique_id("W2762925973")

        Returns:
        A record with key fields.
        """
        self.params = {"email": openalex_email}
        result = self.get(
            OpenAlexEndpoint.work_id.format(openalexId=openalex_id), params=self.params
        )
        return self._process_record(result, format) if result else None

    def _process_fetch_records(self, format, **param_kwargs):
        """
        Process fetched records into the desired output format.

        Args:
            format (str): Output format ('digest', 'digest-ifs3', 'ifs3', or 'openalex').
            **param_kwargs: Parameters for querying OpenAlex API.

        Returns:
            list: Processed records in the requested format.
        """
        if format == "digest":
            return [
                self._extract_digest_record_info(record)
                for record in self.search_query(**self.params)["results"]
            ]
        elif format == "digest-ifs3":
            return [
                self._extract_ifs3_digest_record_info(record)
                for record in self.search_query(**self.params)["results"]
            ]
        elif format == "ifs3":
            return [
                self._extract_ifs3_record_info(record)
                for record in self.search_query(**self.params)["results"]
            ]
        elif format == "openalex":
            return self.search_query(**self.params)["results"]

    def _process_record(self, record, format):
        if format == "digest":
            return self._extract_digest_record_info(record)
        elif format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(record)
        elif format == "ifs3":
            return self._extract_ifs3_record_info(record)
        elif format == "openalex":
            return record

    def _extract_digest_record_info(self, record):
        """
        Extract minimal information for digest format from a single OpenAlex record.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            dict: Extracted information in digest format.
        """
        return {
            "source": "openalex",
            "internal_id": record["id"],
            "doi": self._extract_doi(record), 
            "title": record.get("display_name"),
            "doctype": self._extract_first_doctype(record),
            "pubyear": record.get("publication_year"),
        }

    def _extract_ifs3_digest_record_info(self, record):
        """
        Extract additional information for ifs3-digest format.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            dict: Extracted information in ifs3-digest format.
        """
        digest_info = self._extract_digest_record_info(record)
        digest_info["ifs3_doctype"] = self._extract_ifs3_doctype(record)
        digest_info["ifs3_collection_id"] = self._extract_ifs3_collection_id(record)
        return digest_info

    def _extract_ifs3_record_info(self, record):
        """
        Extract detailed information for ifs3 format.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            dict: Extracted information in ifs3 format.
        """
        ifs3_info = self._extract_ifs3_digest_record_info(record)
        ifs3_info["authors"] = self._extract_ifs3_authors(record)
        return ifs3_info

    def _extract_doi(self, record):
        """
        Extract DOI from an OpenAlex record, removing the prefix 'https://doi.org/'.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            str: DOI without the 'https://doi.org/' prefix, or an empty string if DOI is None.
        """
        doi = record.get("doi", "")
        if isinstance(doi, str) and doi.startswith("https://doi.org/"):
            return doi[len("https://doi.org/") :]  # Remove the DOI prefix
        return (
            doi.lower() if isinstance(doi, str) else ""
        )

    def _extract_first_doctype(self, record):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return record.get("type")

    def _extract_ifs3_doctype(self, record):
        """
        Map OpenAlex document type to `ifs3` document type.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            str: Mapped document type for ifs3.
        """
        doctype = self._extract_first_doctype(record)
        if doctype in accepted_doctypes:
            return mappings.doctypes_mapping_dict["source_openalex"].get(
                doctype, "unknown_doctype"
            )
        return "unknown_doctype"

    def _extract_ifs3_collection_id(self, record):
        """
        Map OpenAlex document type to a collection ID in the `ifs3` schema.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            str: Collection ID in ifs3 schema.
        """
        ifs3_doctype = self._extract_ifs3_doctype(record)
        if ifs3_doctype != "unknown_doctype":
            return mappings.collections_mapping.get(ifs3_doctype, "unknown_collection")
        return "unknown_collection"

    def _extract_author_orcid(self, author):
        """
        Extract ORCID from an author's information, removing the 'https://orcid.org/' prefix.

        Args:
            author (dict): A dictionary containing author information, including ORCID.

        Returns:
            str: ORCID without the 'https://orcid.org/' prefix, or an empty string if ORCID is None.
        """
        orcid = author.get("orcid", "")
        if isinstance(orcid, str) and orcid.startswith("https://orcid.org/"):
            return orcid[len("https://orcid.org/") :]  # Remove the ORCID prefix
        return (
            orcid if isinstance(orcid, str) else ""
        )

    def _extract_ifs3_authors(self, record):
        """
        Extract author information for ifs3 format from a single OpenAlex record.

        Args:
            record (dict): A single OpenAlex record.

        Returns:
            list of dict: List of author information dictionaries.
        """
        authors = []
        try:
            for author in record.get("authorships", []):
                institutions = "|".join(
                    [
                        f"{inst.get('ror', '').split('/')[-1]}:{inst.get('display_name', '')}"
                        for inst in author.get("institutions", [])
                    ]
                )
                authors.append(
                    {
                        "author": author["author"]["display_name"],
                        "internal_author_id": author["author"]["id"],
                        "orcid_id": self._extract_author_orcid(author["author"]),
                        "organizations": institutions,
                    }
                )
        except KeyError:
            self.logger.warning(
                f"Missing authorship information for record {record.get('id', 'unknown')}"
            )
        return authors


# Initialize the OpenAlexClient with a JSON response handler
OpenAlexClient = OpenAlexClient(
    response_handler=JsonResponseHandler,
)

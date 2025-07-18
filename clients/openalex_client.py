"""OpenAlex client for Infoscience imports"""

import os
import re
from typing import List
import tenacity
from nameparser import HumanName
from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    JsonResponseHandler,
)
from apiclient.retrying import retry_if_api_request_error
from dotenv import load_dotenv
from config import logs_dir
from utils import manage_logger
import mappings

# Base URL for OpenAlex API
openalex_api_base_url = "https://api.openalex.org"

# Load environment variables
load_dotenv(os.path.join(os.getcwd(), ".env"))
openalex_email = os.environ.get("CONTACT_API_EMAIL")

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


@endpoint(base_url=openalex_api_base_url)
class OpenAlexEndpoint:
    works = "works"
    work_id = "works/{openalexId}"


class Client(APIClient):
    log_file_path = os.path.join(logs_dir, "logging.log")
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
        response = self.get(OpenAlexEndpoint.works, params=self.params)

        # 🟢 Stocke la dernière réponse ici
        self.last_response = response

        return response

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
        param_kwargs.setdefault("per_page", 50)
        # Curseur initial
        cursor = param_kwargs.pop("cursor", "*")

        all_ids: List[str] = []

        while True:
            # On inclut le curseur à chaque requête
            self.params = {**param_kwargs, "cursor": cursor}
            response = self.search_query(**self.params)

            results = response.get("results", [])
            if not results:
                break

            for record in results:
                raw_doi = record.get("doi")
                if raw_doi:
                    doi_id = re.sub(r'^https?://(?:dx\.)?doi\.org/', '', raw_doi, flags=re.IGNORECASE)
                    all_ids.append(doi_id)
                else:
                    all_ids.append(record["id"])

            # Passage au curseur suivant
            cursor = response.get("meta", {}).get("next_cursor")
            if not cursor:
                break

        return all_ids

    @retry_decorator
    def fetch_records(self, format="digest", **param_kwargs):
        """
        Fetch all records from OpenAlex API using cursor-based pagination.

        Args:
            format (str): Desired format for output records. Options: 'digest', 'digest-ifs3', 'ifs3', or 'openalex'.
            **param_kwargs: Parameters for querying OpenAlex (e.g., filter, per_page).

        Returns:
            list: Processed records in the specified format.
        """
        param_kwargs.setdefault("email", openalex_email)
        param_kwargs.setdefault("per_page", 50)
        cursor = param_kwargs.pop("cursor", "*")

        all_records = []

        while True:
            self.params = {**param_kwargs, "cursor": cursor}
            response = self.search_query(**self.params)

            results = response.get("results", [])
            if not results:
                break

            for record in results:
                parsed = self._process_record(record, format)
                if parsed:
                    all_records.append(parsed)

            cursor = response.get("meta", {}).get("next_cursor")
            if not cursor:
                break

        return all_records

    @retry_decorator
    def fetch_record_by_unique_id(self, openalex_id, format="digest"):
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

    def _process_record(self, x, format):
        if format == "digest":
            return self._extract_digest_record_info(x)
        elif format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(x)
        elif format == "ifs3":
            return self._extract_ifs3_record_info(x)
        elif format == "openalex":
            return x

    def _extract_digest_record_info(self, x):
        """
        Extract minimal information for digest format from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            dict: Extracted information in digest format.
        """
        return {
            "source": "openalex",
            "internal_id": x["id"],
            "issueDate": self._extract_publication_date(x),
            "doi": self.openalex_extract_doi(x),
            "title": x.get("display_name", ""),
            "doctype": self._extract_first_doctype(x),
            "pubyear": x.get("publication_year"),
            "publisher": self._extract_publisher(x),
            "publisherPlace": "",
            "journalTitle": self._extract_journal_title(x),
            "seriesTitle": "",
            "bookTitle": "",
            "editors": "",
            "journalISSN": self._extract_journal_issn(x),
            "seriesISSN": "",
            "bookISBN": "",
            "bookDOI": "",
            "journalVolume": self._extract_volume(x),
            "seriesVolume": "",
            "bookPart": "",
            "issue": self._extract_issue(x),
            "startingPage": self._extract_starting_page(x),
            "endingPage": self._extract_ending_page(x),
            "pmid": "",
            "artno": x.get("biblio", {}).get("article_number", ""),
            "keywords": self._extract_keywords(x),
        }

    def _extract_ifs3_digest_record_info(self, x):
        """
        Extract additional information for ifs3-digest format.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            dict: Extracted information in ifs3-digest format.
        """
        digest_info = self._extract_digest_record_info(x)
        digest_info["ifs3_collection"] = self._extract_ifs3_collection(x)
        digest_info["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        # Get dc.type and dc.type_authority for the document type
        dc_type_info = self.get_dc_type_info(x)
        # Add dc.type and dc.type_authority to the record
        digest_info["dc.type"] = dc_type_info["dc.type"]
        digest_info["dc.type_authority"] = dc_type_info["dc.type_authority"]
        return digest_info

    def _extract_ifs3_record_info(self, x):
        """
        Extract detailed information for ifs3 format.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            dict: Extracted information in ifs3 format.
        """
        ifs3_info = self._extract_ifs3_digest_record_info(x)
        ifs3_info["abstract"] = self.extract_abstract(x)
        ifs3_info["authors"] = self.extract_ifs3_authors(x)
        return ifs3_info

    def openalex_extract_doi(self, x):
        """
        Extract DOI from an OpenAlex record, removing the prefix 'https://doi.org/'.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: DOI without the 'https://doi.org/' prefix, or an empty string if DOI is None.
        """
        doi = x.get("doi", "")
        if isinstance(doi, str) and doi.startswith("https://doi.org/"):
            return doi[len("https://doi.org/") :]  # Remove the DOI prefix
        return (
            doi.lower() if isinstance(doi, str) else ""
        )

    def _extract_first_doctype(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("type_crossref")

    def get_dc_type_info(self, x):
        """
        Retrieves the dc.type and dc.type_authority attributes for a given document type.

        :param data_doctype: The document type (e.g., "Article", "Proceedings Paper", etc.)
        :return: A dictionary with the keys "dc.type" and "dc.type_authority", or "unknown" if not found.
        """
        data_doctype = self._extract_first_doctype(x)
        # Access the doctype mapping for "source_wos"
        doctype_mapping = mappings.doctypes_mapping_dict.get("source_crossref", {})
        # Check if the document type exists in the mapping for dc.type
        document_info = doctype_mapping.get(data_doctype, None)
        dc_type = (
            document_info.get("dc.type", "unknown") if document_info else "unknown"
        )

        dc_type_authority = mappings.types_authority_mapping.get(dc_type, "unknown")

        # Return the dc.type and dc.type_authority
        return {
            "dc.type": dc_type,
            "dc.type_authority": dc_type_authority,
        }

    def _extract_ifs3_collection(self, x):
        # Extract the document type
        data_doctype = self._extract_first_doctype(x)
        # Check if the document type is accepted
        if data_doctype in accepted_doctypes:
            mapped_value = mappings.doctypes_mapping_dict["source_crossref"].get(
                data_doctype
            )

            if mapped_value is not None:
                # Return the mapped collection value
                return mapped_value.get("collection", "unknown")
            else:
                # Log or handle the case where the mapping is missing
                self.logger.warning(
                    f"Mapping not found for data_doctype: {data_doctype}"
                )
                return "unknown"  # or any other default value
        return "unknown"  # or any other default value

    def _extract_ifs3_collection_id(self, x):
        ifs3_collection = self._extract_ifs3_collection(x)
        # Check if the collection is not "unknown"
        if ifs3_collection != "unknown":
            # Assume ifs3_collection is a string and access mappings accordingly
            collection_info = mappings.collections_mapping.get(ifs3_collection, None)
            if collection_info:
                return collection_info["id"]
        return "unknown"

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

    def extract_ifs3_authors(self, x):
        """
        Extract author information for ifs3 format from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            list of dict: List of author information dictionaries.
        """
        authors = []
        try:
            for author in x.get("authorships", []):
                institutions = "|".join(
                    [
                        f"{inst.get('ror', '').split('/')[-1]}:{inst.get('display_name', '')}"
                        for inst in author.get("institutions", [])
                    ]
                )
                raw_name = author["author"]["display_name"]
                formatted_name = self._format_authorname(raw_name)

                authors.append(
                    {
                        "author": formatted_name,
                        "internal_author_id": author["author"]["id"],
                        "orcid_id": self._extract_author_orcid(author["author"]),
                        "organizations": institutions,
                    }
                )
        except KeyError:
            self.logger.warning(
                f"Missing authorship information for record {x.get('id', 'unknown')}"
            )
        return authors


    def extract_abstract(self, x):
        """
        Reconstruit l'abstract depuis abstract_inverted_index.
        """
        try:
            index = x.get("abstract_inverted_index")
            if not index or not isinstance(index, dict):
                return ""

            position_map = {}
            for word, positions in index.items():
                for pos in positions:
                    position_map[pos] = word

            abstract = " ".join(position_map[i] for i in sorted(position_map))
            return abstract.strip()
        except Exception as e:
            self.logger.error(f"Error extracting abstract: {e}")
            return ""

    def _extract_publication_date(self, x):
        try:
            date_parts = x.get("publication_date", "")
            return date_parts if date_parts else ""
        except Exception as e:
            self.logger.error(f"Error extracting publication date: {e}")
            return ""

    def _extract_journal_title(self, x):
        return x.get("primary_location", {}).get("source", {}).get("display_name", "")

    def _extract_journal_issn(self, x):
        issn = x.get("primary_location", {}).get("source", {}).get("issn_l", "")
        return issn if isinstance(issn, str) else "||".join(issn)

    def _extract_publisher(self, x):
        return x.get("primary_location", {}).get("source", {}).get("publisher", "")

    def _extract_volume(self, x):
        return x.get("biblio", {}).get("volume", "")

    def _extract_issue(self, x):
        return x.get("biblio", {}).get("issue", "")

    def _extract_starting_page(self, x):
        return x.get("biblio", {}).get("first_page", "")

    def _extract_ending_page(self, x):
        return x.get("biblio", {}).get("last_page", "")

    def _extract_keywords(self, x):
        concepts = x.get("concepts", [])
        return "||".join([c.get("display_name", "") for c in concepts])

    @staticmethod
    def _format_authorname(raw: str) -> str:
        """
        Formate un nom complet en "Nom, Prénom(s) Initiales" en conservant tous les middle names et initiales.
        """
        nm = HumanName(raw)
        given_parts = []
        if nm.first:
            given_parts.append(nm.first)
        if nm.middle:
            # splitte les middle names/initiales (ex: "D. P.")
            given_parts += nm.middle.split()
        given_str = " ".join(given_parts)
        return f"{nm.last}, {given_str}"


# Initialize the OpenAlexClient with a JSON response handler
OpenAlexClient = Client(
    response_handler=JsonResponseHandler,
)

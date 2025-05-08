"""
Python client for the Datacite API
This client provides methods to interact with the Datacite API
"""

import os
import re
from typing import List, Dict, Optional
import tenacity
from apiclient import APIClient, endpoint, retry_request, JsonResponseHandler
from apiclient.retrying import retry_if_api_request_error
from config import logs_dir
from utils import manage_logger
import mappings


# Base URL for DataCite Public API
DATACITE_API_BASE_URL = "https://api.datacite.org"
# Default pagination size
DEFAULT_PAGE_SIZE = 50

# List of accepted document types (using the same mapping as for OpenAlex to ensure compatibility)
accepted_doctypes = [
    key for key in mappings.doctypes_mapping_dict["source_datacite"].keys()
]


@endpoint(base_url=DATACITE_API_BASE_URL)
class DataCiteEndpoint:
    dois = "dois"
    doi = "dois/{doi}"
    prefixes = "prefixes/{prefix}"


class Client(APIClient):
    """
    Python client for the DataCite Public API.
    """

    log_file_path = os.path.join(logs_dir, "logging.log")
    logger = manage_logger(log_file_path)

    retry_decorator = tenacity.retry(
        retry=retry_if_api_request_error(status_codes=[429]),
        wait=tenacity.wait_fixed(2),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )

    @retry_request
    def search_query(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict[str, str]] = None,
        page_number: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        **extra_params,
    ) -> Dict:
        """
        Search DataCite DOIs with classic numbered pagination.

        Args:
            query (str): Full-text search term.
            filters (dict): Additional query filters (e.g., {"created": "2024,2025"}).
            page_number (int): Page number to retrieve.
            page_size (int): Number of records per page.

        Returns:
            dict: Parsed JSON response from DataCite.
        """
        params: Dict[str, str] = {}
        if query:
            params["query"] = query
        if filters:
            params.update(filters)
        params["page[number]"] = str(page_number)
        params["page[size]"] = str(page_size)
        params.update({k: str(v) for k, v in extra_params.items()})

        self.logger.info(f"Querying page {page_number} with page size {page_size}")
        return self.get(DataCiteEndpoint.dois, params=params)

    def count_results(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict[str, str]] = None,
        **extra_params,
    ) -> int:
        """
        Count the total number of results for a given query and filters.

        Returns:
            int: Total number of matching records.
        """
        result = self.search_query(
            query=query,
            filters=filters,
            page_number=1,
            page_size=1,
            **extra_params,
        )
        return result.get("meta", {}).get("total", 0)

    @retry_decorator
    def fetch_ids(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict[str, str]] = None,
        max_pages: Optional[int] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        **extra_params,
    ) -> List[str]:
        """
        Retrieve a list of DOIs across paginated results.

        Returns:
            List[str]: List of DOI identifiers.
        """
        all_ids = []
        page_number = 1
        pages_fetched = 0

        while True:
            result = self.search_query(
                query=query,
                filters=filters,
                page_number=page_number,
                page_size=page_size,
                **extra_params,
            )
            items = result.get("data", [])
            if not items:
                break

            all_ids.extend([item.get("id", "") for item in items])
            pages_fetched += 1
            page_number += 1

            if max_pages and pages_fetched >= max_pages:
                break

            meta = result.get("meta", {})
            total_pages = meta.get("totalPages")
            if total_pages and page_number > total_pages:
                break

        return all_ids

    @retry_decorator
    def fetch_records(
        self,
        format: str = "digest",
        query: Optional[str] = None,
        filters: Optional[Dict[str, str]] = None,
        max_pages: Optional[int] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        **extra_params,
    ) -> List:
        """
        Fetch and process records using classic page-based pagination.

        Args:
            format (str): One of "digest", "digest-ifs3", "ifs3", or "datacite".
            query (str): Full-text search.
            filters (dict): Filter dictionary (e.g. {"created": "2024,2025"}).

        Returns:
            List[dict]: List of processed records.
        """
        all_items = []
        page_number = 1
        pages_fetched = 0

        while True:
            response = self.search_query(
                query=query,
                filters=filters,
                page_number=page_number,
                page_size=page_size,
                **extra_params,
            )
            batch = response.get("data", [])
            if not batch:
                break

            all_items.extend(batch)
            pages_fetched += 1
            page_number += 1

            if max_pages and pages_fetched >= max_pages:
                break

            meta = response.get("meta", {})
            total_pages = meta.get("totalPages")
            if total_pages and page_number > total_pages:
                break

        return self._process_fetch_records(all_items, format)

    @retry_decorator
    def fetch_record_by_unique_id(self, doi: str, format: str = "digest"):
        """
        Retrieve a single record by DOI.
        """
        response = self.get(DataCiteEndpoint.doi.format(doi=doi), params={})
        data = response.get("data") if response else None
        return self._process_record(data, format) if data else None

    def _process_fetch_records(self, items: List[Dict], format: str) -> List:
        if format == "datacite":
            return items
        if format == "digest":
            return [self._extract_digest_record_info(item) for item in items]
        if format == "digest-ifs3":
            return [self._extract_ifs3_digest_record_info(item) for item in items]
        if format == "ifs3":
            return [self._extract_ifs3_record_info(item) for item in items]
        return []

    def _process_record(self, x: Dict, format: str):
        if format == "datacite":
            return x
        if format == "digest":
            return self._extract_digest_record_info(x)
        if format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(x)
        if format == "ifs3":
            return self._extract_ifs3_record_info(x)
        return None

    def _extract_digest_record_info(self, x: Dict) -> Dict:
        """
        Extracts core metadata fields from a DataCite record using the 'container' object for publication details.
        """
        attrs = x.get("attributes", {})
        doi = x.get("id", "")

        # Basic title
        titles = attrs.get("titles", []) or []
        title = titles[0].get("title", "").strip() if titles else ""

        # Publication type
        doctype = self._extract_first_doctype(x)

        # Date and year
        issue_date = self._extract_pubdate(attrs)
        pubyear = attrs.get("publicationYear")

        # Version
        version = attrs.get("version", "")

        # Contributors: editors and corporate authors
        contributors_obj = attrs.get("contributors", []) or []
        editors = "||".join(
            [
                c.get("name", "")
                for c in contributors_obj
                if c.get("contributorType", "").lower() == "editor"
            ]
        )
        contributors = "||".join(
            [
                c.get("name", "")
                for c in contributors_obj
                if c.get("contributorType", "").lower() != "editor"
            ]
        )

        # Identifiers for ISBN/PMID
        identifiers = attrs.get("identifiers", []) or []
        book_isbns = identifiers or "||".join(
            [
                i.get("identifier", "")
                for i in identifiers
                if i.get("identifierType", "").lower() == "isbn"
            ]
        )
        pmid = next(
            (
                i.get("identifier", "")
                for i in identifiers
                if i.get("identifierType", "").lower() == "pmid"
            ),
            "",
        )

        # Keywords
        subjects = attrs.get("subjects", []) or []
        keywords = "||".join([s.get("subject", "") for s in subjects])

        # Extract related items like book chapters, journal issues, etc.
        related_items_info = self._extract_related_items(attrs)

        return {
            "source": "datacite",
            "internal_id": doi,
            "issueDate": issue_date,
            "doi": doi.lower(),
            "title": title,
            "doctype": doctype,
            "pubyear": pubyear,
            "publisher": self._extract_publisher(x),
            "editors": editors,
            "pmid": pmid,
            "artno": "",
            "contributors": contributors,
            "keywords": keywords,
            "version": version,
            **related_items_info,  # Add the related items info here
        }

    def _extract_publisher(self, x: Dict) -> str:
        """
        Extracts the publisher
        """
        # Extract the attributes from the row x
        attrs = x.get("attributes", {})

        # Try to get the publisher directly from the 'publisher' field
        publisher = attrs.get("publisher", "")

        # If 'publisher' is an object (like {"name": "Zenodo"}), get the 'name' field
        if isinstance(publisher, dict) and "name" in publisher:
            publisher = publisher.get("name", "")

        # Return the publisher as a string
        return str(publisher)   

    def _extract_related_items(self, attrs: Dict) -> Dict:
        """
        Extracts related item metadata from the 'relatedItems' field.
        This includes information about books, journals, conference proceedings, etc.
        """
        related_items = attrs.get("relatedItems", [])
        related_info = {}

        for item in related_items:
            related_item_type = item.get("relatedItemType", "").lower()
            # Extracting identifiers based on the relatedItemIdentifierType
            related_item_identifier = item.get("relatedItemIdentifier", {}).get(
                "relatedItemIdentifier", ""
            )
            related_item_identifier_type = (
                item.get("relatedItemIdentifier", {})
                .get("relatedItemIdentifierType", "")
                .lower()
            )

            # Handle different related item types
            if related_item_type in ["book", "journal", "conferenceproceedings"]:
                # For Book or BookChapter
                if (
                    related_item_type in ["book", "conferenceproceedings"]
                    and item.get("relationType") == "IsPublishedIn"
                ):
                    related_info.update(
                        {
                            "bookTitle": item.get("titles", [{}])[0].get("title", ""),
                            "bookVolume": item.get("volume", ""),
                            "bookEdition": item.get("edition", ""),
                            "bookPart": item.get("number", ""),
                            "startingPage": item.get("firstPage", ""),
                            "endingPage": item.get("lastPage", ""),
                        }
                    )
                    if related_item_identifier_type == "isbn":
                        related_info["bookISBN"] = related_item_identifier
                    elif related_item_identifier_type == "doi":
                        related_info["bookDOI"] = related_item_identifier

                # For Journal (Article)
                elif (
                    related_item_type == "journal"
                    and item.get("relationType") == "IsPublishedIn"
                ):
                    related_info.update(
                        {
                            "journalTitle": item.get("titles", [{}])[0].get("title", ""),
                            "journalVolume": item.get("volume", ""),
                            "journalIssue": item.get("issue", ""),
                            "startingPage": item.get("firstPage", ""),
                            "endingPage": item.get("lastPage", ""),
                        }
                    )
                    if related_item_identifier_type == "issn":
                        related_info["journalISSN"] = related_item_identifier

        return related_info

    def _extract_ifs3_digest_record_info(self, x):
        """
        Extract additional information for the "ifs3-digest" format.

        Args:
            x (dict): A Crossref record.

        Returns:
            dict: Processed information in ifs3-digest format.
        """
        digest_info = self._extract_digest_record_info(x)
        digest_info["ifs3_collection"] = self._extract_ifs3_collection(x)
        digest_info["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        dc_type_info = self.get_dc_type_info(x)
        digest_info["dc.type"] = dc_type_info["dc.type"]
        digest_info["dc.type_authority"] = dc_type_info["dc.type_authority"]
        return digest_info

    def _extract_ifs3_record_info(self, x):
        """
        Returns a complete record in ifs3 format.

        Starting from the enriched ifs3-digest metadata, this function adds:
        - the abstract,
        - authors information,
        - conference information,
        - funding information.

        Args:
            x (dict): The Crossref record (typically the "message" node).

        Returns:
            dict: A dictionary containing the complete ifs3 metadata.
        """
        rec = self._extract_ifs3_digest_record_info(x)
        rec["abstract"] = self._extract_abstract(x)
        rec["authors"] = self._extract_authors_info(x)
        rec["conference_info"] = ""
        rec["fundings_info"] = self._extract_funding(x)
        rec["related_works"] = self._extract_related_identifiers(x)
        rec["HasVersion"] = self._extract_version_info(x, "HasVersion")
        rec["IsVersionOf"] = self._extract_version_info(x, "IsVersionOf")

        return rec

    def _extract_pubdate(self, attrs: Dict) -> str:
        """
        Extract the best available publication date in priority:
        1. dates[Issued]
        2. dates[Published]
        3. publicationYear
        """
        dates = attrs.get("dates", []) or []

        # Try 'Issued' first
        for d in dates:
            if d.get("dateType", "").lower() == "issued":
                return d.get("date", "").strip()

        # Try 'Published' if 'Issued' is missing
        for d in dates:
            if d.get("dateType", "").lower() == "published":
                return d.get("date", "").strip()

        # Fallback: just return the year if available
        year = attrs.get("publicationYear")
        return str(year) if year else ""

    def _extract_abstract(self, x: Dict) -> str:
        descs = x.get("attributes", {}).get("descriptions", []) or []
        for d in descs:
            if d.get("descriptionType", "").lower() == "abstract":
                text = d.get("description", "")
                return re.sub(r"\s+", " ", text).strip()
        return ""

    def _extract_funding(self, x: Dict) -> str:
        """
        Extracts the funding information from the record.
        """
        refs = x.get("attributes", {}).get("fundingReferences", []) or []
        parts = []
        for f in refs:
            funder = str(f.get("funderName", "")).strip()
            grant = str(f.get("awardTitle", "")).strip() 
            grantno = str(
                f.get("awardNumber", "")
            ) 
            parts.append(f"{funder}::{grant}::{grantno}")
        return "||".join(parts)

    def _extract_related_identifiers(self, x: Dict) -> str:
        """
        Extracts related identifiers of type DOI or URL, and converts DOIs to URLs.
        """
        related_identifiers = x.get("attributes", {}).get("relatedIdentifiers", []) or []
        parts = []

        # Regex to check if the DOI is already in URL form
        doi_url_pattern = re.compile(r"^https://(?:doi\.org|dx\.doi\.org)/")

        for identifier in related_identifiers:
            related_id_type = identifier.get("relatedIdentifierType", "").upper()
            related_id = identifier.get("relatedIdentifier", "")
            relation_type = identifier.get("relationType", "")

            # Filter for DOI or URL related identifiers
            if related_id_type in ["DOI", "URL"]:
                # If it's a DOI, convert it to a URL if not already in URL form
                if related_id_type == "DOI":
                    # Check if the DOI is already in URL form using regex
                    if not doi_url_pattern.match(related_id):
                        related_id = f"https://doi.org/{related_id}"

                # Append the relation to the parts list
                parts.append(f"{relation_type}::{related_id}")

        # Join all the relations with '||'
        return "||".join(parts)

    def _extract_authors_info(self, x: Dict) -> List[Dict]:
        """
        Extracts author data from DataCite 'creators' attribute, returning a list of dicts.
        """
        creators = x.get("attributes", {}).get("creators", []) or []
        authors_info: List[Dict] = []

        for creator in creators:
            # Safely handle NoneType by using empty strings if the value is None
            given_name = (creator.get("givenName", "") or "").strip()
            family_name = (creator.get("familyName", "") or "").strip()

            # Construct author string
            if given_name and family_name:
                author_str = f"{family_name}, {given_name}".strip(", ")
            else:
                # Fallback: use the raw name field as-is
                author_str = (creator.get("name", "") or "").strip()

            orcid = self._extract_orcid(creator)
            organizations = self._join_affiliations(creator)

            authors_info.append(
                {
                    "author": author_str,
                    "internal_author_id": "",
                    "orcid_id": orcid,
                    "organizations": organizations,
                }
            )

        return authors_info

    def _extract_orcid(self, creator: Dict) -> str:
        """
        Extract the ORCID from the nameIdentifiers field in the creator object.
        """
        for nid in creator.get("nameIdentifiers", []):
            if isinstance(nid, dict):  # Ensure the element is a dictionary
                if nid.get("nameIdentifierScheme", "").upper() == "ORCID":
                    raw = nid.get("nameIdentifier", "")
                    return raw.replace("https://orcid.org/", "")
        return ""

    def _join_affiliations(self, creator: Dict) -> str:
        """
        Join the affiliations of a creator.
        """
        affiliations = creator.get("affiliation", []) or []

        # Ensure that each affiliation is a string (extract 'name' if it's a dict)
        affiliation_names = []
        for affiliation in affiliations:
            if isinstance(affiliation, dict):
                # Extract the 'name' key from the dictionary if it's a dictionary
                affiliation_name = affiliation.get("name", "")
                if affiliation_name:
                    affiliation_names.append(affiliation_name)
            elif isinstance(affiliation, str):
                # If the affiliation is already a string, just add it
                affiliation_names.append(affiliation)

        return "|".join(affiliation_names)

    def _extract_first_doctype(self, x: Dict) -> Dict:
        """
        Extract doctype from DataCite record using a lowercased resourceTypeGeneral key.
        """
        types = x.get("attributes", {}).get("types", {})
        doctype = types.get("resourceTypeGeneral", "").strip().lower()

        return doctype

    def get_dc_type_info(self, x):
        """
        Retrieve the dc.type and dc.type_authority attributes for a given document type.

        Args:
            x (dict): A Crossref record.

        Returns:
            dict: A dictionary with "dc.type" and "dc.type_authority".
        """
        data_doctype = self._extract_first_doctype(x)
        doctype_mapping = mappings.doctypes_mapping_dict.get("source_datacite", {})
        document_info = doctype_mapping.get(data_doctype, None)
        dc_type = (
            document_info.get("dc.type", "unknown") if document_info else "unknown"
        )
        dc_type_authority = mappings.types_authority_mapping.get(dc_type, "unknown")
        return {
            "dc.type": dc_type,
            "dc.type_authority": dc_type_authority,
        }

    def _extract_ifs3_collection(self, x):
        """
        Determine the ifs3 collection associated with the document type.

        Args:
            x (dict): A Crossref record.

        Returns:
            str: The ifs3 collection or "unknown" if not found.
        """
        data_doctype = self._extract_first_doctype(x)
        if data_doctype in accepted_doctypes:
            mapped_value = mappings.doctypes_mapping_dict["source_datacite"].get(
                data_doctype
            )
            if mapped_value is not None:
                return mapped_value.get("collection", "unknown")
            else:
                self.logger.warning(
                    f"Mapping not found for data_doctype: {data_doctype}"
                )
                return "unknown"
        return "unknown"

    def _extract_ifs3_collection_id(self, x):
        """
        Determine the ifs3 collection identifier.

        Args:
            x (dict): A Crossref record.

        Returns:
            str: The ifs3 collection ID or "unknown".
        """
        ifs3_collection = self._extract_ifs3_collection(x)
        if ifs3_collection != "unknown":
            collection_info = mappings.collections_mapping.get(ifs3_collection, None)
            if collection_info:
                return collection_info["id"]
        return "unknown"

    def _normalize_issn(self, issn_field) -> str:
        if not issn_field:
            return ""
        if isinstance(issn_field, list):
            out = []
            for issn in issn_field:
                if "-" in issn:
                    out.append(issn)
                elif len(issn) == 8:
                    out.append(issn[:4] + "-" + issn[4:])
                else:
                    out.append(issn)
            return "||".join(out)
        if isinstance(issn_field, str):
            return self._normalize_issn(issn_field.split(","))
        return ""

    def _extract_version_info(self, x: Dict, relation_type: str) -> str:
        """
        Extracts related identifiers based on the given relation type (HasVersion or IsVersionOf)
        from the 'relatedIdentifiers' list where the relatedIdentifierType is 'DOI'
        and the DOI has the same prefix as the 'doi' (DOI of the current item) stored in 'attributes'.

        Args:
            x (Dict): The data record containing the 'attributes' and 'relatedIdentifiers'.
            relation_type (str): The relation type to filter by, e.g., 'HasVersion' or 'IsVersionOf'.

        Returns:
            str: A string of related identifiers separated by '||'.
        """
        # Extract the DOI (internal_id) from 'attributes.doi'
        internal_doi = (
            x.get("attributes", {}).get("doi", "").lower()
        )  # Ensure lowercase for comparison
        # Extract the prefix of the internal DOI (before the first dot)
        internal_prefix = internal_doi.split("/")[0]

        # Extract relatedIdentifiers
        related_identifiers = x.get("attributes", {}).get("relatedIdentifiers", [])
        version_ids = []

        # Iterate over the relatedIdentifiers to find those with the correct conditions
        for identifier in related_identifiers:
            # Check if the relationType matches the given relation_type and the relatedIdentifierType is 'DOI'
            if (
                identifier.get("relationType") == relation_type
                and identifier.get("relatedIdentifierType") == "DOI"
            ):
                related_doi = identifier.get("relatedIdentifier", "").lower()

                # Extract the prefix of the related DOI (before the first dot)
                related_prefix = related_doi.split("/")[0]

                # Compare the DOI prefix with the internal DOI prefix
                if related_prefix == internal_prefix:
                    version_ids.append(related_doi)

        # Join the version IDs into a single string separated by '||'
        return "||".join(version_ids)


# Initialize the DataCiteClient
DataCiteClient = Client(response_handler=JsonResponseHandler)

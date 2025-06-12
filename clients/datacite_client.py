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
from dotenv import load_dotenv
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

load_dotenv(os.path.join(os.getcwd(), ".env"))
DATACITE_CONTACT_EMAIL = os.environ.get("CONTACT_API_EMAIL")

USER_AGENT = (
    "infoscience-imports/1.0 "
    "(https://infoscience.epfl.ch; "
    f"mailto:{DATACITE_CONTACT_EMAIL})"
)

@endpoint(base_url=DATACITE_API_BASE_URL)
class DataCiteEndpoint:
    """
    DataCite API endpoints.
    """
    dois = "dois"
    doi = "dois/{doi}"
    prefixes = "prefixes/{prefix}"


class Client(APIClient):
    """
    Python client for the DataCite API.
    """

    user_agent = USER_AGENT

    log_file_path = os.path.join(logs_dir, "logging.log")
    logger = manage_logger(log_file_path)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ici on met à jour le vrai attribut
        self._session.headers.update({"User-Agent": self.user_agent})

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

        self.logger.info("Querying page %d with page size %d", page_number, page_size)
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

        # Keywords
        subjects = attrs.get("subjects", []) or []
        keywords = "||".join([s.get("subject", "") for s in subjects])

        # Extract related items like book chapters, journal issues, etc.
        related_items_info = self._extract_related_items(attrs)
        license_id = self._extract_license(x)

        container_info = self._extract_container_info(x)

        return {
            "source": "datacite",
            "internal_id": doi,
            "registered": self._extract_registered(x),
            "issueDate": issue_date,
            "doi": doi.lower(),
            "title": title,
            "doctype": doctype,
            "pubyear": pubyear,
            "publisher": self._extract_publisher(x),
            "editors": editors,
            "pmid": self._extract_alternate_identifier(x, "pmid"),
            "arxiv": self._extract_alternate_identifier(x, "arxiv"),
            "artno": "",
            "contributors": contributors,
            "corporateAuthor": self._extract_corporate_authors(x),
            "keywords": keywords,
            "version": version,
            "license": license_id,
            "client": self._extract_client(x),
            **related_items_info,  # Add the related items info here
            **container_info,
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
        rec["abstract"] = self._extract_description(x, "Abstract")
        rec["notes"] = self._extract_description(x, "Other")
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

    def _extract_description(self, x: Dict, description_type: str) -> str:
        """
        Extract descriptions from the data record based on the description type.
        """
        descs = x.get("attributes", {}).get("descriptions", []) or []
        matches = []
        for d in descs:
            if (
                d.get("descriptionType", "").strip().lower()
                == description_type.strip().lower()
            ):
                text = d.get("description", "")
                cleaned = re.sub(r"\s+", " ", text).strip()
                if cleaned:
                    matches.append(cleaned)

        if not matches:
            return ""
        # join toutes les descriptions par '||'
        return "||".join(matches)

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
        Extracts personal author data from a DataCite record, skipping
        any creators with nameType == "Organizational".

        Args:
            x (Dict): The raw DataCite record.

        Returns:
            List[Dict]: A list of dicts, each containing:
                - author: "Family, Given" or raw name fallback
                - internal_author_id: (empty)
                - orcid_id: ORCID if present
                - organizations: affiliations joined by "|"
        """
        creators = x.get("attributes", {}).get("creators") or []
        authors_info: List[Dict] = []

        for creator in creators:
            # Skip None or non-dict entries
            if not isinstance(creator, dict):
                continue

            # Safely get and normalize nameType
            name_type = (creator.get("nameType") or "").strip().lower()
            if name_type == "organizational":
                continue

            # Safely extract given and family names
            given_name = (creator.get("givenName") or "").strip()
            family_name = (creator.get("familyName") or "").strip()

            # Build display name
            if given_name and family_name:
                author_str = f"{family_name}, {given_name}"
            else:
                author_str = (creator.get("name") or "").strip()

            # Extract ORCID and affiliations safely
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

    def _extract_first_doctype(self, x: Dict) -> str:
        """
        Extracts the primary document type from a DataCite record.

        Uses the lowercased 'resourceTypeGeneral' value by default.
        Special case:
        - If resourceTypeGeneral == "Text" AND attributes.prefix == "10.48550",
            then return "preprint" instead of "text".

        Args:
            x (Dict): The raw DataCite record.

        Returns:
            str: The normalized document type.
        """
        attrs = x.get("attributes", {})
        types = attrs.get("types", {})
        doc_type = types.get("resourceTypeGeneral", "").strip().lower()

        # Special handling for preprints on this prefix
        if doc_type == "text":
            prefix = attrs.get("prefix", "").strip()
            if prefix == "10.48550":
                return "preprint"

        return doc_type

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
                    "Mapping not found for data_doctype: %s", data_doctype
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
        attrs = x.get("attributes", {})
        # Extract the prefix of the internal DOI (before the first dot)
        internal_prefix = attrs.get("prefix", "").lower()

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
                doi = identifier.get("relatedIdentifier", "").strip()
                doi_lower = doi.lower()

                # Compare the DOI prefix with the internal DOI prefix
                if doi_lower.startswith(f"{internal_prefix}/"):
                    version_ids.append(doi_lower)

        # Join the version IDs into a single string separated by '||'
        return "||".join(version_ids)

    def _extract_license(self, x: Dict) -> str:
        """
        Extrait la licence à partir de l'attribut `rightsList`.
        Retourne le premier `rightsIdentifier` trouvé, ou une chaîne vide sinon.
        """
        rights = x.get("attributes", {}).get("rightsList", []) or []
        for r in rights:
            lic = r.get("rightsIdentifier")
            if lic:
                return lic
        return ""

    def _extract_alternate_identifier(self, x: Dict, alt_type: str) -> str:
        """
        Extracts all alternateIdentifiers of a given type from a DataCite record.

        Args:
            x (Dict): The raw DataCite record.
            alt_type (str): The alternateIdentifierType to filter on (e.g., "arXiv").

        Returns:
            str: All matching alternateIdentifier values joined by "||".
        """
        alt_type_clean = alt_type.strip().lower()

        alternates = x.get("attributes", {}).get("identifiers", []) or []
        # Filter for entries where the type matches (case-sensitive by default)
        values = [
            alt.get("identifier", "").strip()
            for alt in alternates
            if alt.get("identifierType", "").strip().lower() == alt_type_clean
            and alt.get("identifier")
        ]
        return "||".join(values)

    def _extract_corporate_authors(self, x: Dict) -> str:
        """
        Extracts organizational creator names from a DataCite record.

        Args:
            x (Dict): The raw DataCite record.

        Returns:
            str: All organizational names joined by "||".
        """
        creators = x.get("attributes", {}).get("creators") or []
        org_names: List[str] = []

        for creator in creators:
            # Skip non-dicts or None
            if not isinstance(creator, dict):
                continue

            # Safely get nameType and name, default to empty string
            name_type = (creator.get("nameType") or "").strip().lower()
            name = (creator.get("name") or "").strip()

            # Collect only organizational names
            if name_type == "organizational" and name:
                org_names.append(name)

        return "||".join(org_names)

    def _extract_client(self, x: Dict) -> str:
        """
        Get DOI Client ID.
        """
        return (
            x.get("relationships", {}).get("client", {}).get("data", {}).get("id", "")
        )

    def _extract_container_info(self, x: Dict) -> Dict[str, str]:
        """
        Extracts the container metadata from a DataCite record.

        Args:
            x (Dict): The raw DataCite record.

        Returns:
            dict: {
                "container_type": str,
                "container_identifier": str,
                "container_identifier_type": str
            }
            All values sont des chaînes, vides si non présentes.
        """
        container = x.get("attributes", {}).get("container", {}) or {}
        return {
            "container_type": container.get("type", "") or "",
            "container_identifier": container.get("identifier", "") or "",
            "container_identifier_type": container.get("identifierType", "") or "",
        }

    def _extract_registered(self, x: Dict) -> str:
        """
        Extract DOI registration date from the DataCite record.
        """
        return x.get("attributes", {}).get("registered", "") or ""


# Initialize the DataCiteClient
DataCiteClient = Client(response_handler=JsonResponseHandler)

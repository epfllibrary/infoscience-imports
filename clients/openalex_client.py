"""OpenAlex client for Infoscience imports"""

import os
import re
from pathlib import Path
from typing import List, Optional
import requests
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
from utils import get_pipeline_logger
import mappings

# Base URL for OpenAlex API
openalex_api_base_url = "https://api.openalex.org"

_DOI_RE = re.compile(r"^https?://(?:dx\.)?doi\.org/", re.IGNORECASE)
_OA_PREFIX = "https://openalex.org/"

def normalize_doi(doi: str) -> str:
    if not isinstance(doi, str):
        return ""
    doi = doi.strip()
    return _DOI_RE.sub("", doi)

def normalize_openalex_id(full_id: str) -> str:
    if not isinstance(full_id, str):
        return ""
    full_id = full_id.strip()
    return full_id[len(_OA_PREFIX):] if full_id.startswith(_OA_PREFIX) else full_id

_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
PDF_FOLDER = _project_root / "data" / "pdfs"
PDF_FOLDER.mkdir(parents=True, exist_ok=True)

# Load environment variables
load_dotenv(os.path.join(os.getcwd(), ".env"))
openalex_email = os.environ.get("CONTACT_API_EMAIL")
openalex_data_version = os.environ.get("OPENALEX_DATA_VERSION", "2")
openalex_token = os.environ.get("OPENALEX_API_KEY")
user_agent = os.environ.get("USER_AGENT", "EPFL-Institutional-Repository - Infoscience-imports/1.0 (https://github.com/epfllibrary/infoscience-imports)")


accepted_doctypes = list({
    key
    for source in ("source_crossref", "source_openalex")
    for key, val in mappings.doctypes_mapping_dict[source].items()
    if not val.get("rejected", False)
})

# Retry decorator to handle request retries on specific status codes
retry_decorator = tenacity.retry(
    retry=(retry_if_api_request_error(status_codes=[429, 502, 503, 504])),
    wait=tenacity.wait_exponential(multiplier=1, min=2, max=30)
    + tenacity.wait_random(0, 1),
    stop=tenacity.stop_after_attempt(6),
    reraise=True,
)

@endpoint(base_url=openalex_api_base_url)
class OpenAlexEndpoint:
    works = "works"
    work_id = "works/{openalexId}"
    doi = "works/doi:{doi}"
    author = "authors/{authorId}"
class Client(APIClient):
    logger = get_pipeline_logger('openalex')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Paramètres appliqués à TOUTES les requêtes
        self.default_params = {}
        if openalex_email:
            self.default_params["mailto"] = openalex_email
        # Active la nouvelle version de données OpenAlex
        # (surchageable avec OPENALEX_DATA_VERSION)
        if openalex_data_version:
            self.default_params["data-version"] = str(openalex_data_version)
        # OpenAlex API token (query param)
        if openalex_token:
            self.default_params["api_key"] = openalex_token
        # User-Agent header
        if user_agent:
            self.default_headers = {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }

        self.last_response = None

    def _merge_params(self, extra: dict | None = None) -> dict:
        """Merge des paramètres en respectant les défauts globaux."""
        base = dict(self.default_params)
        if extra:
            base.update({k: v for k, v in extra.items() if v is not None})
        return base

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Basic search query in the OpenAlex API.

        Example:
        https://api.openalex.org/works?filter=title.search:cadmium&per_page=5&page=1
        """
        self.params = self._merge_params(param_kwargs)
        response = self.get(OpenAlexEndpoint.works, params=self.params)
        self.last_response = response  # stocke la dernière réponse
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
        param_kwargs.setdefault("per_page", 1)
        param_kwargs.setdefault("page", 1)
        self.params = self._merge_params(param_kwargs)
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
        param_kwargs.setdefault("per_page", 100)
        cursor = param_kwargs.pop("cursor", "*")

        all_ids: List[str] = []

        while True:
            # On inclut le curseur à chaque requête
            self.params = self._merge_params({**param_kwargs, "cursor": cursor})
            response = self.search_query(**self.params)

            results = response.get("results", [])
            if not results:
                break

            for record in results:
                doi = self.openalex_extract_doi(record)
                if doi:
                    all_ids.append(doi)
                else:
                    all_ids.append(normalize_openalex_id(record.get("id", "")))

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
        param_kwargs.setdefault("per_page", 100)
        cursor = param_kwargs.pop("cursor", "*")

        all_records = []
        page_count = 0
        total_count = None

        while True:
            self.params = self._merge_params({**param_kwargs, "cursor": cursor})
            response = self.search_query(**self.params)

            results = response.get("results", [])
            if not results:
                break

            for record in results:
                parsed = self._process_record(record, format)
                if parsed:
                    all_records.append(parsed)

            page_count += 1
            total_count = response.get("meta", {}).get("count", total_count)
            self.logger.info(f"Page {page_count} harvested{' out of ' + str((total_count // param_kwargs['per_page']) + 1) if total_count else ''}.")

            cursor = response.get("meta", {}).get("next_cursor")
            if not cursor:
                break

        return all_records

    @retry_decorator
    def fetch_record_by_unique_id(self, openalex_id, format="digest"):
        """
        Retrieves a specific record by its unique OpenAlex ID or DOI.

        Supports both:
        - OpenAlex ID: "W2762925973"
        - DOI: "10.1103/physrevd.111.l091101"

        Args:
            openalex_id (str): The OpenAlex ID or DOI.
            format (str): Output format for processing ("digest", "digest-ifs3", "ifs3", or "openalex").

        Returns:
            dict or None: Processed metadata record, or None if not found.
        """
        if not openalex_id or str(openalex_id).strip().lower() == "null":
            return None

        self.params = self._merge_params()

        # Determine endpoint based on whether it's a DOI or an OpenAlex ID
        if isinstance(openalex_id, str) and openalex_id.lower().startswith("10."):
            # Handle DOI case
            endpoint_url = OpenAlexEndpoint.doi.format(doi=openalex_id)
        else:
            # Fallback to OpenAlex ID
            endpoint_url = OpenAlexEndpoint.work_id.format(openalexId=openalex_id)

        try:
            result = self.get(endpoint_url, params=self.params)
            return self._process_record(result, format) if result else None
        except Exception as e:
            self.logger.error(f"Error fetching record for ID/DOI '{openalex_id}': {e}")
            return None

    def _process_fetch_records(self, format, **param_kwargs):
        """
        Process fetched records into the desired output format.

        Args:
            format (str): Output format ('digest', 'digest-ifs3', 'ifs3', or 'openalex').
            **param_kwargs: Parameters for querying OpenAlex API.

        Returns:
            list: Processed records in the requested format.
        """
        self.params = self._merge_params(self.params if isinstance(self.params, dict) else {})

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
        digest = {
            "source": "openalex",
            "internal_id": x["id"],
            "issueDate": self._extract_publication_date(x),
            "doi": self.openalex_extract_doi(x),
            "title": x.get("display_name", ""),
            "doctype": self._extract_first_doctype(x),
            "pubyear": x.get("publication_year"),
            "volume": self._extract_volume(x),
            "issue": self._extract_issue(x),
            "startingPage": self._extract_starting_page(x),
            "endingPage": self._extract_ending_page(x),
            "artno": x.get("biblio", {}).get("article_number", ""),
            "keywords": self._extract_keywords(x),
            "is_paratext": self._extract_is_paratext(x),
            "is_retracted": self._extract_is_retracted(x),
            "openalex_type": self._extract_openalex_doctype(x),
            "openalex_id": self._extract_openalex_id(x),
            "referenced_works_count": self._extract_referenced_works_count(x),
            "cited_by_count": self._extract_cited_by_count(x),
            "pmid": self._extract_pmid(x),
        }

        for label, func in [
            ("primary_location", self._extract_primary_location_info),
            ("best_oa_location", self._extract_best_oa_location_info),
            ("open_access", self._extract_open_access_info),
            ("affiliation_info", self._extract_affiliation_info),
            (
                "corresponding_institution_ids",
                self._extract_corresponding_institution_ids,
            ),
            ("content_info", self._extract_content_info),
        ]:
            try:
                result = func(x)
                # self.logger.info(f"[{label}] extracted fields: {result}")
                digest.update(result)
            except Exception as e:
                self.logger.warning(f"[{label}] extraction failed: {e}")

        try:
            apc_info = self._extract_apc_info(x)
            if apc_info:
                digest.update(apc_info)
        except Exception as e:
            self.logger.warning(f"[apc] extraction failed: {e}")

        return digest

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
        dc_type_info = self.get_dc_type_info(x)
        digest_info["dc.type"] = dc_type_info["dc.type"]
        digest_info["dc.type_authority"] = dc_type_info["dc.type_authority"]

        # Canonical field names expected by the loader (mirrors WoS/Crossref/Scopus convention).
        # primary_container_title / primary_issn / primary_host_org are already in digest_info
        # via _extract_primary_location_info; we expose them under the loader-facing names too.
        digest_info["journalTitle"] = digest_info.get("primary_container_title", "")
        digest_info["journalISSN"] = digest_info.get("primary_issn", "")
        digest_info["journalVolume"] = digest_info.get("volume", "")
        digest_info["publisher"] = digest_info.get("primary_host_org", "")
        digest_info["language"] = self._extract_language(x)
        digest_info["abstract"] = self.extract_abstract(x)
        digest_info["fundings_info"] = self._extract_funding_info(x)

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
        # abstract is already included by _extract_ifs3_digest_record_info
        ifs3_info["authors"] = self.extract_ifs3_authors(x)
        return ifs3_info

    def _extract_openalex_id(self, x):
        return normalize_openalex_id(x.get("id", ""))

    def openalex_extract_doi(self, x):
        """
        Extract DOI from an OpenAlex record, removing the prefix 'https://doi.org/'.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: DOI without the 'https://doi.org/' prefix, or an empty string if DOI is None.
        """
        return normalize_doi(x.get("doi", ""))

    def _extract_first_doctype(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("type_crossref") or x.get("type") or "other"

    def _get_doctype_mapping_source(self, x):
        """Return the mapping source key to use for doctype lookups."""
        return "source_crossref" if x.get("type_crossref") else "source_openalex"

    def _extract_openalex_doctype(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("type")

    def _extract_is_paratext(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("is_paratext")

    def _extract_is_retracted(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("is_retracted")

    def _extract_referenced_works_count(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("referenced_works_count")

    def _extract_cited_by_count(self, x):
        """
        Extract the document type from a single OpenAlex record.

        Args:
            x (dict): A single OpenAlex record.

        Returns:
            str: Document type extracted from the record.
        """
        return x.get("cited_by_count")

    def _extract_primary_location_info(self, x: dict) -> dict:
        primary = x.get("primary_location", {}) or {}
        source = primary.get("source", {}) or {}

        return {
            "primary_is_oa": str(primary.get("is_oa", "")),
            "primary_version": primary.get("version", ""),
            "primary_license": primary.get("license", ""),
            "primary_host_org": source.get("host_organization_name", ""),
            "primary_issn": (
                "||".join(source.get("issn", []))
                if isinstance(source.get("issn", []), list)
                else source.get("issn", "")
            ),
            "primary_issn_l": source.get("issn_l", ""),
            "primary_is_core": str(source.get("is_core", "")),
            "primary_source_type": source.get("type", ""),
            "primary_container_title": source.get("display_name", ""),
            "primary_landing_page_url": primary.get("landing_page_url", ""),
            "primary_location_id": primary.get("id", ""),
        }

    def _extract_best_oa_location_info(self, x: dict) -> dict:
        best_oa = x.get("best_oa_location", {}) or {}
        source = best_oa.get("source", {}) or {}

        return {
            "best_oa_is_oa": str(best_oa.get("is_oa", "")),
            "best_oa_pdf_url": best_oa.get("pdf_url", ""),
            "best_oa_landing_url": best_oa.get("landing_page_url", ""),
            "best_oa_license": best_oa.get("license", ""),
            "best_oa_version": best_oa.get("version", ""),
            "best_oa_is_in_doaj": str(source.get("is_in_doaj", "")),
            "best_oa_container_title": source.get("display_name", ""),
            "best_oa_issn": (
                "||".join(source.get("issn", []))
                if isinstance(source.get("issn", []), list)
                else source.get("issn", "")
            ),
            "best_oa_issn_l": source.get("issn_l", ""),
            "best_oa_host_org": source.get("host_organization_name", ""),
            "best_oa_is_core": str(source.get("is_core", "")),
            "best_oa_source_type": source.get("type", ""),
        }

    def _extract_open_access_info(self, x: dict) -> dict:
        oa = x.get("open_access", {}) or {}

        return {
            "oa_status": oa.get("oa_status", ""),
            "oa_any_repository_has_fulltext": str(
                oa.get("any_repository_has_fulltext", "")
            ),
            "oa_is_oa": str(oa.get("is_oa", "")),
            "oa_url": oa.get("oa_url", ""),
        }

    def _extract_affiliation_info(self, x: dict) -> dict:
        """
        Flattens and extracts affiliation information from OpenAlex 'authorships'.

        Returns:
            dict with concatenated values (||-joined):
                - affiliation_ids
                - affiliation_names
                - affiliation_rors
                - affiliation_country_codes
                - affiliation_raw_strings
        """
        ids = set()
        names = set()
        rors = set()
        countries = set()
        raw_strings = set()

        try:
            for authorship in x.get("authorships", []):
                # Normalized institutions
                for inst in authorship.get("institutions", []):
                    if not isinstance(inst, dict):
                        continue
                    inst_id = inst.get("id")
                    ror_id = inst.get("ror")
                    if inst_id:
                        ids.add(inst_id.split("/")[-1])
                    if ror_id:
                        rors.add(ror_id.split("/")[-1])
                    if inst.get("display_name"):
                        names.add(inst["display_name"].strip())
                    if inst.get("country_code"):
                        countries.add(inst["country_code"].strip())

                # Raw affiliation strings (from 'affiliations')
                for aff in authorship.get("affiliations", []):
                    raw = aff.get("raw_affiliation_string", "")
                    if raw:
                        raw_strings.add(raw.strip())

        except Exception as e:
            self.logger.warning(f"Error in _extract_affiliation_info: {e}")

        return {
            "affiliation_ids": "||".join(sorted(ids)),
            "affiliation_names": "||".join(sorted(names)),
            "affiliation_rors": "||".join(sorted(rors)),
            "affiliation_country_codes": "||".join(sorted(countries)),
            "affiliation_raw_strings": "||".join(sorted(raw_strings)),
        }

    def get_dc_type_info(self, x):
        """
        Retrieves the dc.type and dc.type_authority attributes for a given document type.

        :param data_doctype: The document type (e.g., "Article", "Proceedings Paper", etc.)
        :return: A dictionary with the keys "dc.type" and "dc.type_authority", or "unknown" if not found.
        """
        data_doctype = self._extract_first_doctype(x)
        doctype_mapping = mappings.doctypes_mapping_dict.get(self._get_doctype_mapping_source(x), {})
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
            mapped_value = mappings.doctypes_mapping_dict[self._get_doctype_mapping_source(x)].get(
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
                        f"{(inst.get('ror') or '').split('/')[-1]}:{inst.get('display_name') or ''}"
                        for inst in author.get("institutions", [])
                    ]
                )
                raw_name = author["author"]["display_name"]
                formatted_name = self._format_authorname(raw_name)

                authors.append(
                    {
                        "author": formatted_name,
                        "internal_author_id": (author.get("author") or {}).get("id"),
                        "orcid_id": self._extract_author_orcid(author["author"]),
                        "organizations": institutions,
                        "is_corresponding": author.get("is_corresponding", False),
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
            if not isinstance(index, dict):
                return ""
            position_map = {}
            for word, positions in index.items():
                if not isinstance(positions, list):
                    continue
                for pos in positions:
                    if isinstance(pos, int):
                        position_map[pos] = word
            abstract = " ".join(position_map[i] for i in sorted(position_map))
            return abstract.strip()
        except Exception as e:
            return ""

    def _extract_publication_date(self, x):
        try:
            date_parts = x.get("publication_date", "")
            return date_parts if isinstance(date_parts, str) else ""
        except Exception as e:
            return ""

    def _extract_volume(self, x):
        try:
            return x.get("biblio", {}).get("volume", "") or ""
        except Exception as e:
            return ""

    def _extract_issue(self, x):
        try:
            return x.get("biblio", {}).get("issue", "") or ""
        except Exception as e:
            return ""

    def _extract_starting_page(self, x):
        try:
            return x.get("biblio", {}).get("first_page", "") or ""
        except Exception as e:
            return ""

    def _extract_ending_page(self, x):
        try:
            return x.get("biblio", {}).get("last_page", "") or ""
        except Exception as e:
            return ""

    def _extract_keywords(self, x):
        try:
            kw_list = x.get("keywords", [])
            if isinstance(kw_list, list) and kw_list:
                return "||".join(
                    k.get("display_name", "").strip()
                    for k in kw_list
                    if isinstance(k, dict) and k.get("display_name")
                )
            # fallback: concepts field (deprecated by OpenAlex, kept for older records)
            concepts = x.get("concepts", [])
            if isinstance(concepts, list):
                return "||".join(
                    c.get("display_name", "") for c in concepts if isinstance(c, dict)
                )
            return ""
        except Exception:
            return ""

    def _extract_apc_info(self, x: dict) -> dict:
        """
        Extrait apc_list et apc_paid sous forme de champs textuels plats.
        Exemple : apc_list_value, apc_list_currency, apc_list_value_usd, etc.
        """
        out = {}

        def flatten(field: str):
            obj = x.get(field) or {}
            if not isinstance(obj, dict):
                return
            for k in ("value", "currency", "value_usd"):
                if k in obj and obj[k] is not None:
                    out[f"{field}_{k}"] = str(obj[k])

        flatten("apc_list")
        flatten("apc_paid")

        return out

    def _extract_pmid(self, x) -> str:
        pmid = (x.get("ids") or {}).get("pmid", "") or ""
        if isinstance(pmid, str):
            pmid = pmid.replace("https://pubmed.ncbi.nlm.nih.gov/", "").rstrip("/")
        return str(pmid).strip() if pmid else ""

    def _extract_language(self, x) -> str:
        lang = x.get("language", "") or ""
        return str(lang).strip()

    def _extract_funding_info(self, x) -> str:
        """Build 'FunderName::AwardId[||...]' from OpenAlex grants array.

        Mirrors the format produced by Crossref and WoS clients so the loader
        can patch oairecerif.funder / dc.relation.grantno identically.
        """
        try:
            grants = x.get("grants") or []
            if not grants:
                return ""
            parts = []
            for g in grants:
                name = (g.get("funder_display_name") or "").strip()
                award = (g.get("award_id") or "").strip()
                if name:
                    parts.append(f"{name}::{award}")
            return "||".join(parts)
        except Exception as e:
            self.logger.error("Error extracting funding info: %s", e)
            return ""

    def _extract_content_info(self, x: dict) -> dict:
        has_content = x.get("has_content") or {}
        content_urls = x.get("content_urls") or {}
        return {
            "has_content_pdf": bool(has_content.get("pdf", False)),
            "content_url_pdf": content_urls.get("pdf", ""),
        }

    def download_content_pdf(self, content_url: str, doi: str) -> Optional[str]:
        """Download a PDF from the OpenAlex content endpoint and save to data/pdfs/.

        Validates the PDF magic bytes before writing to disk.
        Returns the local filename on success, None otherwise.
        """
        if not content_url or not openalex_token:
            return None

        url = f"{content_url}?api_key={openalex_token}"
        filename = f"{doi.replace('/', '_')}.pdf"
        file_path = PDF_FOLDER / filename

        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            if "application/pdf" not in content_type:
                self.logger.warning(
                    "OpenAlex content returned non-PDF for DOI %s (Content-Type: %s)",
                    doi, content_type,
                )
                return None

            raw = b"".join(response.iter_content(8192))
            if not raw.startswith(b"%PDF"):
                self.logger.warning("OpenAlex content: invalid PDF magic bytes for DOI %s", doi)
                return None

            file_path.write_bytes(raw)
            self.logger.info("OpenAlex content PDF saved: %s", filename)
            return filename

        except Exception as e:
            self.logger.error("OpenAlex content PDF download failed for DOI %s: %s", doi, e)
            return None

    def _extract_corresponding_institution_ids(self, x: dict) -> dict:
        """
        Extrait la liste des corresponding_institution_ids au format texte,
        concaténée par '||' et normalisée (IDs OpenAlex sans préfixe URL).
        """
        try:
            raw_list = x.get("corresponding_institution_ids") or []
            if not isinstance(raw_list, list):
                return {"corresponding_institution_ids": ""}
            ids = [normalize_openalex_id(s) for s in raw_list if isinstance(s, str)]
            ids = [i for i in ids if i]  # supprime les vides
            return {"corresponding_institution_ids": "||".join(sorted(set(ids)))}
        except Exception as e:
            self.logger.warning(f"Error in _extract_corresponding_institution_ids: {e}")
            return {"corresponding_institution_ids": ""}

    def _oa_get(self, path: str, params: dict | None = None) -> dict:
        """Direct requests.get call to the OpenAlex API, bypassing apiclient.

        Used for endpoints (authors) where the apiclient JsonResponseHandler
        raises UnexpectedError despite a valid 200 response.
        Returns the parsed JSON dict, or raises on HTTP/network error.
        """
        url = f"{openalex_api_base_url}/{path}"
        merged = self._merge_params(params or {})
        resp = requests.get(url, params=merged, timeout=15,
                            headers=getattr(self, "default_headers", {}))
        resp.raise_for_status()
        return resp.json()

    def fetch_author_id_by_orcid(self, orcid: str) -> str | None:
        """Look up the OpenAlex author ID for a given ORCID.

        Calls /authors?filter=orcid:{orcid}. Returns the short OpenAlex author ID
        (e.g. "A5014490073") or None when not found or on error.
        """
        try:
            data = self._oa_get("authors", {"filter": f"orcid:{orcid}", "select": "id"})
            results = data.get("results") or []
            if results:
                return normalize_openalex_id(results[0].get("id", "")) or None
        except Exception as exc:
            self.logger.warning("fetch_author_id_by_orcid failed for %s: %s", orcid, exc)
        return None

    def fetch_author_by_id(self, openalex_id: str) -> dict:
        """Fetch author profile from OpenAlex and extract useful identifiers.

        Returns a dict with any of: orcid, scopus_author_id, openalex_name_variants.
        Returns {} on error or when the author is not found.
        """
        import json as _json

        short_id = normalize_openalex_id(openalex_id)
        if not short_id:
            return {}
        try:
            data = self._oa_get(f"authors/{short_id}")
        except Exception as exc:
            self.logger.warning("fetch_author_by_id failed for %s: %s", short_id, exc)
            return {}

        if not data or not isinstance(data, dict):
            return {}

        result: dict = {}
        ids = data.get("ids") or {}

        raw_orcid = ids.get("orcid") or data.get("orcid") or ""
        if raw_orcid:
            orcid_match = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", str(raw_orcid))
            if orcid_match:
                result["orcid"] = orcid_match.group(1)

        raw_scopus = ids.get("scopus") or ""
        if raw_scopus:
            scopus_match = re.search(r"authorI[dD]=(\d+)", str(raw_scopus))
            if scopus_match:
                result["scopus_author_id"] = scopus_match.group(1)

        alternatives = data.get("display_name_alternatives") or []
        if alternatives:
            result["openalex_name_variants"] = _json.dumps(alternatives, ensure_ascii=False)

        return result

    def fetch_author_ids_from_dois(
        self,
        dois: list[str],
        display_name: str,
        max_edit_distance: int = 1,
        batch_size: int = 40,
    ) -> list[str]:
        """Infer OpenAlex author IDs from a batch of DOIs using name matching.

        Queries /works by DOI batch, collects all authorships, and returns IDs
        whose display_name is within max_edit_distance (Levenshtein) of the
        standardized display_name.  Returns at most 5 IDs ordered by frequency.
        """
        from rapidfuzz.distance import Levenshtein

        def _std_name(name: str | None) -> str | None:
            import unicodedata
            import re
            if not name or not isinstance(name, str):
                return None
            s = name.strip()
            if "," in s:
                last, first = [p.strip() for p in s.split(",", 1)]
                s = f"{first} {last}"
            nfkd = unicodedata.normalize("NFKD", s)
            clean = nfkd.encode("ASCII", "ignore").decode()
            parts = re.sub(r"[^\w\s-]", "", clean).split()
            if len(parts) < 2:
                return None
            first_part, last_part = parts[0], parts[-1]
            initials = "".join(p[0] for p in first_part.split("-") if p) or first_part[0]
            return f"{initials.lower()} {last_part.lower()}"

        name_std = _std_name(display_name)
        clean_dois = [d for d in (dois or []) if d]
        if not clean_dois:
            return []

        authors: list[dict] = []
        for i in range(0, len(clean_dois), batch_size):
            chunk = clean_dois[i:i + batch_size]
            try:
                params = self._merge_params({
                    "filter": f"doi:{'|'.join(chunk)}",
                    "select": "authorships",
                    "per_page": batch_size,
                })
                resp = self.get(OpenAlexEndpoint.works, params=params)
                for pub in (resp.get("results") or []):
                    for a in (pub.get("authorships") or []):
                        author_obj = a.get("author") or {}
                        if author_obj.get("id"):
                            authors.append(author_obj)
            except Exception as exc:
                self.logger.warning(
                    "OpenAlex works batch failed (offset %d): %s", i, exc
                )

        if not authors:
            return []

        import pandas as pd
        df = (
            pd.DataFrame(authors)
            .groupby("id")
            .agg(display_name=("display_name", "first"), count=("id", "size"))
            .sort_values("count", ascending=False)
            .head(50)
            .reset_index()
        )
        df["name_std"] = df["display_name"].apply(_std_name)

        if name_std is None:
            return df["id"].tolist()[:5]

        df = df.dropna(subset=["name_std"])
        if df.empty:
            return []

        df["dist"] = df["name_std"].apply(lambda s: Levenshtein.distance(s, name_std))
        close = df[df["dist"] <= max_edit_distance].sort_values(["dist", "count"], ascending=[True, False])
        return close["id"].tolist()

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

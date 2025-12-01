"""Zenodo client for Infoscience imports"""

import os
import re
import dateparser
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional
import tenacity
from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    HeaderAuthentication,
    JsonResponseHandler,
)
from apiclient.retrying import retry_if_api_request_error
from dotenv import load_dotenv
from config import logs_dir
from utils import manage_logger
import mappings


zenodo_api_base_url = "https://zenodo.org/api/"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
zenodo_api_key = os.environ.get("ZENODO_API_KEY")

accepted_doctypes = mappings.doctypes_mapping_dict["source_zenodo"].keys()

zenodo_authentication_method = HeaderAuthentication(token=zenodo_api_key, scheme=None)

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=zenodo_api_base_url)
class Endpoint:
    base = ""
    search = "records"
    uniqueId = "records/{zenodoId}"


class Client(APIClient):
    log_file_path = os.path.join(logs_dir, "logging.log")
    logger = manage_logger(log_file_path)

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Base request example
        https://zenodo.org/records?q=parent.communities.entries.id%3A"3c1383da-d7ab-4167-8f12-4d8aa0cc637f"
        =>
        https://zenodo.org/api/records?q=parent.communities.entries.id:"3c1383da-d7ab-4167-8f12-4d8aa0cc637f"

        Default args (can be orverwritten)
        none

        Usage
        ZenodoClient.search_query(q="gene",size=5,page=1)

        Returns
        A JSON array of Zenodo records
        """

        self.params = {**param_kwargs}
        self.logger.debug((Endpoint.search, self.params))
        return self.get(Endpoint.search, params=self.params)

    @retry_request
    def count_results(self, **param_kwargs) -> int:
        """
        Base request example
        https://zenodo.org/api?q=epfl&size=1&page=1

        Default args (can be orverwritten)
        count (number of returned records) is set to 1
        start (first record) is set to 1

        Usage
        ZenodoClient.count_results(q="polytechnique")
        ZenodoClient.count_results(q="ploytechnique", count=1, page=1)

        Returns
        Number of records found by the query
        """
        param_kwargs.setdefault("size", 25)
        param_kwargs.setdefault("page", 1)

        self.params = {**param_kwargs}
        return self.search_query(**self.params)["hits"]["total"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs) -> List[str]:
        """
        Base request example
        https://zenodo.org/api/?q=epfl&size=25&page=1

        Default args (can be orverwritten)
        size (number of returned records) is set to 10
        page is set to 1

        Usage 1
        ZenodoClient.fetch_ids(q="epfl")
        ZenodoClient.fetch_ids(q="epfl", size=50)

        Usage 2
        naive_epfl_query = "epfl OR \"Ecole Polytechnique Fédérale de Lausanne\""
        total = ZenodoClient.count_results(q=naive_epfl_query, size=1, page=1)
        count = 25
        ids = []
        for i in range(1, int(total), int(count)):
            ids.extend(ZenodoClient.fetch_ids(q=naive_epfl_query,
                                            size=count,
                                            page=i//count+1))

        Returns
        A list of Zenodo ids
        """

        param_kwargs.setdefault("size", 25)
        param_kwargs.setdefault("page", 1)

        self.params = {**param_kwargs}
        results = self.search_query(**self.params)["hits"]["hits"]
        return [x["id"] for x in results]

    @retry_decorator
    def fetch_records(self, format="digest", **param_kwargs):
        """
        Base request example
        https://zenodo.org/api?q=epfl&size=10&page=2

        Default args (can be orverwritten)
        size (number of returned records) is set to 10
        page is set to 1

        Args
        format: digest|digest-ifs3|ifs3|zenodo

        Usage 1
        ZenodoClient.fetch_records(q="epfl")
        ZenodoClient.fetch_records(format="digest-ifs3",q="epfl",size=50)

        Usage 2
        naive_epfl_query = "epfl OR \"Ecole Polytechnique Fédérale de Lausanne\""
        total = 20
        count = 5
        recs = []
        for i in range(1, int(total), int(count)):
            recs.extend(ZenodoClient.fetch_records(q=naive_epfl_query,
                                                size=count,
                                                page=i//count+1))

        Returns
        List of dicts with fields in this list depending on the chosen format:
            zenodo_id, title, DOI, doctype, pubyear, authors,
            ifs3_collection, ifs3_collection_id
        """
        param_kwargs.setdefault("size", 25)
        param_kwargs.setdefault("page", 1)

        self.params = {**param_kwargs}
        result = self.search_query(**self.params)
        if int(result["hits"]["total"]) > 0:
            return self._process_fetch_records(format, **self.params)
        return None

    @retry_decorator
    def fetch_record_by_unique_id(self, zenodo_id, format="digest"):
        """
        Base request example
        https://zenodo.org/api/records/9999

        Args
        format: digest|digest-ifs3|ifs3|zenodo

        Usage
        ZenodoClient.fetch_record_by_unique_id("9999")
        ZenodoClient.fetch_record_by_unique_id("9999", format="zenodo")
        ZenodoClient.fetch_record_by_unique_id("9999", format="ifs3")
        """

        result = self.get(Endpoint.uniqueId.format(zenodoId=zenodo_id))
        if "created" in result:
            return self._process_record(result, format)
        return None

    def _process_fetch_records(self, format, **param_kwargs):
        self.params = param_kwargs
        entries = self.search_query(**self.params)["hits"]["hits"]
        if format == "digest":
            return [self._extract_digest_record_info(x) for x in entries]
        elif format == "digest-ifs3":
            return [self._extract_ifs3_digest_record_info(x) for x in entries]
        elif format == "ifs3":
            return [self._extract_ifs3_record_info(x) for x in entries]
        elif format == "zenodo":
            return entries

    def _process_record(self, record, format):
        if format == "digest":
            return self._extract_digest_record_info(record)
        elif format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(record)
        elif format == "ifs3":
            return self._extract_ifs3_record_info(record)
        elif format == "zenodo":
            return record

    def _extract_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :
            zenodo_id, title, DOI, doctype, pubyear
        """
        record = {
            "source": "zenodo",
            "internal_id": (
                x["conceptdoi"].lower() if x.get("conceptdoi") else x["doi"].lower()
            ),
            "doi": x["doi"].lower(),
            "title": x["metadata"]["title"],
            "doctype": x["metadata"]["resource_type"]["title"],
            "pubyear": x["metadata"]["publication_date"][0:4],
            # Neutral information to keep reporting happy
            "journalTitle": None,
        }
        return record

    def _extract_ifs3_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :
            zenodo_id, title, DOI, doctype, pubyear,
            ifs3_collection, ifs3_collection_id
        """
        record = self._extract_digest_record_info(x)
        record["ifs3_collection"] = self._extract_ifs3_collection(x)
        record["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        # Get dc.type and dc.type_authority for the document type
        dc_type_info = self.get_dc_type_info(x)
        # Add dc.type and dc.type_authority to the record
        record["dc.type"] = dc_type_info["dc.type"]
        record["dc.type_authority"] = dc_type_info["dc.type_authority"]
        record["publisher"] = self._extract_publisher(x)
        record["related_works"] = self._extract_related_identifiers(x)
        record["additional_url"] = self._extract_additional_url(x)
        record["access_conditions"] = self._extract_access_right(x)
        record["language"], record["version"] = self._extract_language_and_version(x)
        record["conference_info"] = self._extract_conference_info(x)

        return record

    def _extract_ifs3_record_info(self, record):
        """
        Returns
        A list of records dict containing the fields :
            zenodo_id, title, DOI, doctype, pubyear, authors
            ifs3_collection, ifs3_collection_id, license, first_creation
        """
        rec = self._extract_ifs3_digest_record_info(record)
        authors = self._extract_ifs3_authors(record)
        rec["authors"] = authors
        rec["license"] = self._extract_ifs3_license(record)
        rec["first_creation"] = self._extract_first_version_creation(record)
        return rec

    def _extract_first_doctype(self, x):
        if "metadata" in x and "resource_type" in x["metadata"]:
            doctype = x["metadata"]["resource_type"].get("type", "unknown")
            if "subtype" in x["metadata"]["resource_type"]:
                doctype += f'/{x["metadata"]["resource_type"]["subtype"]}'
            return doctype
        else:
            return "unknown"

    def get_dc_type_info(self, x):
        """
        Retrieves the dc.type and dc.type_authority attributes for a given document type.

        :param data_doctype: The document type (e.g., "Article", "Proceedings Paper", etc.)
        :return: A dictionary with the keys "dc.type" and "dc.type_authority", or "unknown" if not found.
        """
        data_doctype = self._extract_first_doctype(x)
        # Access the doctype mapping for "source_wos"
        doctype_mapping = mappings.doctypes_mapping_dict.get("source_zenodo", {})
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
            mapped_value = mappings.doctypes_mapping_dict["source_zenodo"].get(
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

    def _extract_ifs3_license(self, x):
        """
        Extracts license information in IFS3 format.
        """
        try:
            license_info = x["metadata"].get("license", {})
            if isinstance(license_info, dict):
                license_id = license_info.get("id", None)
                if license_id:
                    return license_id
                else:
                    return "N/A"
            else:
                return "N/A"
        except KeyError:
            self.logger.warning("License information not found in the record metadata.")
            return {"value": "N/A", "display": "N/A (Copyrighted)"}

    def _extract_ifs3_authors(self, x):
        """
        Extract authors from a Zenodo record, preserving the original order.

        The function combines:
        1) `metadata.creators`  → role = "creator"
        2) `metadata.contributors` → role = "contributor"

        The order is preserved exactly as provided by the source.
        Each entry is returned as a dict with the following keys:
        - author (str): Person's name
        - internal_author_id (None): Placeholder for internal reconciliation
        - orcid_id (str|None): ORCID identifier if present
        - organizations (str|None): Affiliation text
        - role ("creator" | "contributor"): Role based on source array
        - contributor_type (str|None): Contributor type from Zenodo, only for contributors
        """
        result = []

        try:
            if not isinstance(x, dict):
                self.logger.error("Input data must be a dict.")
                return result

            md = x.get("metadata") or {}
            creators = md.get("creators") or []
            contributors = md.get("contributors") or []

            def _push(person: dict, role_label: str):
                """Append a structured person entry to the result list."""
                if not isinstance(person, dict):
                    return
                name = person.get("name")
                if not name:
                    return

                entry = {
                    "author": name,
                    "internal_author_id": None,
                    "orcid_id": person.get("orcid"),
                    "organizations": person.get("affiliation"),
                    "role": role_label,
                }

                # Keep contributor type if present
                if role_label == "contributor":
                    entry["contributor_type"] = person.get("type")

                result.append(entry)

            # Preserve ordering: creators first, contributors second
            for author in creators:
                _push(author, "author")

            for contrib in contributors:
                _push(contrib, "contributor")

        except Exception as e:
            self.logger.error(f"An error occurred during _extract_ifs3_authors: {e}")

        return result

    @retry_request
    def _extract_first_version_creation(self, x):
        """
        Extracts the creation date of the first version of the object.
        Optimized:
        - use server-side sort to get only the oldest version (1 request)
        - handle missing fields safely
        """
        try:
            versions_url = x.get("links", {}).get("versions")
            if not versions_url:
                return x.get("created")

            # ask the API for the oldest version directly (no need to sort client-side)
            resp = self.get(f"{versions_url}?sort=oldest&size=1&page=1") or {}
            hits = (resp.get("hits") or {}).get("hits") or []

            if hits:
                first = hits[0].get("created")
                if first:
                    return first

            # fallback: use current record creation
            return x.get("created")

        except Exception as e:
            self.logger.warning(f"_extract_first_version_creation failed: {e}")
            return x.get("created")

    def _extract_publisher(self, x: Dict[str, Any]) -> str:
        md = x.get("metadata", {}) or {}
        # Sur Zenodo, c’est souvent "Zenodo" ou vide
        return md.get("publisher") or "Zenodo"

    def _extract_related_identifiers(self, x: dict) -> str:
        """
        Return string 'relation::identifier' separated by '||'.

        - Supports 'related_identifiers' (Zenodo v3) and 'relatedIdentifiers' (variant).
        - Accepts 'relation' | 'relation_type' | 'relationType' for the relation.
        - Normalizes identifiers by scheme (case-insensitive):
            * DOI        -> https://doi.org/<doi>
            * ArXiv      -> https://arxiv.org/abs/<id>
            * Handle/HDL -> https://hdl.handle.net/<id>
            * PMID       -> https://pubmed.ncbi.nlm.nih.gov/<id>/
            * Already http(s)               -> kept as is.
        - Also appends 'IsVersionOf::<concept_doi_url>' if a concept DOI exists and
        the same relation (case-insensitive) with the same normalized identifier
        is not already present.
        """
        md = x.get("metadata", {}) or {}
        rels = md.get("related_identifiers") or md.get("relatedIdentifiers") or []
        out, seen_display = [], set()  # for exact string dedupe in output
        seen_norm = set()  # for logical dedupe: (relation_lower, url_lower)

        doi_url_re = re.compile(r"^https?://(?:doi\.org|dx\.doi\.org)/", re.I)
        http_re = re.compile(r"^https?://", re.I)

        def _norm_identifier(scheme: str, identifier: str) -> str:
            """Normalize an identifier to a resolvable URL when possible."""
            scheme_up = (scheme or "").strip().upper()
            ident = (identifier or "").strip()
            if not ident:
                return ""

            if http_re.match(ident):
                return ident
            if scheme_up == "DOI":
                return ident if doi_url_re.match(ident) else f"https://doi.org/{ident}"
            if scheme_up == "ARXIV":
                id_norm = re.sub(r"^arxiv:", "", ident, flags=re.I)
                return f"https://arxiv.org/abs/{id_norm}"
            if scheme_up in ("HANDLE", "HDL"):
                id_norm = re.sub(r"^(?:handle|hdl):", "", ident, flags=re.I)
                return f"https://hdl.handle.net/{id_norm}"
            if scheme_up == "PMID":
                id_norm = re.sub(r"^pmid:\s*", "", ident, flags=re.I)
                return f"https://pubmed.ncbi.nlm.nih.gov/{id_norm}/"
            # Fallback
            return ident

        # Collect existing related identifiers
        for r in rels:
            if not isinstance(r, dict):
                continue

            reltype = (
                r.get("relation") or r.get("relation_type") or r.get("relationType") or ""
            ).strip()
            if not reltype:
                continue

            scheme = (r.get("scheme") or r.get("identifierType") or "").strip()
            identifier = (r.get("identifier") or "").strip()
            if not identifier:
                continue

            norm = _norm_identifier(scheme, identifier)
            if not norm:
                continue

            entry = f"{reltype}::{norm}"
            if entry not in seen_display:
                seen_display.add(entry)
                out.append(entry)

            # also track normalized tuple for robust duplicate detection
            seen_norm.add((reltype.lower(), norm.lower()))

        # Consider adding IsVersionOf::<conceptdoi>
        concept_doi = (x.get("conceptdoi") or md.get("conceptdoi") or "").strip()
        if concept_doi:
            concept_norm = (
                concept_doi
                if doi_url_re.match(concept_doi)
                else f"https://doi.org/{concept_doi}"
            )
            key = ("isversionof", concept_norm.lower())
            if key not in seen_norm:
                concept_entry = f"IsVersionOf::{concept_norm}"
                # keep exact-string dedupe as well (defensive)
                if concept_entry not in seen_display:
                    seen_display.add(concept_entry)
                    out.append(concept_entry)

        return "||".join(out)

    def _extract_additional_url(self, x: dict) -> str:
        """
        Extract 'additional_url' if x['metadata.custom']['code:codeRepository'] is present.
        Returns 'repository url:<URL>' or empty string otherwise.
        """
        md = x.get("metadata", {}) or {}
        custom = md.get("custom") or {}
        url = custom.get("code:codeRepository")

        if isinstance(url, str) and url.strip():
            return f"Code Repository URL::{url.strip()}"
        return ""

    def _extract_access_right(self, x: dict) -> str:
        """
        Extract and map Zenodo access level to local values:
        - 'open'       → 'openaccess'
        - 'restricted' → 'restricted'
        """
        md = x.get("metadata", {}) or {}
        value = md.get("access_right", "")
        if not isinstance(value, str):
            return ""
        value = value.strip().lower()
        mapping = {
            "open": "openaccess",
            "restricted": "restricted",
        }
        return mapping.get(value, value)

    def _extract_language_and_version(self, x: dict) -> tuple[str | None, str | None]:
        """
        Extract language (mapped to ISO 639-1) and version from a Zenodo record.

        Example input:
            "language": "eng"
            "version": "1.0"

        Returns:
            (language_iso2, version)
            e.g. ('en', '1.0')
        """
        md = x.get("metadata", {}) or {}
        lang = md.get("language")
        version = md.get("version")

        # Map 3-letter → 2-letter ISO codes (extend as needed)
        lang_map = {
            "eng": "en",
            "fre": "fr",
            "fra": "fr",
            "ger": "de",
            "deu": "de",
            "ita": "it",
            "spa": "es",
            "por": "pt",
            "nld": "nl",
            "dut": "nl",
            "swe": "sv",
            "nor": "no",
            "dan": "da",
            "fin": "fi",
            "rus": "ru",
            "chi": "zh",
            "zho": "zh",
            "jpn": "ja",
        }

        lang_iso2 = None
        if isinstance(lang, str):
            lang_code = lang.strip().lower()
            lang_iso2 = lang_map.get(lang_code[:3], None) or lang_code[:2]

            # Handle accidental 3-letter codes that are not in map but look valid
            if len(lang_iso2) > 2:
                lang_iso2 = lang_iso2[:2]

        version_str = (
            str(version).strip() if isinstance(version, (str, int, float)) else None
        )
        return lang_iso2, version_str

    def _extract_conference_info(self, x: dict) -> str:
        """
        Extract conference information from a Zenodo record and format it as:
            'conference_title::conference_location::conference_startdate::conference_enddate::conference_acronym'

        Zenodo exposes meeting data under:
            record["metadata"]["meeting"] = {
                "title": "...",
                "acronym": "...",
                "dates": "11 to 15 January 2021",  # free text
                "place": "City, Country",          # optional
                "url": "...",
                "session": "..."
            }

        Notes:
        - Location is read from 'meeting.place' when present; otherwise an empty string is used.
        - Dates are parsed from common textual ranges and returned as ISO 'YYYY-MM-DD'.
        If parsing fails, empty strings are returned for dates.

        Args:
            x (dict): Zenodo record.

        Returns:
            str: 'title::location::start_date::end_date::acronym' or '' if nothing found.
        """
        md = x.get("metadata", {}) or {}
        meeting = md.get("meeting") or {}
        if not isinstance(meeting, dict) or not meeting:
            return ""

        title = (meeting.get("title") or "").strip()
        acronym = (meeting.get("acronym") or "").strip()
        dates_raw = (meeting.get("dates") or "").strip()
        # Take 'place' if provided (e.g., "Virtual Conference", "Lausanne, Switzerland")
        location = (meeting.get("place") or "").strip()

        start_date, end_date = self._parse_zenodo_meeting_dates(dates_raw)

        # If nothing meaningful, return empty string
        if not any([title, location, start_date, end_date, acronym]):
            return ""

        return f"{title}::{location}::{start_date}::{end_date}::{acronym}"

    # def _parse_zenodo_meeting_dates(self, s: str) -> tuple[str, str]:
    #     if not s:
    #         return "", ""

    #     parts = re.split(r"\s*(?:to|-|–|—)\s*", s)
    #     parsed = [dateparser.parse(p, languages=["en"]) for p in parts if p.strip()]
    #     dates = [d.strftime("%Y-%m-%d") for d in parsed if d]

    #     if len(dates) == 1:
    #         return dates[0], dates[0]
    #     elif len(dates) >= 2:
    #         return dates[0], dates[1]
    #     return "", ""

    def _parse_zenodo_meeting_dates(self, s: str) -> tuple[str, str]:
        """
        Parse common date string patterns from Zenodo 'meeting.dates' into (start, end) ISO dates.
        Supported examples:
        - "4-7 May, 2025"
        - "11 to 15 January 2021"
        - "Jan 11–15, 2021"
        - "30 Dec 2020 – 2 Jan 2021"
        - "22–27 August 2020"
        - "15 January 2021" (single day)
        Returns empty strings if parsing fails.
        """
        if not s:
            return "", ""

        s_norm = " ".join(s.split())
        month_map = {
            "january": "01",
            "jan": "01",
            "february": "02",
            "feb": "02",
            "march": "03",
            "mar": "03",
            "april": "04",
            "apr": "04",
            "may": "05",
            "june": "06",
            "jun": "06",
            "july": "07",
            "jul": "07",
            "august": "08",
            "aug": "08",
            "september": "09",
            "sep": "09",
            "sept": "09",
            "october": "10",
            "oct": "10",
            "november": "11",
            "nov": "11",
            "december": "12",
            "dec": "12",
        }

        def iso(y: str, m: str, d: str) -> str:
            try:
                yy, mm, dd = int(y), int(m), int(d)
                if 1 <= mm <= 12 and 1 <= dd <= 31:
                    return f"{yy:04d}-{mm:02d}-{dd:02d}"
            except Exception:
                pass
            return ""

        def m2num(m: str):
            return month_map.get(m.lower())

        # Pattern A: "30 Dec 2020 – 2 Jan 2021"
        m = re.search(
            r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})\s*(?:to|-|–|—)\s*(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})",
            s_norm,
        )
        if m:
            d1, M1, y1, d2, M2, y2 = m.groups()
            m1, m2 = m2num(M1), m2num(M2)
            return iso(y1, m1 or "", d1), iso(y2, m2 or "", d2)

        # Pattern B: "4-7 May, 2025" / "4–7 May 2025"
        m = re.search(
            r"(\d{1,2})\s*(?:to|-|–|—)\s*(\d{1,2})\s+([A-Za-z]{3,}),?\s*(\d{4})", s_norm
        )
        if m:
            d1, d2, M, y = m.groups()
            mm = m2num(M)
            return iso(y, mm or "", d1), iso(y, mm or "", d2)

        # Pattern C: "11 to 15 January 2021"
        m = re.search(
            r"(\d{1,2})\s*(?:to|-|–|—)\s*(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", s_norm
        )
        if m:
            d1, d2, M, y = m.groups()
            mm = m2num(M)
            return iso(y, mm or "", d1), iso(y, mm or "", d2)

        # Pattern D: "Jan 11–15, 2021"
        m = re.search(
            r"([A-Za-z]{3,})\s+(\d{1,2})\s*(?:to|-|–|—)\s*(\d{1,2}),?\s*(\d{4})", s_norm
        )
        if m:
            M, d1, d2, y = m.groups()
            mm = m2num(M)
            return iso(y, mm or "", d1), iso(y, mm or "", d2)

        # Pattern E: single date "15 January 2021" or "January 15, 2021"
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3,}),?\s*(\d{4})", s_norm)
        if not m:
            m = re.search(r"([A-Za-z]{3,})\s+(\d{1,2}),?\s*(\d{4})", s_norm)
            if m:
                M, d, y = m.groups()
                mm = m2num(M)
                return iso(y, mm or "", d), iso(y, mm or "", d)
        else:
            d, M, y = m.groups()
            mm = m2num(M)
            return iso(y, mm or "", d), iso(y, mm or "", d)

        return "", ""


ZenodoClient = Client(
    authentication_method=zenodo_authentication_method,
    response_handler=JsonResponseHandler,
)

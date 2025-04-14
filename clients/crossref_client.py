"""
Python client for the Crossref API
This client provides methods to interact with the Crossref API
"""

import os
import re
from typing import List
import tenacity
from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    JsonResponseHandler,
)
from apiclient.retrying import retry_if_api_request_error
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from config import logs_dir
from utils import manage_logger
import mappings

# Base URL for Crossref API
crossref_api_base_url = "https://api.crossref.org"

# Load environment variables
load_dotenv(os.path.join(os.getcwd(), ".env"))
crossref_email = os.environ.get("CROSSREF_EMAIL")

# List of accepted document types (using the same mapping as for OpenAlex to ensure compatibility)
accepted_doctypes = [
    key for key in mappings.doctypes_mapping_dict["source_crossref"].keys()
]

# Retry decorator to handle errors (e.g., too many requests, HTTP status code 429)
retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=crossref_api_base_url)
class CrossrefEndpoint:
    works = "works"
    work_doi = "works/{doi}"  # Endpoint to retrieve a record by DOI
    prefix = "prefixes/{prefix}"  # Endpoint to retrieve prefix information


class CrossrefClient(APIClient):
    log_file_path = os.path.join(logs_dir, "logging.log")
    logger = manage_logger(log_file_path)

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Basic search query to the Crossref API.

        Example URL generated:
            https://api.crossref.org/works?query=python&rows=5&offset=0

        Usage:
            CrossrefClient.search_query(query="python", per_page=5, page=1)

        Returns:
            A JSON object containing the search results.
        """
        # Add contact email if available (Crossref expects the "mailto" parameter)
        if crossref_email:
            param_kwargs.setdefault("mailto", crossref_email)

        rows = param_kwargs.pop("rows", 50)
        offset = param_kwargs.pop("offset", 0)
        param_kwargs["rows"] = rows
        param_kwargs["offset"] = offset

        self.params = param_kwargs
        return self.get(CrossrefEndpoint.works, params=self.params)

    @retry_request
    def count_results(self, **param_kwargs) -> int:
        """
        Count the total number of results for a given search query.

        Example URL generated:
            https://api.crossref.org/works?query=python&rows=1&offset=0

        Usage:
            CrossrefClient.count_results(query="python")

        Returns:
            The total count of results for the query.
        """
        if crossref_email:
            param_kwargs.setdefault("mailto", crossref_email)
        # Set the pagination to 1 result per page for counting
        param_kwargs["rows"] = 1
        param_kwargs["offset"] = 0

        self.params = param_kwargs
        result = self.search_query(**self.params)
        return result["message"]["total-results"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs) -> List[str]:
        """
        Retrieve a list of DOIs corresponding to a search query.

        Example URL generated:
            https://api.crossref.org/works?query=python&rows=10&offset=0

        Usage:
            CrossrefClient.fetch_ids(query="python", per_page=10)

        Returns:
            A list of DOIs extracted from the results.
        """
        if crossref_email:
            param_kwargs.setdefault("mailto", crossref_email)

        rows = param_kwargs.pop("rows", 50)
        offset = param_kwargs.pop("offset", 0)
        param_kwargs["rows"] = rows
        param_kwargs["offset"] = offset

        self.params = param_kwargs
        results = self.search_query(**self.params)
        items = results["message"]["items"]
        return [x.get("DOI", "") for x in items]

    @retry_decorator
    def fetch_records(self, format="digest", **param_kwargs):
        """
        Retrieve records from the Crossref API and process them into the desired format.

        Args:
            format (str): Desired output format ("digest", "digest-ifs3", "ifs3", or "crossref").
            **param_kwargs: Query parameters for Crossref API (e.g., query, filter, rows, offset, mailto).

        Returns:
            A list of processed records or None if no results are found.
        """
        # Add email if provided for polite API usage
        if crossref_email:
            param_kwargs.setdefault("mailto", crossref_email)

        # Save for reuse/debugging if needed
        self.params = param_kwargs

        # Perform the API query
        result = self.search_query(**self.params)

        # Process only if results are found
        if result.get("message", {}).get("total-results", 0) > 0:
            return self._process_fetch_records(format, **self.params)

        return None

    @retry_decorator
    def fetch_record_by_unique_id(self, doi):
        """
        Retrieve a specific record by its DOI.

        Example URL generated:
            https://api.crossref.org/works/10.5555/12345678

        Usage:
            CrossrefClient.fetch_record_by_unique_id("10.5555/12345678")

        Returns:
            A processed record (default output format is "digest").
        """
        if crossref_email:
            self.params = {"mailto": crossref_email}
        else:
            self.params = {}
        result = self.get(CrossrefEndpoint.work_doi.format(doi=doi), params=self.params)
        return (
            self._process_record(result["message"], format="digest") if result else None
        )

    def _process_fetch_records(self, format, **param_kwargs):
        """
        Process the retrieved records into the desired output format.

        Args:
            format (str): Output format ("digest", "digest-ifs3", "ifs3", or "crossref").
            **param_kwargs: Query parameters.

        Returns:
            A list of processed records.
        """
        records = self.search_query(**self.params)["message"]["items"]
        if format == "digest":
            return [self._extract_digest_record_info(record) for record in records]
        elif format == "digest-ifs3":
            return [self._extract_ifs3_digest_record_info(record) for record in records]
        elif format == "ifs3":
            return [self._extract_ifs3_record_info(record) for record in records]
        elif format == "crossref":
            return records

    def _process_record(self, x, format):
        """
        Process a single record into the desired output format.
        """
        if format == "digest":
            return self._extract_digest_record_info(x)
        elif format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(x)
        elif format == "ifs3":
            return self._extract_ifs3_record_info(x)
        elif format == "crossref":
            return x

    def _is_valid_doi(self, doi):
        """
        Checks that the string `doi` matches a basic DOI format.
        Example pattern: 10.<number>/<string>
        """
        pattern = r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$"
        return re.match(pattern, doi, re.IGNORECASE) is not None

    def _extract_digest_record_info(self, x):
        """
        Extracts the main metadata from a Crossref record.

        Args:
            x (dict): The Crossref record (usually the "message" node).

        Returns:
            dict: Dictionary containing the extracted metadata.
        """
        self.logger.debug(f"Processing Crossref record: {x.get('DOI', '')}")

        # Déterminer le type de publication
        aggregation_type = x.get("type", "").lower()

        # Initialize separated fields for publicationName
        journal_title = ""
        series_title = ""
        book_title = ""

        # Initialize separated fields for ISSN, ISBN, and volume
        journal_issn = ""
        series_issn = ""
        journal_volume = ""
        series_volume = ""
        book_part = ""
        book_doi = ""
        publisher = ""

        # Extraction de l'ISSN pour les journaux (la clé "ISSN" est généralement une liste)
        issn_field = x.get("ISSN", "")

        container_title = x.get("container-title", [])

        if aggregation_type in ["journal-article"]:
            journal_title = container_title[0] if container_title else ""
            journal_issn = self._normalize_issn(issn_field)
            journal_volume = x.get("volume", "")

        elif aggregation_type in [
            "book",
            "book-chapter",
            "proceedings",
            "proceedings-article",
            "edited-book",
        ]:
            publisher = x.get("publisher", "")
            # On teste le nombre d’éléments dans container_title
            if len(container_title) >= 2:
                # Si au moins deux valeurs sont présentes, on affecte :
                # - la première à series_title,
                # - la deuxième à book_title.
                series_title = container_title[0]
                series_volume = x.get("volume", "")
                series_issn = self._normalize_issn(issn_field)
                book_title = container_title[1]
            elif len(container_title) == 1:
                # Si une seule valeur, c’est le book_title qui est renseigné
                book_title = container_title[0]

        issue = x.get("issue", "")

        pages = x.get("page", "")
        starting_page = ""
        ending_page = ""
        if pages:
            if "-" in pages:
                parts = pages.split("-", 1)
                starting_page = parts[0].strip()
                ending_page = parts[1].strip()
            else:
                starting_page = pages.strip()

        book_isbn = self._extract_all_isbns(x.get("ISBN", ""))
        issue_date = self._extract_publication_date(x)
        keywords = self._extract_keywords(x)
        editors = self._extract_editors(x)

        main_doi = x.get("DOI", "")
        alternative_ids = x.get("alternative-id", [])
        alt_doi_list = []

        if alternative_ids:
            # Retirer dans la liste le DOI principal s'il s'y trouve
            remaining_ids = [alt for alt in alternative_ids if alt != main_doi]
            # Si au moins une alternative est différente, on traite ces identifiants
            if remaining_ids:
                for alt in remaining_ids:
                    if self._is_valid_doi(alt):
                        alt_doi_list.append(alt)

        # Pour les types de publication de type livre, on ajoute les alternatifs
        if aggregation_type in [
            "book",
            "book-chapter",
            "proceedings",
            "proceedings-article",
            "edited-book",
        ]:
            # Le ou les DOIs alternatifs seront ajoutés dans "bookDOI"
            book_doi = "||".join(alt_doi_list)

        if aggregation_type in [
            "posted-content",
        ]:
            prefix_doi = x.get("prefix", "")
            publisher = self.fetch_prefix_name(prefix_doi)

        title = x.get("title", [""])[0] if x.get("title") else ""
        cleaned_title = re.sub(r"\${2,}", "$", title.strip())

        return {
            "source": "crossref",
            "internal_id": x.get("DOI", ""),
            "issueDate": issue_date,
            "doi": self._extract_doi(x),
            "title": cleaned_title,
            "doctype": self._extract_first_doctype(x),
            "pubyear": self._extract_pubyear(x),
            "publisher": publisher,
            "publisherPlace": x.get("publisher-location", ""),
            "journalTitle": journal_title,
            "seriesTitle": series_title,
            "bookTitle": book_title,
            "editors": editors,
            "journalISSN": journal_issn,
            "seriesISSN": series_issn,
            "bookISBN": book_isbn,
            "bookDOI": book_doi,
            "journalVolume": journal_volume,
            "seriesVolume": series_volume,
            "bookPart": book_part,
            "issue": issue,
            "startingPage": starting_page,
            "endingPage": ending_page,
            "pmid": "",
            "artno": x.get("article-number", ""),
            "corporateAuthor": "",
            "keywords": keywords,
        }

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
        rec["authors"] = self._extract_ifs3_authors(x)
        rec["conference_info"] = self._extract_conference_info(x)
        rec["fundings_info"] = self._extract_funding_info(x)
        return rec

    def _remove_jats_from_abstract(self, abstract_text: str) -> str | None:
        """
        Cleans the abstract text by removing JATS XML tags while preserving and processing
        math expressions (e.g., LaTeX/MathJax). Extra whitespace is normalized and any
        doubled dollar signs are collapsed to a single $ delimiter.

        Args:
            abstract_text (str): Abstract text containing XML tags.

        Returns:
            str: Cleaned abstract text, or None if no abstract text is provided.
        """
        if not abstract_text:
            return None

        soup = BeautifulSoup(abstract_text, "lxml")

        # Remove title tags (e.g., generic titles like "Abstract")
        for title in soup.find_all("jats:title"):
            title.decompose()

        def normalize_text(text: str) -> str:
            # Remove extra whitespace and newlines.
            return " ".join(text.split())

        # Process all inline formula tags first.
        for inline_formula in soup.find_all("jats:inline-formula"):
            alternatives = inline_formula.find("jats:alternatives")
            if alternatives:
                tex_math = alternatives.find("jats:tex-math")
                if tex_math:
                    # Use the LaTeX version if available.
                    math_text = normalize_text(tex_math.get_text(separator=" ", strip=True))
                else:
                    math_text = normalize_text(
                        inline_formula.get_text(separator=" ", strip=True)
                    )
            else:
                math_text = normalize_text(
                    inline_formula.get_text(separator=" ", strip=True)
                )
            # Replace the inline formula tag with its normalized text.
            inline_formula.replace_with(math_text)

        # Process any remaining standalone math tags (jats:tex-math or mml:math)
        # that are not part of an alternatives element.
        for math_tag in soup.find_all(["jats:tex-math", "mml:math"]):
            if math_tag.find_parent("jats:alternatives"):
                continue  # Already handled in inline formulas.
            math_text = normalize_text(math_tag.get_text(separator=" ", strip=True))
            math_tag.replace_with(math_text)

        # Concatenate text from all paragraph tags.
        paragraphs = []
        for para in soup.find_all("jats:p"):
            para_text = normalize_text(para.get_text(separator=" ", strip=True))
            if para_text:
                paragraphs.append(para_text)
        cleaned_text = " ".join(paragraphs)

        # Collapse multiple consecutive dollar signs into a single dollar sign.
        cleaned_text = re.sub(r"\${2,}", "$", cleaned_text)

        return cleaned_text.strip()

    def _extract_abstract(self, x):
        """
        Extracts and cleans the abstract from a Crossref record.
        If the abstract contains JATS XML tags, this function converts them into plain text,
        processing math expressions (LaTeX/MathJax) accordingly while ensuring the dollar
        delimiters are not doubled.

        Args:
            x (dict): The Crossref record.

        Returns:
            str: The cleaned abstract text, or an empty string if the abstract is not available.
        """
        abstract_text = x.get("abstract", "")
        if abstract_text and isinstance(abstract_text, str):
            return self._remove_jats_from_abstract(abstract_text)
        return ""

    def _extract_conference_info(self, x):
        """
        Extracts conference information from a Crossref record and formats it as:
            'conference_title::conference_location::conference_startdate::conference_enddate::conference_acronym'.

        This method first looks for an "event" object in the record. If present, it extracts:
        - conference title from event["name"],
        - location from event["location"],
        - start date from event["start"] (using a helper function to format dates),
        - end date from event["end"] (using a helper function to format dates),
        - and conference acronym from event["acronym"] if available.

        If the "event" object is not present, the method tries to extract the same information from assertions
        related to "ConferenceInfo". The acronym is then obtained from the assertion with key "conference_acronym".

        Args:
            x (dict): The Crossref record.

        Returns:
            str: A formatted string with conference information, or an empty string if no data is found.
        """
        try:
            event = x.get("event", None)
            if event:
                conference_title = event.get("name", "")
                location = event.get("location", "")
                start_date = self._convert_date(event.get("start", {}))
                end_date = self._convert_date(event.get("end", {}))
                acronym = event.get("acronym", "")
                return (
                    f"{conference_title}::{location}::{start_date}::{end_date}::{acronym}"
                )
            elif "assertion" in x:
                assertions = x.get("assertion", [])
                conf_assertions = {}
                # Extraire les assertions appartenant au groupe ConferenceInfo
                for item in assertions:
                    group = item.get("group", {})
                    if group.get("name", "").lower() == "conferenceinfo":
                        key = item.get("name", "").lower()
                        value = item.get("value", "").strip()
                        conf_assertions[key] = value

                if conf_assertions:
                    conference_name = conf_assertions.get("conference_name", "")
                    conference_number = conf_assertions.get("conference_number", "")
                    if conference_number:
                        ordinal_suffix = self._get_ordinal_suffix(conference_number)
                        conference_title = (
                            f"{conference_number}{ordinal_suffix} {conference_name}"
                        )
                    else:
                        conference_title = conference_name

                    conference_city = conf_assertions.get("conference_city", "")
                    conference_country = conf_assertions.get("conference_country", "")
                    if conference_city and conference_country:
                        location = f"{conference_city}, {conference_country}"
                    elif conference_city:
                        location = conference_city
                    elif conference_country:
                        location = conference_country
                    else:
                        location = ""

                    # Convert the assertion dates using the separate conversion function
                    raw_start_date = conf_assertions.get("conference_start_date", "")
                    raw_end_date = conf_assertions.get("conference_end_date", "")
                    start_date = self._convert_assertion_date(raw_start_date)
                    end_date = self._convert_assertion_date(raw_end_date)
                    acronym = conf_assertions.get("conference_acronym", "")
                    return f"{conference_title}::{location}::{start_date}::{end_date}::{acronym}"
                else:
                    return ""
            else:
                return ""
        except Exception as e:
            self.logger.error(f"Error extracting conference info: {e}")
            return ""

    def _convert_assertion_date(self, raw_date: str) -> str:
        """
        Converts a raw assertion date string into the format YYYY-MM-DD.
        Expected input format: '%d %B %Y' (e.g., '26 September 2016').

        Args:
            raw_date (str): The raw date string from an assertion.

        Returns:
            str: The formatted date string in YYYY-MM-DD format, or the original string
                if conversion fails.
        """
        from datetime import datetime
        if not raw_date:
            return ""
        try:
            dt = datetime.strptime(raw_date, "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except Exception as e:
            self.logger.error(f"Error converting assertion date '{raw_date}': {e}")
            return raw_date

    def _get_ordinal_suffix(self, number):
        """
        Determine the correct ordinal suffix for a given number.

        Args:
            number (int): The number for which to determine the suffix.

        Returns:
            str: The ordinal suffix ("-st", "-nd", "-rd", "-th").
        """
        # Convert to integer if the number is provided as a string
        try:
            number = int(number)
        except ValueError:
            return ""  # Return empty string if conversion fails

        # Special cases for numbers ending in 11, 12, or 13
        if 11 <= number % 100 <= 13:
            return "th"

        # Determine suffix based on the last digit
        last_digit = number % 10
        if last_digit == 1:
            return "st"
        elif last_digit == 2:
            return "nd"
        elif last_digit == 3:
            return "rd"
        else:
            return "th"

    def _convert_date(self, date_obj):
        """
        Formats an event date object containing 'date-parts' into a string.

        Args:
            date_obj (dict): An event date object from Crossref (e.g., event['start'] or event['end']).

        Returns:
            str: A formatted date string (YYYY-MM-DD, YYYY-MM, or YYYY) or an empty string if not available.
        """
        try:
            date_parts = date_obj.get("date-parts", [])
            if date_parts and len(date_parts) > 0:
                parts = date_parts[0]
                if len(parts) == 3:
                    return f"{parts[0]}-{str(parts[1]).zfill(2)}-{str(parts[2]).zfill(2)}"
                elif len(parts) == 2:
                    return f"{parts[0]}-{str(parts[1]).zfill(2)}"
                elif len(parts) == 1:
                    return f"{parts[0]}"
        except Exception as e:
            self.logger.error(f"Error formatting event date: {e}")
        return ""

    def _extract_funding_info(self, x):
        """
        Extracts and consolidates funding information from a Crossref record.

        The Crossref record includes the 'funder' field, which is a list of funder objects.
        This method groups funding entries by funder and concatenates any award identifiers.

        Args:
            x (dict): The Crossref record.

        Returns:
            str: A formatted string with consolidated funding information,
                or an empty string if no funding data is available.
        """
        try:
            funders = x.get("funder", [])
            if not funders:
                return ""
            funding_infos = []
            for funder in funders:
                name = funder.get("name", "").strip()
                award = funder.get("award", [])
                if isinstance(award, list):
                    award_str = ",".join(award)
                elif isinstance(award, str):
                    award_str = award
                else:
                    award_str = ""
                funding_infos.append(f"{name}::{award_str}")
            return "||".join(funding_infos)
        except Exception as e:
            self.logger.error(f"Error extracting funding information: {e}")
            return ""

    def _extract_doi(self, x):
        """
        Extract the DOI from a Crossref record.

        Args:
            x (dict): A Crossref record.

        Returns:
            str: The DOI in lowercase.
        """
        doi = x.get("DOI", "")
        return doi.lower() if isinstance(doi, str) else ""

    def _extract_first_doctype(self, x):
        """
        Extracts the document type from a Crossref record.
        
        If the record's 'assertion' object contains a 'conference_name' value
        (i.e., conference-related information is available), the document type is overridden:
            - 'book' becomes 'proceedings'
            - 'book-chapter' becomes 'proceedings-article'
        
        Args:
            x (dict): A Crossref record.
        
        Returns:
            str: The original or overridden document type based on the presence of conference information.
        """
        doctype = x.get("type", "")

        # Check if the record contains conference-related assertions
        if "assertion" in x:
            assertions = x.get("assertion", [])
            for item in assertions:
                group = item.get("group", {})
                # Identify assertions belonging to the ConferenceInfo group
                if group.get("name", "").lower() == "conferenceinfo":
                    # Check if the assertion relates to conference_name and has a non-empty value
                    if item.get("name", "").lower() == "conference_name" and item.get("value", "").strip():
                        if doctype.lower() == "book":
                            return "proceedings"
                        elif doctype.lower() == "book-chapter":
                            return "proceedings-article"
                        # Stop after overriding if applicable
                        break
        return doctype

    def _extract_publication_date(self, x):
        """
        Extracts and formats the publication date from a Crossref record.
        The 'published' field typically contains 'date-parts', for example [[2020, 12, 05]].

        Args:
            x (dict): The Crossref record.

        Returns:
            str: The formatted date ("YYYY-MM-DD", "YYYY-MM", or "YYYY"), or an empty string if not available.
        """
        try:
            published = x.get("published", {})
            return self._convert_date(published)
        except Exception as e:
            self.logger.error(f"Error extracting publication date: {e}")
            return ""

    def _extract_pubyear(self, x):
        """
        Extract the publication year from a Crossref record.

        Args:
            x (dict): A Crossref record.

        Returns:
            int or None: The publication year.
        """
        issued = x.get("issued", {})
        date_parts = issued.get("date-parts", [])
        if (
            date_parts
            and isinstance(date_parts, list)
            and len(date_parts) > 0
            and len(date_parts[0]) > 0
        ):
            return date_parts[0][0]
        return None

    def get_dc_type_info(self, x):
        """
        Retrieve the dc.type and dc.type_authority attributes for a given document type.

        Args:
            x (dict): A Crossref record.

        Returns:
            dict: A dictionary with "dc.type" and "dc.type_authority".
        """
        data_doctype = self._extract_first_doctype(x)
        doctype_mapping = mappings.doctypes_mapping_dict.get("source_crossref", {})
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
            mapped_value = mappings.doctypes_mapping_dict["source_crossref"].get(
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

    def _extract_author_orcid(self, author):
        """
        Extract the ORCID for an author, removing any URL prefix if present.

        Args:
            author (dict): Author information.

        Returns:
            str: The ORCID without the prefix, or an empty string.
        """
        ORCID_PREFIX = "https://orcid.org/"
        orcid = author.get("ORCID", "")

        if isinstance(orcid, str):
            orcid = orcid.strip()
            if orcid.startswith(ORCID_PREFIX):
                # Retourne l'ORCID sans le préfixe
                return orcid[len(ORCID_PREFIX) :]
            return orcid

        return ""

    def _extract_ifs3_authors(self, x):
        """
        Extract author information in ifs3 format from a Crossref record.

        Args:
            x (dict): A Crossref record.

        Returns:
            list of dict: A list of author information dictionaries.
        """
        authors = []
        for author in x.get("author", []):
            given = author.get("given", "")
            family = author.get("family", "")
            full_name = f"{family}, {given}".strip()
            affiliations = author.get("affiliation", [])
            orgs = "|".join([aff.get("name", "") for aff in affiliations])
            authors.append(
                {
                    "author": full_name,
                    "internal_author_id": "",  # Crossref does not provide a unique author identifier
                    "orcid_id": self._extract_author_orcid(author),
                    "organizations": orgs,
                }
            )
        return authors

    def _normalize_issn(self, issn_field):
        """
        Normalise les valeurs ISSN en s'assurant du format XXXX-XXXX.
        Si plusieurs ISSN sont présents, ils sont joint par '||'.

        Args:
            issn_field (str or list): La ou les valeurs ISSN.

        Returns:
            str: ISSN(s) normalisé(s).
        """
        if not issn_field:
            return ""
        if isinstance(issn_field, list):
            normalized_issns = []
            for issn in issn_field:
                if "-" in issn:
                    normalized_issns.append(issn)
                else:
                    if len(issn) == 8:
                        normalized_issns.append(issn[:4] + "-" + issn[4:])
                    else:
                        normalized_issns.append(issn)
            return "||".join(normalized_issns)
        if isinstance(issn_field, str):
            issns = [s.strip() for s in issn_field.split(",")]
            return self._normalize_issn(issns)
        return ""

    def _extract_all_isbns(self, isbn_field):
        """
        Extrait toutes les valeurs ISBN de l'enregistrement Crossref.

        Args:
            isbn_field (str or list): Le ou les ISBN.

        Returns:
            str: ISBN(s) concaténés par '||' ou une chaîne vide.
        """
        if not isbn_field:
            return ""
        if isinstance(isbn_field, list):
            return "||".join(isbn_field)
        elif isinstance(isbn_field, str):
            return isbn_field
        return ""

    def _extract_keywords(self, x):
        """
        Extrait les mots-clés depuis le champ 'subject' de l'enregistrement Crossref.

        Args:
            x (dict): L'enregistrement Crossref.

        Returns:
            str: Mots-clés séparés par '||' ou une chaîne vide.
        """
        subjects = x.get("subject", [])
        if subjects:
            if isinstance(subjects, list):
                return "||".join(subjects)
            elif isinstance(subjects, str):
                return subjects
        return ""

    def _extract_editors(self, x):
        """
        Extracts editors from the Crossref record if available.
        Expects the 'editor' key containing a list of dictionaries with 'given' and 'family'.

        Args:
            x (dict): The Crossref record.

        Returns:
            str: The editors formatted as "Last name, First name" separated by '||'.
        """
        editors = []
        if "editor" in x:
            for editor in x["editor"]:
                given = editor.get("given", "")
                family = editor.get("family", "")
                if given or family:
                    name = f"{family}, {given}" if family and given else f"{given}{family}"
                    editors.append(name.strip())
        return "||".join(editors)

    @retry_decorator
    def fetch_prefix_name(self, prefix: str) -> str:
        """
        Retrieve the issuer name for a given DOI prefix using the Crossref API.

        Example usage:
            issuer_name = CrossrefClient.fetch_prefix_name(prefix="10.26434")

        The expected API response is similar to:
            {
                "status": "ok",
                "message-type": "prefix",
                "message-version": "1.0.0",
                "message": {
                "member": "https://id.crossref.org/member/316",
                "name": "American Chemical Society (ACS)",
                "prefix": "https://id.crossref.org/prefix/10.26434"
                }
            }

        Returns:
            str: The issuer name (e.g., "American Chemical Society (ACS)") 
                    or an empty string if the request fails.
        """
        params = {}
        if crossref_email:
            params["mailto"] = crossref_email
        response = self.get(CrossrefEndpoint.prefix.format(prefix=prefix), params=params)
        if response and response.get("status") == "ok":
            message = response.get("message", {})
            return message.get("name", "")
        else:
            self.logger.error(f"Error retrieving prefix info for {prefix}")
            return ""


# Initialize the CrossrefClient with a JSON response handler
CrossrefClient = CrossrefClient(
    response_handler=JsonResponseHandler,
)

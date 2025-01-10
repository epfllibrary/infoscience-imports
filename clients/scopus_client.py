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
import ast
import os
import time
import pycountry
from dotenv import load_dotenv
from utils import manage_logger
import mappings
from config import logs_dir

scopus_api_base_url = "https://api.elsevier.com/content"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
scopus_api_key = os.environ.get("SCOPUS_API_KEY")
scopus_inst_token = os.environ.get("SCOPUS_INST_TOKEN")

accepted_doctypes = [key for key in mappings.doctypes_mapping_dict["source_scopus"].keys()]

scopus_authentication_method = HeaderAuthentication(
    token=scopus_api_key,
    parameter="X-ELS-APIKey",
    scheme=None,
    extra={"X-ELS-Insttoken": scopus_inst_token},
)

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=scopus_api_base_url)
class Endpoint:
    base = ""
    search = "search/scopus"
    abstract = "abstract"
    scopusId = "abstract/scopus_id/{scopusId}"
    doi = "abstract/doi/{doi}"


class Client(APIClient):
    log_file_path = os.path.join(logs_dir, "scopus_client.log")
    logger = manage_logger(log_file_path)

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Base request example
        https://api.elsevier.com/content/search/scopus?query=all(gene)&count=5&start=1&view=COMPLETE

        Default args (can be orverwritten)
        view is set to STANDARD (native Scopus API)

        Usage
        ScopusClient.search_query(query="all(gene)",count=5,start=1,view=COMPLETE)

        Returns
        A json object of Wos records
        """
        self.params = {**param_kwargs}
        # return self.get(wos_api_base_url, params=self.params)
        return self.get(Endpoint.search, params=self.params)

    @retry_request
    def count_results(self, **param_kwargs)-> int:
        """
        Base request example
        https://api.elsevier.com/content/search/scopus?query=all(gene)&count=1&start=1&field=dc:identifier

        Default args (can be orverwritten)
        count (number of returned records) is set to 1
        start (first record) is set to 1
        field is set to dc:identifier to get a minimal record

        Usage
        ScopusClient.count_results(query="all(gene)")
        ScopusClient.count_results(query="all(gene)",count=1,firstRecord=1)

        Returns
        The number of records found for the request
        """
        param_kwargs.setdefault('count', 1)
        param_kwargs.setdefault('start', 0)
        param_kwargs.setdefault('field', "dc:identifier") #to get minimal records
        self.params = {**param_kwargs}
        return self.search_query(**self.params)["search-results"]["opensearch:totalResults"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs) -> List[str]:
        """
        Fetches SCOPUS IDs based on the query parameters, handling cases with no or single results.
        """
        param_kwargs.setdefault('count', 10)
        param_kwargs.setdefault('start', 0)
        param_kwargs.setdefault('field', "dc:identifier")  # Minimal records

        self.params = {**param_kwargs}
        response = self.search_query(**self.params)
        entries = response.get("search-results", {}).get("entry", [])

        if not entries:
            return []

        ids = []
        for entry in entries:
            if "dc:identifier" in entry:
                ids.append(entry["dc:identifier"])

        return ids

    @retry_decorator
    def fetch_records(self, format="digest", **param_kwargs):
        """
        Fetch records using fetch_ids and fetch_record_by_unique_id.

        Args:
            format: digest|digest-ifs3|ifs3|scopus
            **param_kwargs: query parameters for fetching IDs.

        Returns:
            A list of records in the requested format.
        """
        param_kwargs.setdefault("start", 0)
        param_kwargs.setdefault("count", 10)
        delay_between_calls = 1  # Add a 1-second delay between API calls

        try:
            # Fetch IDs first
            ids = self.fetch_ids(**param_kwargs)
            self.logger.debug(f"IDs fetched from scopus: {ids}")

            # Fetch records for each ID
            records = []
            for scopus_id in ids:
                record = self.fetch_record_by_unique_id(scopus_id, format=format)
                if record:
                    records.append(record)
                else:
                    self.logger.warning(f"Skipped record for ID {scopus_id}")
                time.sleep(delay_between_calls)  # Wait before the next call

            return records

        except Exception as e:
            self.logger.error(f"An error occurred while fetching records: {e}")
            return None

    @retry_decorator
    def fetch_record_by_unique_id(self, scopus_id, format="digest"):
        """
        Base request example
        https://api.elsevier.com/content/abstract/scopus_id/SCOPUS_ID:85145343484

        Args:
            format: digest|digest-ifs3|ifs3|scopus

        Usage:
            ScopusClient.fetch_record_by_unique_id("SCOPUS_ID:85200150104")
            ScopusClient.fetch_record_by_unique_id("SCOPUS_ID:85200150104", format="scopus")
            ScopusClient.fetch_record_by_unique_id("SCOPUS_ID:85200150104", format="ifs3")
        """
        try:
            headers = {"Accept": "application/json"}
            result = self.get(
                Endpoint.scopusId.format(scopusId=scopus_id),
                headers=headers
            )
            item = result.get("abstracts-retrieval-response", {})
            return self._process_record(item, format)

        except Exception as e:
            self.logger.error(f"Error fetching record by unique ID {scopus_id}: {e}")
            return None

    def _process_record(self, record, format):
        if format == "digest":
            return self._extract_digest_record_info(record)
        elif format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(record)
        elif format == "ifs3":
            return self._extract_ifs3_record_info(record)
        elif format == "scopus":
            return record

    def _extract_digest_record_info(self, x):
        """
        Extracts core metadata fields from the Scopus record.

        This function parses and processes a Scopus record to extract important metadata
        fields such as Scopus ID, title, DOI, document type, publication year, and more.
        It separates fields such as "publicationName", "issn", "isbn", and "volume"
        into specific fields based on their context (e.g., journal, book, or series).

        Args:
            x (dict): The Scopus record in JSON format.

        Returns:
            dict: A dictionary containing the extracted metadata fields.
        """
        coredata = x.get("coredata", {})
        self.logger.debug(f"Processing Scopus record: {coredata.get('eid')}")

        # Extract prism:aggregationType to determine the type of publication
        aggregation_type = coredata.get("prism:aggregationType", "").lower()

        # Initialize separated fields for publicationName
        journal_title = ""
        series_title = ""
        book_title = ""

        # Initialize separated fields for ISSN, ISBN, and volume
        journal_issn = ""
        series_issn = ""
        book_isbn = ""
        journal_volume = ""
        series_volume = ""
        book_part = ""
        book_publisher = ""

        bibrecord = x.get("item", {}).get("bibrecord", {})

        # Separate publicationName into specific fields based on aggregationType
        publication_name = coredata.get("prism:publicationName", "")

        if aggregation_type == "journal":
            journal_title = publication_name
            journal_issn = self._normalize_issn(coredata.get("prism:issn", ""))
            journal_volume = coredata.get("prism:volume", "")

        elif aggregation_type in {"book", "conference proceeding"}:
            book_isbn = self._extract_all_isbns(coredata.get("prism:isbn", ""))
            book_part = coredata.get("prism:volume", "")
            book_publisher = coredata.get("dc:publisher", "")
            book_title = publication_name

            # specific case for "conference proceeding"
            if (
                aggregation_type == "conference proceeding"
                and (not book_isbn or book_isbn.strip() == "")
                and coredata.get("prism:volume", "").strip()
            ):
                series_title = publication_name
                series_volume = coredata.get("prism:volume", "")
                series_issn = self._normalize_issn(coredata.get("prism:issn", ""))
                book_title = ""
                book_part = ""

        elif aggregation_type == "book series":
            series_title = publication_name
            series_issn = self._normalize_issn(coredata.get("prism:issn", ""))
            series_volume = coredata.get("prism:volume", "")
            book_publisher = coredata.get("dc:publisher", "")

            # Check for a book title in the `bibrecord` node
            source = bibrecord.get("head", {}).get("source", {})
            book_title_from_source = source.get("issuetitle", "")
            if book_title_from_source:
                book_title = book_title_from_source
                self.logger.debug(f"Found container title in source: {book_title}")

        # Extract collaboration.ce:text
        author_group = bibrecord.get("head", {}).get("author-group", [])
        collaboration_text = ""

        if isinstance(author_group, list):
            for group in author_group:
                collaboration = group.get("collaboration", {})
                if collaboration and "ce:text" in collaboration:
                    collaboration_text = collaboration["ce:text"]

        self.logger.debug(f"Collected collaboration texts: {collaboration_text}")

        # Return a dictionary of the extracted metadata fields
        return {
            "source": "scopus",
            "internal_id": coredata.get("eid"),
            "issueDate": self._extract_publication_date(x),
            "doi": coredata.get("prism:doi", "").lower(),
            "title": coredata.get("dc:title"),
            "doctype": self._extract_first_doctype(x),
            "pubyear": coredata.get("prism:coverDate", "")[:4],
            "publisher": book_publisher,
            "journalTitle": journal_title,
            "seriesTitle": series_title,
            "bookTitle": book_title,
            "editors": self._extract_editors_from_scopus_record(x),
            "journalISSN": journal_issn,
            "seriesISSN": series_issn,
            "bookISBN": book_isbn,
            "journalVolume": journal_volume,
            "seriesVolume": series_volume,
            "bookPart": book_part,
            "issue": coredata.get("prism:issueIdentifier", ""),
            "startingPage": coredata.get("prism:startingPage", ""),
            "endingPage": coredata.get("prism:endingPage", ""),
            "pmid": coredata.get("pubmed-id", ""),
            "artno": coredata.get("article-number", ""),
            "corporateAuthor": collaboration_text,
        }

    def _extract_ifs3_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  scopus_id, title, DOI, doctype, pubyear, ifs3_collection, ifs3_collection_id
        """
        record = self._extract_digest_record_info(x)
        record["ifs3_collection"] = self._extract_ifs3_collection(x)
        record["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        # Get dc.type and dc.type_authority for the document type
        type_info = self.get_dc_type_info(x)
        record["dc.type"] = type_info.get("dc.type", "unknown")
        record["dc.type_authority"] = type_info.get("dc.type_authority", "unknown")

        return record

    def _extract_ifs3_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  scopus_id, title, DOI, doctype, pubyear, ifs3_collection, ifs3_collection_id, authors
        """
        rec = self._extract_ifs3_digest_record_info(x)
        rec["abstract"] = self._extract_abstract(x)
        authors = self._extract_ifs3_authors(x)
        rec["authors"] = authors
        rec["conference_info"] = self._extract_conference_info(x)
        rec["fundings_info"] = self._extract_funding_info(x)

        return rec

    def _extract_publication_date(self, x):
        """
        Extracts the publication date from the Scopus API response.

        Parameters:
        x (dict): JSON data from the Scopus API response.

        Returns:
        str: The formatted publication date ("YYYY-MM-DD", "YYYY-MM", or "YYYY") depending on available data, or "" if not available.
        """
        try:
            self.logger.debug("Attempting to extract publication date.")

            # Access the publicationdate node in the specific path
            publication_date_node = (
                x.get("item", {})
                .get("bibrecord", {})
                .get("head", {})
                .get("source", {})
                .get("publicationdate", {})
            )

            if not publication_date_node:
                self.logger.warning("No publicationdate node found in the data.")
                return ""

            # Extract date components
            year = publication_date_node.get("year", "")
            month = publication_date_node.get("month", "")
            day = publication_date_node.get("day", "")

            # Construct the formatted date based on available components
            if year:
                if month:
                    if day:
                        formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        self.logger.debug(f"Formatted date with day: {formatted_date}")
                        return formatted_date
                    formatted_date = f"{year}-{month.zfill(2)}"
                    self.logger.debug(f"Formatted date without day: {formatted_date}")
                    return formatted_date
                self.logger.debug(f"Formatted year-only date: {year}")
                return year

            # If year is missing, log a warning
            self.logger.warning("Year is missing in publicationdate.")
            return ""

        except Exception as e:
            self.logger.error(f"Error extracting publicationDate: {e}")
            return ""

    def _extract_all_isbns(self, isbn_field):
        """
        Extract all ISBN values from the provided field, separated by '||'.

        Parameters
        ----------
        isbn_field : str or list
            The raw ISBN field from the Scopus API response, which may contain a single value or a list of dictionaries.

        Returns
        -------
        str : All ISBN values found, separated by '||', or an empty string if none are valid.
        """
        if not isbn_field:
            return ""

        isbns = []

        if isinstance(isbn_field, list):
            for item in isbn_field:
                if isinstance(item, dict) and "$" in item:
                    isbns.append(item["$"])
        elif isinstance(isbn_field, str):
            isbns.append(isbn_field)

        if isbns:
            return "||".join(isbns)

        self.logger.warning("No valid ISBN format encountered.")
        return ""

    def _normalize_issn(self, issn_field):
        """
        Normalize ISSN values to the format XXXX-XXXX.

        Parameters
        ----------
        issn_field : str
            The raw ISSN field from the Scopus API response, which may contain multiple ISSNs.

        Returns
        -------
        str : A single string containing normalized ISSNs separated by commas.
        """
        import re

        if not issn_field:
            return ""

        # Split multiple ISSNs (if any) by spaces or other delimiters (e.g., commas or pipes)
        issns = re.split(r"[\s,|]+", issn_field)

        # Normalize each ISSN to the format XXXX-XXXX
        normalized_issns = []
        for issn in issns:
            match = re.match(r"^(\d{4})(\d{3}[\dX])$", issn, re.IGNORECASE)
            if match:
                normalized_issn = f"{match.group(1)}-{match.group(2).upper()}"
                normalized_issns.append(normalized_issn)
            else:
                self.logger.warning(f"Invalid ISSN format encountered: {issn}")

        return "||".join(normalized_issns)

    def _extract_first_doctype(self, x):
        subtype = x.get("coredata", {}).get("subtypeDescription")
        if isinstance(subtype, list):
            return subtype[0] if subtype else None
        return subtype

    def _extract_abstract(self, x):
        """
        Extracts the abstract from the Scopus record.
        Handles cases where the abstract is None, null, or empty.
        """
        try:
            abstract_text = (
                x.get("item", {})
                .get("bibrecord", {})
                .get("head", {})
                .get("abstracts", "")
            )

            # Ensure the abstract_text is a string and strip whitespace
            if abstract_text and isinstance(abstract_text, str):
                abstract_text = abstract_text.strip()

            if abstract_text and abstract_text.startswith("©"):
                first_period_index = abstract_text.find('.')
                if first_period_index != -1:
                    abstract_text = abstract_text[first_period_index + 1:].strip()

            # Return the abstract text if it's non-empty, otherwise return an empty string
            if abstract_text:
                return abstract_text

            return ""

        except KeyError as e:
            self.logger.error(f"Error during abstract retrieval: {e}")
            return ""

    def _extract_editors_from_scopus_record(self, x):
        """
        Extracts contributors with the role of editors from the Scopus record.

        Args:
            x (dict): The Scopus record in JSON format.

        Returns:
            str: A string containing editors formatted as "Surname, Given-name" separated by "||".
        """
        try:
            # Navigate to the contributor-group in the JSON
            contributor_group = (
                x.get("item", {})
                .get("bibrecord", {})
                .get("head", {})
                .get("source", {})
                .get("contributor-group", [])
            )

            # If contributor-group is a dictionary, wrap it in a list for uniform processing
            if isinstance(contributor_group, dict):
                contributor_group = [contributor_group]

            # Helper function to extract and format editor names
            def extract_names(contributor):
                if contributor.get("@role") == "edit":
                    surname = contributor.get("ce:surname", "")
                    given_name = contributor.get("ce:given-name", "")
                    if surname and given_name:
                        return f"{surname}, {given_name}"
                return None

            # Collect all formatted editor names
            editors = []
            for contributor_entry in contributor_group:
                contributor = contributor_entry.get("contributor")

                if isinstance(contributor, list):
                    editors.extend(filter(None, (extract_names(c) for c in contributor)))
                elif isinstance(contributor, dict):
                    name = extract_names(contributor)
                    if name:
                        editors.append(name)

            # Join all formatted names with "||"
            return "||".join(editors) if editors else ""

        except Exception as e:
            # Log the error and return an empty string
            print(f"Error extracting editors: {e}")
            return ""

    def extract_orcids_from_bibrecord(self, bibrecord_data):
        """
        Extracts ORCID based on @auid from the provided bibrecord data,
        handling cases where author-group is a list or a single dictionary.
        """
        orcid_map = {}

        try:
            author_groups = bibrecord_data.get("head", {}).get("author-group", [])

            if isinstance(author_groups, dict):
                author_groups = [author_groups]

            if not isinstance(author_groups, list):
                self.logger.warning("Invalid format for 'author-group' in bibrecord data.")
                return orcid_map

            for author_group in author_groups:
                if not isinstance(author_group, dict):
                    self.logger.warning("Invalid author group format; skipping.")
                    continue

                authors = author_group.get("author", [])
                if not isinstance(authors, list):
                    self.logger.warning(
                        "Invalid format for 'author' in author group; skipping."
                    )
                    continue

                for author in authors:
                    if not isinstance(author, dict):
                        self.logger.warning("Invalid author format; skipping.")
                        continue

                    auid = author.get("@auid")
                    orcid = author.get("@orcid")

                    if auid and orcid:
                        orcid_map[auid] = orcid
                    elif auid:
                        self.logger.debug(f"Missing ORCID for author with @auid: {auid}")
                    else:
                        self.logger.debug("Missing @auid for an author; skipping.")

        except Exception as e:
            self.logger.error(f"An error occurred while extracting ORCID: {e}")

        return orcid_map

    def _extract_ifs3_authors(self, x):
        """
        Extracts author information and their affiliations from the provided JSON data.
        """
        result = []

        try:
            # Ensure required keys are present in the input
            if "affiliation" not in x or "authors" not in x:
                self.logger.debug(x)
                self.logger.warning(
                    "Input data must contain 'affiliation' and 'author' keys."
                )
                return result

            bibrecord_data = x.get("item", {}).get("bibrecord", None)
            if not bibrecord_data:
                self.logger.warning("No 'item.bibrecord' key found in the input data.")

            orcid_map = self.extract_orcids_from_bibrecord(bibrecord_data)

            # Check if the necessary keys are present
            authors_data = x.get("authors", {}).get("author", [])
            affiliations_data = x.get("affiliation", [])
            if not authors_data or not affiliations_data:
                self.logger.warning("No authors or affiliations data found.")
                return result

            # Ensure affiliations_data is always a list, even if it's a single dictionary
            if isinstance(affiliations_data, dict):  # If affiliation is a single dictionary
                affiliations_data = [affiliations_data]

            # Create a dictionary to map afid to their corresponding organization name and affiliation details
            affiliation_map = {
                aff.get("@id"): f"{aff.get('@id')}:{aff.get('affilname')}"
                for aff in affiliations_data
                if aff.get("@id") and aff.get("affilname")
            }

            # Process each author
            for author in authors_data:
                # Prioritize 'preferred-name' fields
                preferred_name = author.get("preferred-name", {})
                surname = preferred_name.get("ce:surname", author.get("ce:surname", ""))
                given_name = preferred_name.get("ce:given-name", author.get("ce:given-name", ""))
                name = f"{surname}, {given_name}".strip(", ")
                self.logger.debug(f"Processing Author : {name}")

                if not name:
                    self.logger.warning("Author name is missing; skipping author.")
                    continue

                internal_author_id = author.get("@auid")
                orcid_id = orcid_map.get(internal_author_id)

                if not internal_author_id:
                    self.logger.warning(
                        f"Missing internal author ID for author '{name}'; skipping."
                    )
                    continue

                # Check if affiliation is either a dictionary or a list
                affiliations = author.get("affiliation", [])
                if isinstance(affiliations, dict):  # If affiliation is a single dictionary
                    affiliations = [affiliations]
                elif not isinstance(
                    affiliations, list
                ):  # If affiliation is of an invalid type
                    self.logger.warning(
                        f"Invalid affiliation type for author '{name}'; skipping."
                    )
                    continue

                # Map affiliation IDs to their corresponding organization names
                organizations = [
                    affiliation_map.get(aff.get("@id"), "Unknown")
                    for aff in affiliations
                    if aff.get("@id") in affiliation_map
                ]

                if not organizations:
                    self.logger.warning(f"No valid affiliations found for author '{name}'.")
                    continue  # Skip this author if no valid affiliations are found

                organizations_str = "|".join(organizations)

                # Add the author and their information to the result list
                result.append(
                    {
                        "author": name,
                        "internal_author_id": internal_author_id,
                        "orcid_id": orcid_id,
                        "organizations": organizations_str,
                    }
                )

        except Exception as e:
            self.logger.error(f"An error occurred during author extraction: {e}")

        return result

    def get_dc_type_info(self, x):
        """
        Retrieves the dc.type and dc.type_authority attributes for a given document type.

        :param data_doctype: The document type (e.g., "Article", "Proceedings Paper", etc.)
        :return: A dictionary with the keys "dc.type" and "dc.type_authority", or "unknown" if not found.
        """
        data_doctype = self._extract_first_doctype(x)
        # Access the doctype mapping for "source_wos"
        doctype_mapping = mappings.doctypes_mapping_dict.get("source_scopus", {})
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
            mapped_value = mappings.doctypes_mapping_dict["source_scopus"].get(
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

    def _extract_conference_info(self, x):
        """
        Extracts conference information from the Scopus record and formats it as:
        'conference_title::conference_location::conference_startdate::conference_enddate'.
        - Missing fields are replaced with "None".
        - If end_date is None or missing, it reuses start_date if available.
        - If multiple conferences are found, they are separated by "||".

        Parameters:
        x (dict): A Scopus record containing conference data.

        Returns:
        str: A formatted string with conference information or "None" if no data is found.
        """
        try:
            # Navigate to the conference info structure
            conference_info = (
                x.get("item", {})
                .get("bibrecord", {})
                .get("head", {})
                .get("source", {})
                .get("additional-srcinfo", {})
                .get("conferenceinfo", {})
            )
            if not conference_info:
                return ""

            # Extract conference event details
            confevent = conference_info.get("confevent", {})
            confname = confevent.get("confname", "")
            confnumber = confevent.get("confnumber", "")
            confseriestitle = confevent.get("confseriestitle", "")
            full_title = (
                f"{confnumber} {confseriestitle}".strip() if confseriestitle else confname
            )

            # Extract conference location
            conflocation = confevent.get("conflocation", {})
            city = conflocation.get("city", "")
            country_code = conflocation.get("@country", "")
            country_name = self._get_country_name_from_code(country_code)
            location = f"{city}, {country_name}".strip(", ")

            # Extract conference dates
            confdate = confevent.get("confdate", {})
            start_date = self.format_date(confdate.get("startdate"))

            # If end_date is missing or None, reuse start_date
            end_date = self.format_date(confdate.get("enddate"))
            if not end_date and start_date:
                end_date = start_date

            # Build the formatted conference info string
            conference_info_str = f"{full_title}::{location}::{start_date}::{end_date}"
            return conference_info_str

        except Exception as e:
            self.logger.error(f"Error extracting conference info: {e}")
            return ""

    def format_date(self, date_obj):
        """
        Helper function to format Scopus date objects into 'YYYY-MM-DD'.
        Handles cases where date_obj is missing or incomplete.

        Parameters:
        date_obj (dict): Date object with '@day', '@month', and '@year'.

        Returns:
        str: Formatted date string or "None" if data is missing.
        """
        try:
            if not date_obj:
                return ""  # Return None if date_obj is missing
            day = date_obj.get("@day", "")  # Default to "01" if missing
            month = date_obj.get("@month", "")
            year = date_obj.get("@year", "")
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        except Exception:
            return ""

    def _get_country_name_from_code(self, country_code):
        """
        Converts an ISO 3166-1 alpha-3 country code into its human-readable name.

        Parameters:
        country_code (str): The ISO 3166-1 alpha-3 country code.

        Returns:
        str: The country name, or the original code if the name is not found.
        """
        try:
            country = pycountry.countries.get(alpha_3=country_code.upper())
            return country.name if country else country_code
        except Exception:
            return country_code

    def _extract_funding_info(self, x):
        """
        Extracts funding information from the Scopus record and formats it as:
        'funding_agency::grant_id1,grant_id2'.
        - Missing fields are replaced with "None".
        - If multiple fundings are found, they are separated by "||".

        Parameters:
        x (dict): A Scopus record containing funding data.

        Returns:
        str: A formatted string with funding information or "None" if no data is found.
        """
        # Navigate to the funding list
        funding_data = (
            x.get("item", {})
            .get("xocs:meta", {})
            .get("xocs:funding-list", {})
            .get("xocs:funding", [])
        )

        # Handle the case where funding_data is a dictionary instead of a list
        if isinstance(funding_data, dict):
            funding_data = [funding_data]

        if not funding_data:
            return ""

        funding_infos = []
        for funding in funding_data:
            # Extract funding agency name
            agency = funding.get("xocs:funding-agency-matched-string", "")

            # Extract funding IDs (may be a string, a list, or a dict)
            grant_ids = funding.get("xocs:funding-id", [])
            if isinstance(grant_ids, str):
                grant_ids = [grant_ids]  # Wrap single string in a list
            elif isinstance(grant_ids, list):
                grant_ids = [
                    grant.get("$", "") if isinstance(grant, dict) else str(grant)
                    for grant in grant_ids
                ]
            elif isinstance(grant_ids, dict):
                grant_ids = [grant_ids.get("$", "")]  # Handle dict case

            # Combine agency and grant IDs, separating grant IDs by commas if multiple exist
            if grant_ids:
                grant_ids_str = ",".join(filter(None, grant_ids))  # Remove empty values
                funding_infos.append(f"{agency}::{grant_ids_str}")
            else:
                funding_infos.append(f"{agency}::")

        # Join all funding entries with "||"
        return "||".join(funding_infos) if funding_infos else ""


ScopusClient = Client(
    authentication_method=scopus_authentication_method,
    response_handler=JsonResponseHandler,
)

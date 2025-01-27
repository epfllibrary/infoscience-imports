from apiclient import (
    APIClient,
    endpoint,
    retry_request,
    paginated,
    HeaderAuthentication,
    JsonResponseHandler,
    exceptions,
)
import os
import tenacity
from datetime import datetime
from apiclient.retrying import retry_if_api_request_error
from typing import List, Dict
from collections import defaultdict
import ast
import traceback
from dotenv import load_dotenv
from utils import manage_logger
import mappings
from utils import normalize_title
from config import logs_dir

wos_api_base_url = "https://api.clarivate.com/api/wos"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
wos_token = os.environ.get("WOS_TOKEN")

accepted_doctypes = [key for key in mappings.doctypes_mapping_dict["source_wos"].keys()]

wos_authentication_method = HeaderAuthentication(
    token=wos_token,
    parameter="X-ApiKey",
    scheme=None,
    extra={"User-agent": "noto-epfl-workflow"},
)

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=wos_api_base_url)
class Endpoint:
    base = ""
    uniqueId = "id/{wosId}"
    queryId = "query/{queryId}"


class Client(APIClient):

    log_file_path = os.path.join(logs_dir, "wos_client.log")
    logger = manage_logger(log_file_path)

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=5&firstRecord=1

        Default args (can be orverwritten)
        databaseId is set to "WOS"

        Usage
        WosClient.search_query(usrQuery="(TS='cadmium')",count=5,firstRecord=1,optionView=SR)

        Returns
        A json object of Wos records
        """
        param_kwargs.setdefault('databaseId', "WOS")
        self.params = param_kwargs
        # return self.get(wos_api_base_url, params=self.params)
        return self.get(Endpoint.base, params=self.params)

    @retry_request
    def count_results(self, **param_kwargs)-> int:
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=1&firstRecord=1

        Default args (can be orverwritten)
        databaseId is set to "WOS"
        count (number of returned records) is set to 1
        firstRecord is set to 1
        viewfield (returned fields) is set to UID # to get the lightest json result

        Usage
        WosClient.count_results(usrQuery="(TS='cadmium')")
        WosClient.count_results(databaseId="WOS",usrQuery="(TS='cadmium')",count=1,firstRecord=1)

        Returns
        The number of records found for the request
        """
        param_kwargs.setdefault('databaseId', "WOS")
        param_kwargs.setdefault('viewField', "UID")
        param_kwargs.setdefault('count', 1)
        param_kwargs.setdefault('firstRecord', 1)
        self.params = {**param_kwargs}
        return self.search_query(**self.params)["QueryResult"]["RecordsFound"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs)->List[str]:
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=10&firstRecord=1&viewField=UID
        
        Default args (can be orverwritten)
        databaseId is set to "WOS"
        count (number of returned records) is set to 10
        firstRecord is set to 1
        viewfield (returned fields) is set to UID

        Usage 1
        WosClient.fetch_ids(usrQuery="(TS='cadmium')")
        WosClient.fetch_ids(databaseId="WOS",usrQuery="(TS='cadmium')",count=50)

        Usage 2
        epfl_query = "OG=(Ecole Polytechnique Federale de Lausanne) AND DT=article"
        total = 20
        count = 5
        ids = []
        for i in range(1, int(total), int(count)):
            ids.extend(WosClient.fetch_ids(usrQuery = epfl_query, count = count, firstRecord =i))

        Returns
        A list of WOS ids
        """
        param_kwargs.setdefault('databaseId', "WOS")
        param_kwargs.setdefault('viewField', "UID")
        param_kwargs.setdefault('count', 10)
        param_kwargs.setdefault('firstRecord', 1)
        self.params = {**param_kwargs}
        return [x["UID"] for x in self.search_query(**self.params)["Data"]["Records"]["records"]["REC"]]

    @retry_decorator
    def fetch_records(self, format="digest",**param_kwargs):
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=10&firstRecord=1&optionView=SR
        
        Default args (can be orverwritten)
        databaseId is set to "WOS"
        count (number of returned records) is set to 10
        firstRecord is set to 1
        optionView (returned fields) is set to SR for digest formats #to get minimal records

        Args
        format: digest|digest-ifs3|ifs3|wos

        Usage 1
        WosClient.fetch_records(usrQuery="(TS='cadmium')")
        WosClient.fetch_records(format="digest-ifs3",databaseId="WOS",usrQuery="(TS='cadmium')",count=50)

        Usage 2
        epfl_query = "OG=(Ecole Polytechnique Federale de Lausanne) AND DT=article"
        total = 20
        count = 5
        recs = []
        for i in range(1, int(total), int(count)):
            recs.extend(WosClient.fetch_records(usrQuery = epfl_query, count = count, firstRecord =i))

        Returns
        A list of records dict containing fields in this list according to to choosen format:  wos_id, title, DOI, doctype, pubyear, ifs3_collection, ifs3_collection_id, authors
        """
        param_kwargs.setdefault('databaseId', "WOS")
        param_kwargs.setdefault('count', 10)
        param_kwargs.setdefault('firstRecord', 1)
        self.params = {**param_kwargs}
        result = self.search_query(**self.params)
        if result["QueryResult"]["RecordsFound"] > 0:
            return self._process_fetch_records(format,**self.params)
        return None

    @retry_decorator
    def fetch_record_by_unique_id(self, wos_id, format="digest"):
        """
        Base request example
        https://api.clarivate.com/api/wos/id/WOS:001173421300001?databaseId=WOS&count=1&firstRecord=1
        https://api.clarivate.com/api/wos/id/WOS:001173421300001?databaseId=WOS

        Default WOS query args (cannot be orverwritten)
        databaseId = WOS
        count = 1
        firstRecord = 1

        Args
        format: digest|digest-ifs3|ifs3|wos

        Usage
        WosClient.fetch_record_by_unique_id("WOS:001173421300001")
        WosClient.fetch_record_by_unique_id("WOS:001173421300001", format="wos")
        WosClient.fetch_record_by_unique_id("WOS:001173421300001", format="ifs3")
        """
        self.params = {"databaseId": "WOS", "count": 1, "firstRecord": 1}
        result = self.get(Endpoint.uniqueId.format(wosId=wos_id), params=self.params)
        if result["QueryResult"]["RecordsFound"] == 1:
            return self._process_record(result["Data"]["Records"]["records"]["REC"][0], format)
        return None

    def _process_fetch_records(self, format,**param_kwargs):
        if format == "digest":
            param_kwargs.setdefault('optionView', "SR")
            self.params = param_kwargs
            return [self._extract_digest_record_info(x) for x in self.search_query(**self.params)["Data"]["Records"]["records"]["REC"]]
        elif format == "digest-ifs3":
            param_kwargs.setdefault('optionView', "SR")
            self.params = param_kwargs
            return [self._extract_ifs3_digest_record_info(x) for x in self.search_query(**self.params)["Data"]["Records"]["records"]["REC"]]
        elif format == "ifs3":
            self.params = param_kwargs
            return [self._extract_ifs3_record_info(x) for x in self.search_query(**self.params)["Data"]["Records"]["records"]["REC"]]
        elif format == "wos":
            self.params = param_kwargs
            return self.search_query(**self.params)["Data"]["Records"]["records"]["REC"]

    def _process_record(self, record, format):
        if format == "digest":
            return self._extract_digest_record_info(record)
        elif format == "digest-ifs3":
            return self._extract_ifs3_digest_record_info(record)
        elif format == "ifs3":
            return self._extract_ifs3_record_info(record)
        elif format == "wos":
            return record

    def _extract_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  wos_id, title, DOI, doctype, pubyear
        """
        record = {
            "source": "wos",
            "internal_id": x["UID"],
            "doi": self._extract_doi(x),
            "title": self._extract_title(x),
            "doctype": self._extract_first_doctype(x),
            "pubyear": self._extract_pubyear(x),
            "publisher": "",
            "journalTitle": self._extract_container_title(x),
            "seriesTitle": "",
            "bookTitle": "",
            "editors": "",
            "journalISSN": self._extract_issns(x),
            "seriesISSN": "",
            "bookISBN": "",
            "journalVolume": self._extract_volume(x),
            "seriesVolume": "",
            "bookPart": "",
            "issue": self._extract_issue(x),
            "startingPage": self._extract_starting_page(x),
            "endingPage": self._extract_ending_page(x),
            "pmid": self._extract_pmid(x),
            "artno": self._extract_artno(x),
            "corporateAuthor": self._extract_corporate_authors(x),
        }
        return record

    def _extract_ifs3_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  wos_id, title, DOI, doctype, pubyear, ifs3_collection, ifs3_collection_id
        """
        record = self._extract_digest_record_info(x)
        record["ifs3_collection"] = self._extract_ifs3_collection(x)
        record["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        # Get dc.type and dc.type_authority for the document type
        dc_type_info = self.get_dc_type_info(x)
        # Add dc.type and dc.type_authority to the record
        record["dc.type"] = dc_type_info["dc.type"]
        record["dc.type_authority"] = dc_type_info["dc.type_authority"]
        return record

    def _extract_ifs3_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  wos_id, title, DOI, doctype, pubyear, ifs3_collection, ifs3_collection_id, authors, conference_info, fundings_info
        """

        rec = self._extract_ifs3_digest_record_info(x)
        rec["abstract"] = self._extract_abstract(x)
        authors = self._extract_ifs3_authors(x)
        rec["authors"] = authors
        # Conference metadata as a single field
        rec["conference_info"] = self._extract_conference_info(x)
        rec["fundings_info"] = self._extract_funding_info(x)
        return rec

    def _extract_abstract(self, x):
        """
        Extracts the abstract from the record, but only if the 'has_abstract' flag is 'Y'.

        Parameters:
        x (dict): A record containing the data from which the abstract needs to be extracted.

        Returns:
        str: The abstract text if available, or an empty string if not found or not available.
        """
        try:
            # Check if the record has an abstract available
            has_abstract = (
                x.get("static_data", {})
                .get("summary", {})
                .get("pub_info", {})
                .get("has_abstract", "N")
            )

            if has_abstract == "Y":
                # Navigate to the abstract data
                abstract_data = (
                    x.get("static_data", {})
                    .get("fullrecord_metadata", {})
                    .get("abstracts", {})
                    .get("abstract", {})
                    .get("abstract_text", {})
                    .get("p", [])  # Paragraph(s) under 'p'
                )

                # Handle case where 'p' is a string instead of a list
                if isinstance(abstract_data, str):
                    return abstract_data.strip()

                # Ensure abstract_data is a list, then join the paragraphs
                elif isinstance(abstract_data, list):
                    abstract_text = " ".join(
                        paragraph.strip() for paragraph in abstract_data
                    )
                    return abstract_text.strip()

            # Return an empty string if no abstract is available
            return ""
        except Exception as e:
            # Log unexpected errors with context
            self.logger.error(f"Error extracting abstract: {e}\nRecord: {x}")
            return ""

    def _extract_conference_info(self, x):
        """
        Extracts information about conferences from the record and formats it as:
        'conference_title::conference_location::conference_startdate::conference_enddate'.
        - If a field is missing, it is replaced with an empty string.
        - If there are multiple conferences, they are separated by "||".
        - Returns an empty string if no valid conference title is found.

        Parameters:
        record (dict): A dictionary containing the record data from the API.

        Returns:
        str: A formatted string with conference information or an empty string if no valid conference title is found.
        """
        # Retrieve the conference data from the record
        conferences_data = (
            x.get("static_data", {})
            .get("summary", {})
            .get("conferences", {})
            .get("conference", [])
        )

        # Ensure the conference data is a list (even if there is only one conference)
        if not isinstance(conferences_data, list):
            conferences_data = [conferences_data]

        # List to store formatted conference information
        conference_infos = []

        for conference in conferences_data:
            # Extract conference title
            title = conference.get("conf_titles", {}).get("conf_title", None)

            # Skip if the title is None, empty, or only contains whitespace
            if not title or not title.strip():
                continue

            # Extract conference location
            location_data = conference.get("conf_locations", {}).get("conf_location", {})
            location = f"{location_data.get('conf_city', '')}, {location_data.get('conf_state', '')}".strip(
                ", "
            )
            location = (
                location if location else ""
            )  # Ensure location is an empty string if no data

            # Extract conference dates
            date_data = conference.get("conf_dates", {}).get("conf_date", {})
            start_date = self.format_date(date_data.get("conf_start"))
            end_date = self.format_date(date_data.get("conf_end"))

            # Format fields, replacing missing values with empty strings
            location = location or ""
            start_date = str(start_date) if start_date else ""
            end_date = str(end_date) if end_date else start_date

            # Build the formatted string for this conference
            conference_info = f"{title}::{location}::{start_date}::{end_date}"
            conference_infos.append(conference_info)

        # Join all conference entries with "||" or return an empty string if no valid titles
        return "||".join(conference_infos) if conference_infos else ""

    def _extract_funding_info(self, x):
        """
        Extracts funding information from the record and formats it as:
        'funding_agency::grant_id'.
        - If a field is missing, it will be left empty (for agency) or replaced with "None" (for grant_id).
        - If there are multiple funding entries, they are separated by "||".

        Parameters:
        record (dict): A dictionary containing the record data from the API.

        Returns:
        str: A formatted string with funding information or an empty string if no funding data is found.
        """
        # Ensure we are working with a dictionary at the correct level
        static_data = x.get("static_data", {})
        if not isinstance(static_data, dict):
            return ""  # If 'static_data' is not a dictionary, return empty string

        fullrecord_metadata = static_data.get("fullrecord_metadata", {})
        if not isinstance(fullrecord_metadata, dict):
            return ""  # If 'fullrecord_metadata' is not a dictionary, return empty string

        fund_ack = fullrecord_metadata.get("fund_ack", {})
        if not isinstance(fund_ack, dict):
            return ""  # If 'fund_ack' is not a dictionary, return empty string

        grants = fund_ack.get("grants", {})
        if not isinstance(grants, dict):
            return ""  # If 'grants' is not a dictionary, return empty string

        # Get the list of grants (it may be a list or dictionary, so ensure it's a list)
        fundings_data = grants.get("grant", [])
        if isinstance(fundings_data, dict):
            fundings_data = [fundings_data]  # Convert single grant dictionary to a list
        elif not isinstance(fundings_data, list):
            return ""  # If 'grant' is neither a dict nor a list, return empty string

        # List to store formatted funding information
        funding_infos = []

        for funding in fundings_data:
            # Extract funding agency names
            agency_names = funding.get("grant_agency_names", [])
            preferred_agency = None
            if isinstance(agency_names, list):
                # Get the preferred agency name (if available)
                preferred_agency = next(
                    (
                        agency["content"]
                        for agency in agency_names
                        if agency.get("pref") == "Y"
                    ),
                    None,
                )
            agency_name = preferred_agency or funding.get("grant_agency", "")

            # Extract grant ID(s)
            grant_ids = funding.get("grant_ids", {})
            if isinstance(grant_ids, dict):
                grant_id = grant_ids.get("grant_id", "")
            else:
                grant_id = ""

            # Handle the case where grant_id is nested or a list (if applicable)
            if isinstance(grant_id, list):
                grant_id = ";".join(grant_id)  # Combine multiple IDs into a single string

            # Ensure agency and grant ID pair is properly associated
            if agency_name or grant_id:
                funding_info = f"{agency_name}::{grant_id}"
                funding_infos.append(funding_info)

        # Join all funding entries with "||" or return an empty string if no funding info
        return "||".join(funding_infos) if funding_infos else ""

    def _extract_doi(self, x):
        identifiers = x["dynamic_data"]["cluster_related"]["identifiers"]["identifier"]
        if isinstance(identifiers, dict) and identifiers.get("type") == "doi":
            return identifiers.get("value").lower()
        elif isinstance(identifiers, list):
            return next((val["value"].lower() for val in identifiers if val["type"] == "doi"), None)
        return None

    def _extract_title(self, x):
        raw_title = next(
            (
                y["content"]
                for y in x["static_data"]["summary"]["titles"]["title"]
                if y["type"] == "item"
            ),
            None,
        )
        return normalize_title(raw_title) if raw_title else None

    def _extract_first_doctype(self, x):
        """
        Extracts the first doctype from the input dictionary.

        Parameters:
            x (dict): Input data structure containing 'static_data' -> 'summary' -> 'doctypes' -> 'doctype'.

        Returns:
            str: The first doctype as a string, or None if not found.
        """
        doctype = x["static_data"]["summary"]["doctypes"]["doctype"]

        if isinstance(doctype, dict):  # Case where 'doctype' is a single dictionary
            doctype = [doctype]
        elif not isinstance(doctype, list):  # Ensure 'doctype' is a list in all cases
            doctype = [doctype]

        # Extract the first doctype if the list is not empty
        return doctype[0] if doctype else None

    def get_dc_type_info(self, x):
        """
        Retrieves the dc.type and dc.type_authority attributes for a given document type.

        :param x: The input data (could be a string or object from which the document type is extracted)
        :return: A dictionary with the keys "dc.type" and "dc.type_authority", or "unknown" if not found.
        """
        data_doctype = self._extract_first_doctype(x)

        if isinstance(data_doctype, list):
            data_doctype = data_doctype[
                0
            ]

        # Access the doctype mapping for "source_wos"
        doctype_mapping = mappings.doctypes_mapping_dict.get("source_wos", {})

        # Check if the document type exists in the mapping
        document_info = doctype_mapping.get(data_doctype)

        if document_info is None:
            # Handle the case where the doctype is not found
            self.logger.warning(
                f"Document type '{data_doctype}' not found in doctype_mapping."
            )
            dc_type = "unknown"
        else:
            dc_type = document_info.get("dc.type", "unknown")

        # Retrieve dc.type_authority from the types_authority_mapping
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
            # Mapping document types for "source_wos"
            mapped_value = mappings.doctypes_mapping_dict["source_wos"].get(data_doctype)

            if mapped_value is not None:
                # Return the mapped collection value
                return mapped_value.get("collection", "unknown")
            else:
                # Log or handle the case where the mapping is missing
                self.logger.warning(f"Mapping not found for data_doctype: {data_doctype}")
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

    def _extract_pubyear(self, x):
        pub_info = x["static_data"]["summary"].get("pub_info", {})
        return pub_info.get("pubyear") if not isinstance(pub_info.get("pubyear"), list) else None

    def _extract_publication_date(self, x):
        pub_info = x["static_data"]["summary"].get("pub_info", {})
        return (
            pub_info.get("sortdate")
            if not isinstance(pub_info.get("sortdate"), list)
            else None
        )

    def _extract_issue_date(self, x):
        """
        Extracts the publication date (issueDate) in YYYY-MM-DD format if available.
        """
        try:
            pub_info = x.get("static_data", {}).get("summary", {}).get("pub_info", {})
            pub_date = pub_info.get("sortdate", "")  # Format is usually YYYY-MM-DD
            return pub_date if pub_date else ""
        except Exception as e:
            self.logger.error(f"Error extracting issueDate: {e}")
            return ""

    def _extract_publisher(self, x):
        """
        Extracts the 'unified_name' of all publishers (role == 'publisher') from the record.
        If multiple publishers exist, concatenates them with '; ' as the separator.
        Transforms the publisher names to title case.

        Parameters:
            x (dict): The JSON record containing publisher data.

        Returns:
            str: Concatenated unified_names of publishers in title case, or an empty string if none are found.
        """
        try:
            publisher_data = (
                x.get("static_data", {})
                .get("summary", {})
                .get("publishers", {})
                .get("publisher", {})
            )

            names_data = publisher_data.get("names", {}).get("name", None)

            if isinstance(names_data, dict):
                names_data = [names_data]

            publishers = []
            for name in names_data:
                if name.get("role") == "publisher":
                    # Check for 'unified_name' first, then fallback to 'full_name'
                    publisher_name = name.get("unified_name") or name.get("full_name")
                    if publisher_name:
                        publishers.append(publisher_name.strip().title())  # Convert to Title Case

            if not publishers:
                self.logger.debug("No publisher found.")
                return ""

            result = "; ".join(publishers)
            return result
        except Exception as e:
            self.logger.error(f"Error extracting publisher: {e}")
            return ""

    def _extract_container_title(self, x):
        """
        Extracts the journal title from the record and adjusts capitalization intelligently
        using 'abbrev_iso' as a reference to preserve acronyms and proper formatting.

        Args:
            x (dict): JSON object containing metadata, including the journal titles.

        Returns:
            str: The formatted journal title, or an empty string if not found.
        """
        try:
            # Extract the list of titles from the JSON object
            titles = (
                x.get("static_data", {})
                .get("summary", {})
                .get("titles", {})
                .get("title", [])
            )

            # Find the full title (type "source") and the ISO abbreviation (type "abbrev_iso")
            full_title = next(
                (t.get("content", "") for t in titles if t.get("type") == "source"), ""
            )
            abbrev_iso = next(
                (t.get("content", "") for t in titles if t.get("type") == "abbrev_iso"), ""
            )

            if not full_title:
                # Return an empty string if the full title is not found
                return ""

            if not abbrev_iso:
                # If no ISO abbreviation is available, apply default title case formatting
                return full_title.title()

            # Apply custom title case using the ISO abbreviation as a reference
            return self._apply_custom_title_case(full_title, abbrev_iso)

        except Exception as e:
            # Log any unexpected errors with the input data for debugging purposes
            self.logger.error(f"Error extracting container title: {e}, input: {x}")
            return ""

    def _apply_custom_title_case(self, full_title, abbrev_iso):
        """
        Adjusts the capitalization of the full title based on the ISO abbreviation.

        This ensures acronyms and proper nouns are preserved in their intended format.

        Args:
            full_title (str): The full journal title.
            abbrev_iso (str): The ISO abbreviation of the title.

        Returns:
            str: The intelligently formatted journal title.
        """
        # Split both full title and ISO abbreviation into individual words
        full_title_words = full_title.split()
        abbrev_iso_words = abbrev_iso.split()

        # Create a set of words in uppercase from the ISO abbreviation 
        uppercase_words = {word for word in abbrev_iso_words if word.isupper()}

        # Build the resulting title, preserving uppercase for acronyms and title case otherwise
        result_words = []
        for word in full_title_words:
            if word.upper() in uppercase_words:
                # Preserve words found in the ISO abbreviation as uppercase
                result_words.append(word.upper())
            else:
                # Apply title case for all other words
                result_words.append(word.capitalize())

        # Join the formatted words into a single string
        return " ".join(result_words)

    def _extract_issns(self, x):
        """
        Extracts both the ISSN and eISSN from the record.
        Combines them into a single string separated by '||'.

        Returns:
            str: A string in the format "ISSN||eISSN", or an empty string if neither is found.
        """
        try:
            identifiers = (
                x.get("dynamic_data", {})
                .get("cluster_related", {})
                .get("identifiers", {})
                .get("identifier", [])
            )
            issn = None
            eissn = None

            for identifier in identifiers:
                if identifier.get("type") == "issn":
                    issn = identifier.get("value", "")
                if identifier.get("type") == "eissn":
                    eissn = identifier.get("value", "")

            # Combine ISSN and eISSN, separated by '||'
            return "||".join(filter(None, [issn, eissn]))  # Only include non-empty values
        except Exception as e:
            self.logger.debug(f"Error extracting ISSNs: {e}")
            return ""

    def _extract_isbns(self, x):
        """
        Extracts the ISBN for books.
        """
        try:
            return x["static_data"]["summary"]["pub_info"].get("isbn", "")
        except Exception:
            return ""

    def _extract_volume(self, x):
        """
        Extracts the volume of the journal.
        """
        try:
            return x["static_data"]["summary"]["pub_info"].get("vol", "")
        except Exception:
            return ""

    def _extract_issue(self, x):
        """
        Extracts the issue number of the publication.
        """
        try:
            return x["static_data"]["summary"]["pub_info"].get("issue", "")
        except Exception:
            return ""

    def _extract_starting_page(self, x):
        """
        Extracts the starting page of the publication.
        """
        try:
            return x["static_data"]["summary"]["pub_info"].get("page", {}).get("begin", "")
        except Exception:
            return ""

    def _extract_ending_page(self, x):
        """
        Extracts the ending page of the publication.
        """
        try:
            return x["static_data"]["summary"]["pub_info"].get("page", {}).get("end", "")
        except Exception:
            return ""

    def _extract_pmid(self, x):
        """
        Extracts the PubMed ID (PMID) from the record and removes the prefix 'MEDLINE:' if present.

        Returns:
            str: The PMID as a string without the 'MEDLINE:' prefix, or an empty string if not found.
        """
        try:
            identifiers = (
                x.get("dynamic_data", {})
                .get("cluster_related", {})
                .get("identifiers", {})
                .get("identifier", [])
            )

            # If identifiers is a dict instead of a list, convert it to a list
            if isinstance(identifiers, dict):
                identifiers = [identifiers]

            # Look for the identifier with type "pmid"
            for identifier in identifiers:
                if identifier.get("type") == "pmid":
                    pmid = identifier.get("value", "")
                    # Remove the 'MEDLINE:' prefix if it exists
                    return pmid.replace("MEDLINE:", "").strip()

            return ""
        except Exception as e:
            self.logger.error(f"Error extracting PMID: {e}")
            return ""

    def _extract_artno(self, x):
        """
        Extracts the PubMed ID (PMID) from the record and removes the prefix 'MEDLINE:' if present.

        Returns:
            str: The PMID as a string without the 'MEDLINE:' prefix, or an empty string if not found.
        """
        try:
            identifiers = (
                x.get("dynamic_data", {})
                .get("cluster_related", {})
                .get("identifiers", {})
                .get("identifier", [])
            )

            # If identifiers is a dict instead of a list, convert it to a list
            if isinstance(identifiers, dict):
                identifiers = [identifiers]

            # Look for the identifier with type "art_no"
            for identifier in identifiers:
                if identifier.get("type") == "art_no":
                    artno = identifier.get("value", "")
                    # Remove the prefix if it exists
                    return artno.replace("ARTN ", "").strip()

            return ""
        except Exception as e:
            self.logger.error(f"Error extracting Article Number: {e}")
            return ""

    def _extract_ifs3_authors(self, x):
        """
        Extracts authors and reconciles their affiliations using 'addr_no' to link
        authors (from 'summary > names') with affiliations (from 'fullrecord_metadata > addresses').

        Parameters:
            record (dict): The JSON record containing author data.

        Returns:
            List[dict]: A sorted list of dictionaries containing author details, including name,
            organizations, and affiliations.
        """
        authors = []

        try:
            # Extract authors from static_data > summary > names
            names = (
                x.get("static_data", {})
                .get("summary", {})
                .get("names", {})
                .get("name", [])
            )
            if isinstance(names, dict):
                names = [names]

            # Extract affiliations from static_data > fullrecord_metadata > addresses
            address_entries = (
                x.get("static_data", {})
                .get("fullrecord_metadata", {})
                .get("addresses", {})
                .get("address_name", [])
            )
            if isinstance(address_entries, dict):
                address_entries = [address_entries]

            # Map addr_no to affiliations for easy lookup
            addr_to_affiliation = {}
            for address_entry in address_entries:
                addr_no = address_entry.get("address_spec", {}).get("addr_no")
                organizations = (
                    address_entry.get("address_spec", {})
                    .get("organizations", {})
                    .get("organization", [])
                )
                suborganization = (
                    address_entry.get("address_spec", {})
                    .get("suborganizations", {})
                    .get("suborganization")
                )

                if isinstance(organizations, dict):
                    organizations = [organizations]
                if isinstance(suborganization, list):
                    suborganization = "|".join(suborganization)

                organizations_str = "|".join(
                    [org.get("content", "") for org in organizations]
                )

                addr_to_affiliation[addr_no] = {
                    "organizations": organizations_str,
                    "suborganization": suborganization,
                }

            # Process each author and link affiliations using addr_no
            for author in names:
                try:
                    if author.get("role") != "author":
                        continue
                    # Extract author details
                    preferred_name = author.get("preferred_name", {}).get("full_name")
                    full_name = author.get("full_name")
                    display_name = author.get("display_name")
                    author_name = preferred_name or full_name or display_name

                    if not author_name:
                        self.logger.warning(f"Skipping author due to missing name.")
                        continue

                    # Extract seq_no, addr_no, and other details
                    seq_no = int(author.get("seq_no", 0))  # Ensure seq_no is an integer
                    addr_no = author.get("addr_no")
                    affiliation = addr_to_affiliation.get(
                        addr_no, {"organizations": None, "suborganization": None}
                    )

                    # Build author info
                    author_info = {
                        "seq_no": seq_no,
                        "author": author_name,
                        "internal_author_id": self._get_internal_author_id(
                            author.get("data-item-ids", {}).get("data-item-id", None)
                        ),
                        "orcid_id": author.get("orcid_id"),
                        "organizations": affiliation["organizations"],
                        "suborganization": affiliation["suborganization"],
                        "role": author.get("role"),
                    }

                    authors.append(author_info)

                except KeyError as e:
                    self.logger.error(f"Missing key for author: {e}")
                    continue

            # Sort authors by seq_no
            authors = sorted(authors, key=lambda x: x["seq_no"])

        except Exception as e:
            error_message = traceback.format_exc()
            self.logger.error(f"Error processing record: {error_message}")

        return authors

    def _get_internal_author_id(self, data_item_id):
        """
        Extracts the internal author ID from the data-item-id field.
        Handles both dictionary and list cases.
        """
        if isinstance(data_item_id, list):
            # Look for the dictionary with id-type "PreferredRID"
            for item in data_item_id:
                if isinstance(item, dict) and item.get("id-type") == "PreferredRID":
                    return item.get("content")
        elif isinstance(data_item_id, dict):
            # If it's a dictionary, return the content directly
            return data_item_id.get("content")
        return None  # Return None if no valid ID is found

    def format_date(self, date_str):
        try:
            return datetime.strptime(str(date_str), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return "None"

    def _extract_corporate_authors(self, x):
        """
        Extracts corporate authors (role == 'corp') and concatenates their display names.

        Parameters:
            record (dict): The JSON record containing corporate author data.

        Returns:
            str: A concatenated string of corporate author display names, separated by '||'.
        """
        try:
            # Extract names from static_data > summary > names
            names = (
                x.get("static_data", {})
                .get("summary", {})
                .get("names", {})
                .get("name", [])
            )
            if isinstance(names, dict):
                names = [names]

            # Filter for corporate authors and concatenate display_name values
            corp_display_names = [
                author.get("display_name", "")
                for author in names
                if author.get("role") == "corp"
            ]

            # Concatenate names with '||'
            return "||".join(filter(None, corp_display_names))

        except Exception as e:
            error_message = traceback.format_exc()
            self.logger.error(f"Error extracting corporate authors: {error_message}")
            return ""


WosClient = Client(
    authentication_method=wos_authentication_method,
    response_handler=JsonResponseHandler,
)

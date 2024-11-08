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
from dotenv import load_dotenv
from utils import manage_logger
import mappings
from config import logs_dir

scopus_api_base_url = "https://api.elsevier.com/content"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
scopus_api_key = os.environ.get("SCOPUS_API_KEY")

accepted_doctypes = [key for key in mappings.doctypes_mapping_dict["source_scopus"].keys()]

scopus_authentication_method = HeaderAuthentication(
    token=scopus_api_key,
    parameter="X-ELS-APIKey",
    scheme=None,
    #extra={"User-agent": "noto-epfl-workflow"},
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
    doi ="doi/{doi}" 


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
        param_kwargs.setdefault('start', 1)
        param_kwargs.setdefault('field', "dc:identifier") #to get minimal records
        self.params = {**param_kwargs}
        return self.search_query(**self.params)["search-results"]["opensearch:totalResults"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs)->List[str]:
        """
        Base request example
        https://api.elsevier.com/content/search/scopus?query=all(gene)&count=100&start=1&field=dc:identifier
        
        Default args (can be orverwritten)
        count (number of returned records) is set to 10
        start is set to 1
        field is set to dc:identifier to get a minimal records

        Usage 1
        ScopusClient.fetch_ids(query="AF-ID(60028186)")
        ScopusClient.fetch_ids(query="AF-ID(60028186)",count=50)

        Usage 2
        epfl_query = "AF-ID(60028186) OR AF-ID(60210159) OR AF-ID(60070536) OR AF-ID(60204330) OR AF-ID(60070531) OR AF-ID(60070534) OR AF-ID(60070538) OR AF-ID(60014951) OR AF-ID(60070529) OR AF-ID(60070532) OR AF-ID(60070535) OR AF-ID(60122563) OR AF-ID(60210160) OR AF-ID(60204331)"
        count = 5
        ids = []
        for i in range(1, int(total), int(count)):
            ids.extend(ScopusClient.fetch_ids(query = epfl_query, count = count, start =i))

        Returns
        A list of SCOPUS ids
        """

        param_kwargs.setdefault('count', 10)
        param_kwargs.setdefault('start', 1)
        param_kwargs.setdefault('field', "dc:identifier") #to get minimal records
        self.params = {**param_kwargs}
        return [x["dc:identifier"] for x in self.search_query(**self.params)["search-results"]["entry"]]

    @retry_decorator
    def fetch_records(self, format="digest",**param_kwargs):
        """
        Base request example
        https://api.elsevier.com/content/search/scopus?query=AF-ID(60028186)&count=10&start=1
        
        Default args (can be orverwritten)
        count (number of returned records) is set to 10
        start is set to 1

        Args
        format: digest|digest-ifs3ifs3|wos

        Usage 1
        ScopusClient.fetch_records(query="AF-ID(60028186)")
        ScopusClient.fetch_records(format="digest-ifs3",query="AF-ID(60028186)",count=50)

        Usage 2
        epfl_query = "AF-ID(60028186)"
        total = 20
        count = 5
        recs = []
        for i in range(1, int(total), int(count)):
            recs.extend(ScopusClient.fetch_records(query = epfl_query, count = count, start =i))

        Returns
        A list of records dict containing fields in this list according to to choosen format:  scopus_id, title, DOI, doctype, pubyear, ifs3_doctype, ifs3_collection_id, authors
        """
        param_kwargs.setdefault('count', 10)
        param_kwargs.setdefault('start', 1)

        self.params = {**param_kwargs}
        result = self.search_query(**self.params)
        if int(result["search-results"]["opensearch:totalResults"]) > 0:
            return self._process_fetch_records(format, **self.params)
        return None

    @retry_decorator
    def fetch_record_by_unique_id(self, scopus_id, format="digest"):
        """
        Base request example
        https://api.elsevier.com/content/abstract/scopus_id/SCOPUS_ID:85145343484

        Args
        format: digest|digest-ifs3|ifs3|scopus

        Usage
        ScopusClient.fetch_record_by_unique_id("SCOPUS_ID:85200150104")
        ScopusClient.fetch_record_by_unique_id("SCOPUS_ID:85200150104", format="scopus")
        ScopusClient.fetch_record_by_unique_id("SCOPUS_ID:85200150104", format="ifs3")
        """
        # posiibilité de passer des params comme pour search : field=prism:doi,dc:title,author,affiliation
        result = self.get(Endpoint.scopusId.format(scopusId=scopus_id))
        if result["search-results"]["opensearch:totalResults"] == 1:
            return self._process_record(result["search-results"]["entry"][0], format)
        return None

    def _process_fetch_records(self, format,**param_kwargs):
        if format == "digest":
            param_kwargs.setdefault('field', "eid,dc:identifier,prism:doi,dc:title,subtypeDescription,prism:coverDate")
            self.params = param_kwargs
            return [self._extract_digest_record_info(x) for x in self.search_query(**self.params)["search-results"]["entry"]]
        elif format == "digest-ifs3":
            param_kwargs.setdefault('field', "eid,dc:identifier,prism:doi,dc:title,subtypeDescription,prism:coverDate")
            self.params = param_kwargs
            return [self._extract_ifs3_digest_record_info(x) for x in self.search_query(**self.params)["search-results"]["entry"]]
        elif format == "ifs3":
            param_kwargs.setdefault('field', "eid,dc:identifier,prism:doi,dc:title,subtypeDescription,prism:coverDate,author,affiliation")
            self.params = param_kwargs
            return [self._extract_ifs3_record_info(x) for x in self.search_query(**self.params)["search-results"]["entry"]]
        elif format == "scopus":
            self.params = param_kwargs
            return self.search_query(**self.params)["search-results"]["entry"]

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
        Returns
        A list of records dict containing the fields :  scopus_id, title, DOI, doctype, pubyear
        """
        record = {
            "source": "scopus",
            "internal_id": x["eid"],
            "doi": self._extract_doi(x),
            "title": self._extract_title(x),
            "doctype": self._extract_first_doctype(x),
            "pubyear": self._extract_pubyear(x),
        }
        return record

    def _extract_ifs3_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  scopus_id, title, DOI, doctype, pubyear, ifs3_doctype, ifs3_collection_id
        """
        record = self._extract_digest_record_info(x)
        record["ifs3_doctype"] = self._extract_ifs3_doctype(x)
        record["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        return record

    def _extract_ifs3_record_info(self, record):
        """
        Returns
        A list of records dict containing the fields :  scopus_id, title, DOI, doctype, pubyear, ifs3_doctype, ifs3_collection_id, authors
        """
        rec = self._extract_ifs3_digest_record_info(record)
        authors = self._extract_ifs3_authors(record)
        rec["authors"] = authors
        return rec

    def _extract_doi(self, x):
        if "prism:doi" in x:
            return x["prism:doi"].lower()
        return None

    def _extract_title(self, x):
        return x.get("dc:title", None)

    def _extract_first_doctype(self, x):
        if isinstance(x.get("subtypeDescription"), list):
            return x["subtypeDescription"][0] if x["subtypeDescription"] else None
        return x.get("subtypeDescription", None)

    def _extract_ifs3_doctype(self, x):
        data_doctype = self._extract_first_doctype(x)
        # Check if data_doctype is in accepted_doctypes
        if data_doctype in accepted_doctypes:
            mapped_value = mappings.doctypes_mapping_dict["source_scopus"].get(data_doctype)
            if mapped_value is not None:
                return mapped_value
            else:
                # Log or handle the case where mapping is missing
                self.logger.warning(f"Mapping not found for data_doctype: {data_doctype}")
                return "unknown_doctype"  # or any other default value
        return "unknown_doctype"  # or any other default value

    def _extract_ifs3_collection_id(self, x):
        ifs3_doctype = self._extract_ifs3_doctype(x)
        if ifs3_doctype != "unknown_doctype":  # Check against the default value
            return mappings.collections_mapping.get(ifs3_doctype, "unknown_collection")  # Default for missing collection
        return "unknown_collection"  # or any other default value

    def _extract_pubyear(self, x):
        return x.get("prism:coverDate", None)[:4]

    def _extract_ifs3_authors(self, x):
        # Initialize result list
        result = []

        try:
            # Ensure the input is a dictionary
            if not isinstance(x, dict):
                self.logger.debug(x)
                self.logger.warning("Input data must be a dictionary.")
                return result  # Return an empty result

            # Ensure required keys are present in the input
            if "affiliation" not in x or "author" not in x:
                self.logger.debug(x)
                self.logger.warning(
                    "Input data must contain 'affiliation' and 'author' keys."
                )
                return result  # Return an empty result

            # Create a dictionary to map afid to their corresponding organization name and affiliation details
            affiliation_map = {
                affiliation.get(
                    "afid"
                ): f"{affiliation.get('afid')}:{affiliation.get('affilname')}"
                for affiliation in x.get("affiliation", [])
                if affiliation.get("afid") and affiliation.get("affilname")
            }

            # Process authors
            for author in x.get("author", []):
                # Check if required keys are present in the author
                if not isinstance(author.get("afid"), list):
                    self.logger.warning("Each 'author' item must contain 'afid' as a list.")
                    continue  # Skip this author and continue

                # Determine author name using surname and given-name if both exist, otherwise fallback to authname
                surname = author.get("surname")
                given_name = author.get("given-name")
                if surname and given_name:
                    author_name = f"{surname}, {given_name}".strip()
                else:
                    author_name = author.get("authname", "")

                if not author_name:
                    self.logger.warning(
                        "Author name could not be determined; skipping author."
                    )
                    continue

                # Extract additional author details
                orcid_id = author.get("orcid")
                internal_author_id = author.get("authid")

                # Safely map affiliations to organizations using 'afid'
                affiliations = [
                    affiliation_map.get(af.get("$"))
                    for af in author["afid"]
                    if affiliation_map.get(af.get("$")) is not None
                ]

                if not affiliations:
                    self.logger.warning(
                        f"No valid affiliations found for author '{author_name}' with provided 'afid'."
                    )
                    continue  # Skip this author if no valid affiliations are found

                # Combine organizations
                organizations = "|".join(affiliations)

                # Add to result list
                result.append(
                    {
                        "author": author_name,
                        "internal_author_id": internal_author_id,
                        "orcid_id": orcid_id,
                        "organizations": organizations,
                    }
                )

        except Exception as e:
            self.logger.error(f"An error occurred during processing: {e}")

        return result


ScopusClient = Client(
    authentication_method=scopus_authentication_method,
    response_handler=JsonResponseHandler,
)

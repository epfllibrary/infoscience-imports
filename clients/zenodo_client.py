"""Zenodo client for Infoscience imports"""

import os
from typing import List
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
        ZenodoClient.search_query(query="gene",count=5,start=1)

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
        param_kwargs.setdefault("size", 1)
        param_kwargs.setdefault("page", 1)

        self.params = {**param_kwargs}
        return self.search_query(**self.params)["hits"]["total"]

    @retry_decorator
    def fetch_ids(self, **param_kwargs) -> List[str]:
        """
        Base request example
        https://zenodo.org/api/?q=epfl&size=100&page=1

        Default args (can be orverwritten)
        size (number of returned records) is set to 10
        page is set to 1

        Usage 1
        ZenodoClient.fetch_ids(q="epfl")
        ZenodoClient.fetch_ids(q="epfl", size=50)

        Usage 2
        naive_epfl_query = "epfl OR \"Ecole Polytechnique Fédérale de Lausanne\""
        total = ZenodoClient.count_results(q=naive_epfl_query, size=1, page=1)
        count = 100
        ids = []
        for i in range(1, int(total), int(count)):
            ids.extend(ZenodoClient.fetch_ids(q=naive_epfl_query,
                                            size=count,
                                            page=i//count+1))

        Returns
        A list of Zenodo ids
        """

        param_kwargs.setdefault("size", 10)
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
        format: digest|digest-ifs3ifs3|wos

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
        param_kwargs.setdefault("size", 10)
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
        return record

    def _extract_ifs3_record_info(self, record):
        """
        Returns
        A list of records dict containing the fields :
            zenodo_id, title, DOI, doctype, pubyear, authors
            ifs3_collection, ifs3_collection_id, license
        """
        rec = self._extract_ifs3_digest_record_info(record)
        authors = self._extract_ifs3_authors(record)
        rec["authors"] = authors
        rec["license"] = self._extract_ifs3_license(record)
        return rec

    def _extract_first_doctype(self, x):
        if "metadata" in x and "resource_type" in x["metadata"]:
            doctype = x["metadata"]["resource_type"].get(
                "type", "unknown"
            ) 
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
        # Initialize result list
        result = []

        try:
            # Ensure the input is a dictionary
            if not isinstance(x, dict):
                self.logger.error(f"Input data must be a dictionary.")
                return result  # Return an empty result

            # Ensure required keys are present in the input
            if "creators" not in x["metadata"]:
                self.logger.error(f"Input data must contain a 'creators' key.")
                return result  # Return an empty result

            # Process authors
            for author in x["metadata"]["creators"]:
                try:
                    # Check if required keys are present in the author
                    if "name" not in author:
                        self.logger.error(
                            "Each 'author' item must contain a 'name' key."
                        )
                        continue  # Skip this author and continue

                    # Extract author details
                    author_name = author.get("name", None)
                    orcid_id = author.get("orcid", None)
                    affiliation = author.get("affiliation", None)

                    # Add to result list
                    result.append(
                        {
                            "author": author_name,
                            "internal_author_id": None,
                            "orcid_id": orcid_id,
                            "organizations": affiliation,
                        }
                    )

                except KeyError as e:
                    self.logger.error(f"Skipping author due to missing key: {e}")
                    continue  # Skip this author and continue

        except Exception as e:
            self.logger.error(f"An error occurred during processing: {e}")

        return result


ZenodoClient = Client(
    authentication_method=zenodo_authentication_method,
    response_handler=JsonResponseHandler,
)

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
        Returns
        A list of records dict containing the fields :  scopus_id, title, DOI, doctype, pubyear
        """
        coredata = x.get("coredata", {})

        return {
            "source": "scopus",
            "internal_id": coredata.get("eid"),
            "doi": coredata.get("prism:doi", "").lower(),
            "title": coredata.get("dc:title"),
            "doctype": self._extract_first_doctype(x),
            "pubyear": coredata.get("prism:coverDate", "")[:4],
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
        authors = self._extract_ifs3_authors(x)
        rec["authors"] = authors

        return rec

    def _extract_first_doctype(self, x):
        subtype = x.get("coredata", {}).get("subtypeDescription")
        if isinstance(subtype, list):
            return subtype[0] if subtype else None
        return subtype

    def extract_orcids_from_bibrecord(self, bibrecord_data):
        """
        Extracts ORCID based on @auid from the provided bibrecord data.
        """
        orcid_map = {}

        try:
            # Parcours des groupes d'auteurs dans le bibrecord
            author_groups = bibrecord_data.get("head", {}).get("author-group", [])

            for author_group in author_groups:
                # Liste des auteurs dans chaque groupe d'auteurs
                authors = author_group.get("author", [])

                # Parcours de chaque auteur dans le groupe
                for author in authors:
                    auid = author.get("@auid")
                    orcid = author.get("@orcid")

                    if auid and orcid:
                        # Associer l'@auid à l'@orcid
                        orcid_map[auid] = orcid
                    elif auid:
                        self.logger.warning(f"Missing ORCID for author with @auid: {auid}")
                    else:
                        self.logger.warning("Missing @auid for an author; skipping.")

        except Exception as e:
            self.logger.error(f"An error occurred while extracting ORCID: {e}")

        return orcid_map

    def _extract_ifs3_authors(self, entry):
        """
        Extracts author information and their affiliations from the provided JSON data.
        """
        result = []

        try:

            bibrecord_data = entry.get("bibrecord", {})
            orcid_map = self.extract_orcids_from_bibrecord(bibrecord_data)

            # Check if the necessary keys are present
            authors_data = entry.get("authors", {}).get("author", [])
            affiliations_data = entry.get("affiliation", [])
            if not authors_data or not affiliations_data:
                self.logger.warning("No authors or affiliations data found.")
                return result

            # Create a map of affiliations with their ID as the key
            affiliation_map = {
                aff.get("@id"): aff.get("affilname", "Unknown")
                for aff in affiliations_data
                if aff.get("@id")
            }

            # Process each author
            for author in authors_data:
                surname = author.get("ce:surname", "")
                given_name = author.get("ce:given-name", "")
                name = f"{surname}, {given_name}".strip(", ")
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


ScopusClient = Client(
    authentication_method=scopus_authentication_method,
    response_handler=JsonResponseHandler,
)

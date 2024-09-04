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
import traceback
from dotenv import load_dotenv
import mappings

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
        format: digest|digest-ifs3ifs3|wos

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
        A list of records dict containing fields in this list according to to choosen format:  wos_id, title, DOI, doctype, pubyear, ifs3_doctype, ifs3_collection_id, authors
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
            "pubyear": self._extract_pubyear(x)
        }
        return record

    def _extract_ifs3_digest_record_info(self, x):
        """
        Returns
        A list of records dict containing the fields :  wos_id, title, DOI, doctype, pubyear, ifs3_doctype, ifs3_collection_id
        """
        record = self._extract_digest_record_info(x)
        record["ifs3_doctype"] = self._extract_ifs3_doctype(x)
        record["ifs3_collection_id"] = self._extract_ifs3_collection_id(x)
        return record

    def _extract_ifs3_record_info(self, record):
        """
        Returns
        A list of records dict containing the fields :  wos_id, title, DOI, doctype, pubyear, ifs3_doctype, ifs3_collection_id, authors
        """
        rec = self._extract_ifs3_digest_record_info(record)
        authors = self._extract_ifs3_authors(record)
        rec["authors"] = authors
        return rec

    def _extract_doi(self, x):
        identifiers = x["dynamic_data"]["cluster_related"]["identifiers"]["identifier"]
        if isinstance(identifiers, dict) and identifiers.get("type") == "doi":
            return identifiers.get("value").lower()
        elif isinstance(identifiers, list):
            return next((val["value"].lower() for val in identifiers if val["type"] == "doi"), None)
        return None

    def _extract_title(self, x):
        return next(
            (y["content"] for y in x["static_data"]["summary"]["titles"]["title"] if y["type"] == "item"),
            None
        )

    def _extract_first_doctype(self, x):
        doctype = x["static_data"]["summary"]["doctypes"]["doctype"]
        if isinstance(doctype, dict):  # Case where 'doctype' is a single dictionary
            doctype = [doctype]
        return doctype

    def _extract_ifs3_doctype(self, x):
        data_doctype = self._extract_first_doctype(x)
        if data_doctype in accepted_doctypes:
            mapped_value = mappings.doctypes_mapping_dict["source_wos"].get(data_doctype)
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
        pub_info = x["static_data"]["summary"].get("pub_info", {})
        return pub_info.get("pubyear") if not isinstance(pub_info.get("pubyear"), list) else None

    
    def _extract_ifs3_authors(self, x):
        # Initialize the authors list
        authors = []

        try:
            # Safely navigate to the addresses section
            #addresses = x.get("static_data", {}).get("fullrecord_metadata", {}).get("addresses", {}).get("address_name", [])
            addresses = x["static_data"]["fullrecord_metadata"]["addresses"]["address_name"]

            # Handle the case where a single address is given as a dictionary instead of a list
            if isinstance(addresses, dict):
                addresses = [addresses]

            # Iterate over each address entry
            for address_entry in addresses:
                try:
                    # Safely extract organizations and suborganizations
                    organization_list = address_entry.get("address_spec", {}).get("organizations", {}).get("organization", [])
                    suborganization = address_entry.get("address_spec", {}).get("suborganizations", {}).get("suborganization", None)

                    # Handle cases where organizations might be a single dictionary instead of a list
                    if isinstance(organization_list, dict):
                        organization_list = [organization_list]

                    # Combine organization names into a single string separated by '|'
                    organizations_str = '|'.join([org.get("content", "") for org in organization_list])

                    # Safely extract the names section
                    names = address_entry.get("names", {}).get("name", [])

                    # Handle the case where a single name is given as a dictionary instead of a list
                    if isinstance(names, dict):
                        names = [names]

                    # Iterate over each author in the names list
                    for author in names:
                        try:
                            # Extract author information
                            author_info = {
                                "author": author.get("display_name", None),
                                #"internal_author_id": author.get("data-item-ids", {}).get("data-item-id", {}).get("content", None),
                                "internal_author_id": self._get_internal_author_id(author.get("data-item-ids", {}).get("data-item-id", None)),
                                "orcid_id": author.get("orcid_id", None),
                                "organizations": organizations_str
                            }

                            # Add suborganization if it exists
                            if suborganization:
                                author_info["suborganization"] = suborganization

                            # Append the author information to the authors list
                            authors.append(author_info)

                        except KeyError as e:
                            # Handle missing keys for the author entry and continue
                            print(f"Skipping author due to missing key: {e}")
                            continue

                except KeyError as e:
                    # Handle missing keys for the address entry and continue
                    print(f"Skipping address entry due to missing key: {e}")
                    continue

        except Exception as e:
            # Handle any unexpected errors
            error_message = traceback.format_exc()  # Get the formatted traceback
            print(f"{x.get('UID', 'Unknown UID')} : An error occurred during processing: {error_message}")

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


WosClient = Client(
    authentication_method=wos_authentication_method,
    response_handler=JsonResponseHandler,
)

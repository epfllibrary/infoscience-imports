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
from collections import defaultdict
import ast
import os
from dotenv import load_dotenv

wos_api_base_url = "https://api.clarivate.com/api/wos"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
wos_token = os.environ.get("WOS_TOKEN")

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
    def count_results(self, **param_kwargs):
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=5&firstRecord=1

        Usage
        Client.count_results(databaseId="WOS",usrQuery="(TS='cadmium')")
        Returns the number of records found for the request
        """
        self.params = param_kwargs
        self.params["count"] = 1
        self.params["firstRecord"] = 1

        return self.search_query(**self.params)["QueryResult"]["RecordsFound"]

    @retry_decorator
    def get_wos_ids(self, **param_kwargs):
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=5&firstRecord=1

        Usage ex 1
        Client.get_wos_ids(databaseId="WOS",usrQuery="(TS='cadmium')",count=5,firstRecord=1)
        Returns a list of WOS IDs

        Usage ex 2
        count = 5
        ids = []
        for i in range(1, 20, int(count)):
            ids.extend(WosClient.get_wos_ids(databaseId = databaseId, usrQuery = epfl_query_lite, count = count, firstRecord =i))
        Returns a list of WOS IDs
        """
        self.params = param_kwargs
        return [
            x["UID"]
            for x in self.search_query(**self.params)["Data"]["Records"]["records"][
                "REC"
            ]
        ]

    @retry_decorator
    def get_wos_digest(self, **param_kwargs):
        """
        The `get_wos_digest` function retrieves a list of WOS IDs, along with their associated DOIs,
        titles, and document types, based on the specified search parameters.
        :return: The function `get_wos_digest` returns a list of dictionaries. Each dictionary contains
        information about a WOS (Web of Science) record, including the WOS ID, DOI (if available),
        title, and document type.

        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=5&firstRecord=1

        Usage ex 1
        Client.get_wos_digest(databaseId="WOS",usrQuery="(TS='cadmium')",count=5,firstRecord=1)
        """
        self.params = param_kwargs
        result = []
        for x in self.search_query(**self.params)["Data"]["Records"]["records"]["REC"]:
            record = {}
            record["wos_id"] = x["UID"]
            if (
                type(x["dynamic_data"]["cluster_related"]["identifiers"]["identifier"])
                is dict
            ):
                if (
                    x["dynamic_data"]["cluster_related"]["identifiers"]["identifier"][
                        "type"
                    ]
                    == "doi"
                ):
                    record["doi"] = x["value"]
            else:
                for val in x["dynamic_data"]["cluster_related"]["identifiers"][
                    "identifier"
                ]:
                    if val["type"] == "doi":
                        record["doi"] = val["value"]
            record["title"] = [
                y["content"]
                for y in x["static_data"]["summary"]["titles"]["title"]
                if y["type"] == "item"
            ][0]
            data_doctypes = _get_all_occurrences_by_path(
                x, "static_data.summary.doctypes.doctype"
            )
            if type(data_doctypes) == list:
                record["doctype"] = data_doctypes[0]
            else:
                record["doctype"] = data_doctypes
            result.append(record)

            # Extract publication year
            pub_info = x["static_data"]["summary"].get("pub_info", {})
            pubyear = pub_info.get("pubyear")
            if pubyear and not isinstance(pubyear, list):
                record["pubyear"] = pubyear

        return result

    @retry_request
    def search_query(self, **param_kwargs):
        """
        Base request example
        https://api.clarivate.com/api/wos?databaseId=WOS&usrQuery=(TS='cadmium')&count=5&firstRecord=1

        Usage
        Client.search_query(databaseId="WOS",usrQuery="(TS='cadmium')",count=5,firstRecord=1)
        Returns a json object of Wos records
        """
        self.params = param_kwargs
        # return self.get(wos_api_base_url, params=self.params)
        return self.get(Endpoint.base, params=self.params)

    @retry_decorator
    def query_unique_id(self, wos_id, infoscience_format=False):
        """
        Base request example
        https://api.clarivate.com/api/wos/id/WOS:001103466700001?databaseId=WOS&count=1&firstRecord=1

        Usage
        Client.query_by_id("WOS:001103466700001")
        Returns the Json object of the WOS resource

        Client.query_by_id("WOS:001103466700001", infoscience_format=True)
        Returns the Json object processed for Infoscience ingestion only if the record is not yet in Infoscience (dedup step)
        """
        self.params = {"databaseId": "WOS", "count": 1, "firstRecord": 1}
        # return self.get(wos_api_base_url, params=self.params)
        result = self.get(Endpoint.uniqueId.format(wosId=wos_id), params=self.params)
        # print(result)
        if result["QueryResult"]["RecordsFound"] == 1:
            record = self.get(
                Endpoint.uniqueId.format(wosId=wos_id), params=self.params
            )["Data"]["Records"]["records"]["REC"][0]
            if infoscience_format == True:
                # identifiers ########################################
                identifiers = {}
                data_identifiers = _get_all_occurrences_by_path(
                    record, "dynamic_data.cluster_related.identifiers.identifier"
                )
                if len(data_identifiers) > 0:
                    for i in data_identifiers:
                        identifiers[i["type"]] = i["value"]
                # doctypes #########################################
                data_doctypes = _get_all_occurrences_by_path(
                    record, "static_data.summary.doctypes.doctype"
                )
                if type(data_doctypes) == list:
                    doctypes = "|".join(
                        [x for x in data_doctypes]
                    )  # returns sub headings strings pipe-separated
                else:
                    doctypes = data_doctypes
                # publication infos #############################################
                pub_info = record["static_data"]["summary"]["pub_info"].copy()
                # publisher infos ############################################
                data_publishers = _get_all_occurrences_by_path(
                    record, "static_data.summary.publishers.publisher"
                )
                if len(data_publishers) > 0:
                    publisher_full_name = "|".join(
                        [
                            x["full_name"]
                            for x in _get_all_occurrences_by_path(
                                record,
                                "static_data.summary.publishers.publisher.names.name",
                            )
                        ]
                    )
                    # publisher_unified_name = "|".join([x["unified_name"] for x in _get_all_occurrences_by_path(record, "static_data.summary.publishers.publisher.names.name")])
                    publisher_city = "|".join(
                        [
                            x["city"]
                            for x in _get_all_occurrences_by_path(
                                record,
                                "static_data.summary.publishers.publisher.address_spec",
                            )
                        ]
                    )
                # titles #############################################
                titles = {}
                data_titles = _get_all_occurrences_by_path(
                    record, "static_data.summary.titles.title"
                )
                for i in data_titles:
                    titles[i["type"]] = i["content"]
                # abstract ############################################
                data_abstracts = _get_all_occurrences_by_path(
                    record,
                    "static_data.fullrecord_metadata.abstracts.abstract.abstract_text.p",
                )
                if type(data_abstracts) == list:
                    abstract = "|".join(
                        [x for x in data_abstracts]
                    )  # returns sub headings strings pipe-separated
                else:
                    abstract = data_abstracts
                # author keywords processing #############################################
                auth_data_keywords = _get_all_occurrences_by_path(
                    record, "static_data.fullrecord_metadata.keywords.keyword"
                )
                if type(auth_data_keywords) == list:
                    auth_keywords = "|".join(
                        [x for x in auth_data_keywords]
                    )  # returns sub headings strings pipe-separated
                else:
                    auth_keywords = auth_data_keywords
                # keywords processing #############################################
                data_keywords = _get_all_occurrences_by_path(
                    record, "static_data.item.keywords_plus.keyword"
                )
                if type(data_keywords) == list:
                    keywords = "|".join(
                        [x for x in data_keywords]
                    )  # returns sub headings strings pipe-separated
                else:
                    keywords = data_keywords
                # subjects headings processing ####################################
                data_subheadings = _get_all_occurrences_by_path(
                    record,
                    "static_data.fullrecord_metadata.category_info.subheadings.subheading",
                )
                if type(data_subheadings) == list:
                    sub_headings = "|".join(
                        [x for x in data_subheadings]
                    )  # returns sub headings strings pipe-separated
                else:
                    sub_headings = data_subheadings
                # grants ###############################################
                data_grants = _get_all_occurrences_by_path(
                    record, "static_data.fullrecord_metadata.fund_ack.grants.grant"
                )
                grants = []
                if len(data_grants) > 0:
                    for x in data_grants:
                        w_funder = _get_all_occurrences_by_path(x, "grant_agency")
                        w_grant = _get_all_occurrences_by_path(x, "grant_ids.grant_id")
                        if len(w_funder) == 0:
                            funder = ""
                        else:
                            funder = w_funder[0]
                        if len(w_grant) == 0:
                            grant = ""
                        else:
                            grant = w_grant[0]
                        grants.append({"funder": funder, "grant_id": grant})
                # conferences ######################################################
                data_conferences = _get_all_occurrences_by_path(
                    record, "static_data.summary.conferences.conference"
                )
                conferences = []
                if len(data_conferences) > 0:
                    conferences.append(
                        {
                            "conf_title": _get_all_occurrences_by_path(
                                data_conferences[0], "conf_titles.conf_title"
                            )[0],
                            "conf_date": _get_all_occurrences_by_path(
                                data_conferences[0], "conf_dates.conf_date.content"
                            )[0],
                            "conf_place": f'{_get_all_occurrences_by_path(data_conferences[0], "conf_locations.conf_location.conf_city")[0]}, {_get_all_occurrences_by_path(data_conferences[0], "conf_locations.conf_location.conf_state")[0]}',
                        }
                    )

                # authors processing ###############################################
                all_authors = record["static_data"]["summary"]["names"]["name"]
                if type(all_authors) is dict:
                    all_authors = [all_authors]
                result = []
                if (
                    int(
                        record["static_data"]["fullrecord_metadata"]["addresses"][
                            "count"
                        ]
                    )
                    == 1
                ):
                    address_names = [
                        record["static_data"]["fullrecord_metadata"]["addresses"][
                            "address_name"
                        ]
                    ]
                else:
                    address_names = [
                        x
                        for x in record["static_data"]["fullrecord_metadata"][
                            "addresses"
                        ]["address_name"]
                    ]
                # for x in record["static_data"]["fullrecord_metadata"]["addresses"]["address_name"]:
                for x in address_names:
                    flipped_address_names = _flipped_dict(x)
                    if flipped_address_names["organizations"]:
                        organizations = "|".join(
                            [
                                x["content"]
                                for x in flipped_address_names["organizations"][
                                    "address_spec"
                                ]["organization"]
                            ]
                        )
                    else:
                        organizations = ""
                    if flipped_address_names["count"]:
                        if int(flipped_address_names["count"]["names"]) > 1:
                            for x in flipped_address_names["name"]["names"]:
                                auth_obj = _parse_wos_authors_object(x, organizations)
                                result.append(auth_obj)
                        else:
                            auth_obj = _parse_wos_authors_object(
                                flipped_address_names["name"]["names"], organizations
                            )
                            result.append(auth_obj)
                result_unique_seq_no = set(str(d["seq_no"]) for d in result)
                authors_without_addresses = [
                    x
                    for x in all_authors
                    if str(x["seq_no"]) not in result_unique_seq_no
                ]
                if len(authors_without_addresses) > 0:
                    for x in authors_without_addresses:
                        result.append(_parse_wos_authors_object(x, ""))
                authors = _deduplicate_and_concat(result, "seq_no", "organizations")
                return {
                    "wos_id": wos_id,
                    "identifiers": identifiers,
                    "doctypes": doctypes,
                    "publisher_full_name": publisher_full_name,
                    # "publisher_unified_name": publisher_unified_name,
                    "publisher_city": publisher_city,
                    "titles": titles,
                    "pub_info": pub_info,
                    "abstract": abstract,
                    "auth_keywords": auth_keywords,
                    "keywords": keywords,
                    "sub_headings": sub_headings,
                    "grants": grants,
                    "conferences": conferences,
                    "authors": authors,
                }
            else:
                return record
        else:
            pass


def _flipped_dict(obj):
    flipped = defaultdict(dict)
    for key, val in obj.items():
        for subkey, subval in val.items():
            flipped[subkey][key] = subval
    return flipped


def _parse_wos_authors_object(obj, organizations_strings):
    if "reprint" in obj:
        if obj["reprint"] == "Y":
            reprint = "Y"
    else:
        reprint = "N"
    if "wos_standard" in obj:
        wos_standard_name = obj["wos_standard"]
    else:
        wos_standard_name = ""
    return {
        "seq_no": str(obj["seq_no"]),
        "full_name": obj["full_name"],
        "wos_standard_name": wos_standard_name,
        "role": obj["role"],
        "reprint": reprint,
        "organizations": organizations_strings,
    }


def _deduplicate_and_concat(list_of_obj, key_to_dedup, key_to_concat):
    result = []
    seq_no_dict = {}

    for item in list_of_obj:
        seq_no = item[key_to_dedup]
        if seq_no in seq_no_dict:
            # Concatenate organizations if 'seq_no' is already in the dictionary
            seq_no_dict[seq_no][key_to_concat] += "|" + item[key_to_concat]
        elif seq_no != "{}":
            # Add the current item to the result list and update the dictionary
            result.append(item)
            seq_no_dict[seq_no] = item
    return result


def _get_all_occurrences_by_path(data, key_path):
    """
    Retrieve all occurrences of the key specified by dot notation in JSON data.

    Parameters:
    - data: JSON data (can be a dictionary or a list)
    - key_path: The key path with dot notation (e.g., "friends.name")

    Returns:
    - A flat list of values associated with the key, or an empty list if the key is not found
    """

    def get_value_by_path(data, keys):
        """
        Retrieve the value specified by a list of keys in nested JSON data.

        Parameters:
        - data: JSON data (can be a dictionary or a list)
        - keys: A list of keys representing the path

        Returns:
        - The value associated with the specified path, or None if the path is not found
        """
        if not keys:
            return (
                [data]
                if isinstance(data, (dict, list))
                else [data] if data is not None else []
            )

        current_key, *remaining_keys = keys

        if isinstance(data, dict):
            if current_key in data:
                if not remaining_keys:
                    return (
                        [data[current_key]]
                        if isinstance(data[current_key], (dict, list))
                        else [data[current_key]]
                    )
                return get_value_by_path(data[current_key], remaining_keys)

        elif isinstance(data, list):
            values = [get_value_by_path(item, keys) for item in data]
            return [value for sublist in values for value in sublist]

        return []

    keys = key_path.split(".")
    values = get_value_by_path(data, keys)
    if not (values):
        return []
    elif (type(values[0]) == dict) | (type(values[0]) == str):
        return values
    else:
        return values[0]


WosClient = Client(
    authentication_method=wos_authentication_method,
    response_handler=JsonResponseHandler,
)

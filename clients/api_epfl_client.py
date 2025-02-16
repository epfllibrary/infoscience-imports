"""EPFL API client for Infoscience imports"""

import os
import tenacity
from apiclient import (
    APIClient,
    endpoint,
    BasicAuthentication,
    JsonResponseHandler,
    exceptions,
)
from apiclient.retrying import retry_if_api_request_error
from apiclient.error_handlers import ErrorHandler
from dotenv import load_dotenv
from utils import manage_logger, clean_value
from config import logs_dir


api_epfl_base_url = "https://api.epfl.ch/v1"
# env var
load_dotenv(os.path.join(os.getcwd(), ".env"))
api_epfl_user = os.environ.get("API_EPFL_USER")
api_epfl_pwd = os.environ.get("API_EPFL_PWD")

api_epfl_authentication_method = BasicAuthentication(
    username=api_epfl_user, password=api_epfl_pwd
)

retry_decorator = tenacity.retry(
    retry=retry_if_api_request_error(status_codes=[429]),
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)


@endpoint(base_url=api_epfl_base_url)
class Endpoint:
    base = ""
    personsQuery = "persons?query={query}"
    personsFirstnameLastname = "persons?firstname={firstname}&lastname={lastname}"
    accredsId = "accreds?persid={sciperID}"
    unitsId = "units/{unitID}"


class Client(APIClient):

    log_file_path = os.path.join(logs_dir, "logging.log")
    logger = manage_logger(log_file_path)

    @retry_decorator
    def query_person(
        self,
        query,
        firstname=None,
        lastname=None,
        format="sciper",
        use_firstname_lastname=False,
    ):
        """
        Query a person's information from the API based on the provided parameters.

        This function attempts to retrieve a person's details using either a general query or specific first and last names.
        It supports retries in case of failures due to the `@retry_decorator`. The results are processed to identify a
        unique person or the best candidate among multiple matches.

        Args:
            query (str): The search query, which can be a person's name or identifier.
            firstname (str, optional): The person's first name. Required if `use_firstname_lastname` is True.
            lastname (str, optional): The person's last name. Required if `use_firstname_lastname` is True.
            format (str, optional): The format in which to return the person's details (default is "sciper").
            use_firstname_lastname (bool, optional): A flag indicating whether to use the first and last names in the query.

        Example:
            >>> person_info = query_person(query="Doe J", firstname="John", lastname="Doe", use_firstname_lastname=True)
            >>> print(person_info)
        """
        self.logger.debug(
            f"Initiating search for person with query: '{query}' using format: '{format}'"
        )

        def attempt_query(firstname, lastname):
            """Helper function to query the API and log results."""
            result = self.get(
                Endpoint.personsFirstnameLastname.format(
                    firstname=firstname, lastname=lastname
                )
            )
            self.logger.debug(
                f"Received response for {firstname} {lastname} from personsFirstnameLastname: {result}"
            )
            return result

        # Initialize results
        results = []

        # If using firstname/lastname, perform both queries
        if use_firstname_lastname:
            if (
                firstname and lastname
            ):  # Vérifie que firstname et lastname ne sont ni None ni vides
                self.logger.info(
                    f"Attempting personsFirstnameLastname for {lastname} {firstname}."
                )
                try:
                    results.append(attempt_query(lastname, firstname))
                except exceptions.ServerError:
                    self.logger.error(f"{lastname} {firstname} caused an EPFL API error")
                    pass
                
                self.logger.info(
                    f"Attempting personsFirstnameLastname for {firstname} {lastname}."
                )
                try:
                    results.append(attempt_query(firstname, lastname))
                except exceptions.ServerError:
                    self.logger.error(f"{firstname} {lastname} caused an EPFL API error")
                    pass
            else:
                self.logger.warning("Firstname or lastname is missing; skipping query.")

        # Always attempt personsQuery
        if query:
            self.logger.info(f"Attempting personsQuery for {query}.")
            result_query = self.get(Endpoint.personsQuery.format(query=query))
            self.logger.debug(
                f"Received response for {query} from personsQuery: {result_query}"
            )
            results.append(result_query)
            self.logger.debug(f"Response for personsQuery : {result_query}.")
            # Process results based on the count
            for result in results:
                if result and result["count"] == 1:
                    person_record = result["persons"][0]
                    self.logger.info(f"Single record found for {query}. Processing record.")
                    # Verify that the returned name matches the requested name
                    if (
                        lastname
                        and clean_value(person_record["lastname"]) == lastname
                    ):
                        return self._process_person_record(result, query, format)
                    else:
                        self.logger.warning(
                            f"The single record found does not match the requested name: {lastname}."
                        )
                    return None
        else:
            self.logger.warning("personsQuery is missing; skipping...")

        # Handle multiple records
        combined_results = [
            person
            for result in results
            if result
            for person in result.get("persons", [])
        ]

        if len(combined_results) > 1:
            self.logger.warning(
                f"Multiple records found for {query}. Attempting to identify the best candidate."
            )

            initial = query.split(" ")[1].upper() if " " in query else None

            best_candidate = self._identify_best_candidate(
                {"persons": combined_results}, lastname, initial
            )

            if best_candidate:
                self.logger.info(
                    f"Best candidate identified: {best_candidate['display']}"
                )
                return self._process_person_record(
                    {"count": 1, "persons": [best_candidate]}, query, format
                )
            else:
                self.logger.warning(
                    "No suitable candidate found among the multiple records."
                )
                return None

        # If no records found
        self.logger.warning(f"No valid record found for {query}.")

    @retry_decorator
    def fetch_accred_by_unique_id(self, sciper_id: str, format="digest"):
        self.logger.info(
            f"Fetching accreditation for sciper_id: '{sciper_id}' using format: '{format}'"
        )
        result = self.get(Endpoint.accredsId.format(sciperID=sciper_id))
        self.logger.debug(f"Received response for {sciper_id}: {result}")
        return self._process_accred_record(result, sciper_id, format)

    @retry_decorator
    def fetch_unit_by_unique_id(self, unit_id: str, format="digest"):
        self.logger.info(
            f"Fetching units for unit_id: '{unit_id}' using format: '{format}'"
        )
        result = self.get(Endpoint.unitsId.format(unitID=unit_id))
        self.logger.debug(f"Received response for {unit_id}: {result}")
        return self._process_unit_record(result, unit_id, format)

    def _process_person_record(self, record, query, format):
        self.logger.debug(
            f"Processing person record for query: '{query}' with format: '{format}'"
        )
        if format == "sciper":
            self.logger.debug(f"Extracting sciperId information for {query}.")
            return self._extract_sciper_person_info(record)
        elif format == "digest":
            self.logger.debug(
                f"Extracting sciperId and unitIds information for {query}."
            )
            return self._extract_digest_person_info(record)
        elif format == "epfl":
            self.logger.debug(f"Returning full record for {query}.")
            return record

    def _process_accred_record(self, record, sciper_id, format):
        self.logger.debug(
            f"Processing accred record for sciper_id: '{sciper_id}' with format: '{format}'"
        )

        # Check if 'accreds' is present and not empty
        if "accreds" not in record or not record["accreds"]:
            self.logger.warning(
                f"No accreditation records found for sciper_id: '{sciper_id}'."
            )
            return None  # or return an empty list or dict as needed

        if format == "digest":
            self.logger.debug(f"Extracting sciperId information for {sciper_id}.")
            return [
                self._extract_accred_units_info(accred, accred.get("order"))
                for accred in record.get("accreds", [])
            ]  # to keep the order of units
        elif format == "mainUnit":
            self.logger.debug(f"Extracting main unit information for {sciper_id}.")
            return self._extract_accred_units_info(
                record["accreds"][0], record["accreds"][0].get("order")
            )
        elif format == "epfl":
            self.logger.debug(f"Returning full record for {sciper_id}.")
            return record

    def _extract_sciper_person_info(self, x):
        self.logger.info("Extracting sciper person information from the record.")
        sciper_id = x["persons"][0]["id"]
        self.logger.debug(f"Extracted sciper_id from record: {sciper_id}")
        return sciper_id

    def _extract_digest_person_info(self, x):
        self.logger.info("Extracting digest person information from the record.")
        record = {
            "sciper_id": x["persons"][0]["id"],
            "unitsIds": "|".join([unit["unitid"] for unit in x["persons"][0]["rooms"]]),
        }
        self.logger.debug(f"Extracted digest record: {record}")
        return record

    def _extract_accred_units_info(self, x, parent_order=None):
        self.logger.info("Extracting units information from the accred record.")
        unit_type = self.fetch_unit_by_unique_id(str(x["unit"]["id"]))
        record = {
            "unit_id": str(x["unit"]["id"]),
            "unit_name": x["unit"]["name"],
            "unit_type": unit_type,
            "unit_order": parent_order,
        }
        self.logger.debug(f"Extracted units from accred record: {record}")
        return record

    def _process_unit_record(self, record, unit_id, format):
        if format == "digest":
            self.logger.debug(f"Extracting unit type information for {unit_id}.")
            return self._extract_unittype_info(record)
        elif format == "epfl":
            self.logger.debug(f"Returning full record for {unit_id}.")
            return record

    def _extract_unittype_info(self, x):
        self.logger.info("Extracting unit type information from the unit record.")
        unit_type = None  # Default value
        try:
            unit_type = x.get("unittype", {}).get("label")
            self.logger.debug(f"Extracted unit type from unit record: {unit_type}")
        except (AttributeError, TypeError):
            self.logger.warning("Missing 'unittype' or 'label' in the record.")
        return unit_type

    def _identify_best_candidate(self, results, lastname, initial=None):
        """
        Identifies the best candidate from the list of results based on the last name
        and an optional initial of the first name.

        :param results: List of person records returned from the API.
        :param lastname: Last name to match against.
        :param initial: Initial of the first name to match against (optional).
        :return: The best candidate record or None if no match is found.
        """
        candidates = []

        for person in results["persons"]:
            if person["lastname"].lower() == lastname.lower():
                if initial:
                    # Check if the initial matches
                    if person["firstname"].startswith(initial.upper()):
                        candidates.append(person)
                else:
                    # No initial provided, consider all matches
                    candidates.append(person)

        # Return the best candidate based on additional criteria, if necessary
        if candidates:
            if len(candidates) == 1:
                return candidates[0]  # Only one candidate found
            else:
                # If multiple candidates, apply further logic to determine the best candidate
                # For this example, we can prioritize based on some custom logic, such as:
                # 1. Prioritize based on the presence of an email
                # 2. Prioritize based on known affiliations or roles
                best_candidate = max(
                    candidates, key=lambda x: ("email" in x) + ("org" in x)
                )
                return best_candidate

        return None


ApiEpflClient = Client(
    authentication_method=api_epfl_authentication_method,
    response_handler=JsonResponseHandler,
    error_handler=ErrorHandler,
)

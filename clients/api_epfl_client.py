"""EPFL API client for Infoscience imports"""

import os
import re
import string
import requests
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
from utils import get_pipeline_logger, clean_value
from config import unit_types as DEFAULT_UNIT_TYPES, secondary_unit_types as DEFAULT_SECONDARY_UNIT_TYPES, excluded_unit_types as DEFAULT_EXCLUDED_UNIT_TYPES


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
    personsId = "persons/{sciperID}"
    personsIds = "persons?ids={sciperIDs}"
    personsQuery = "persons?query={query}"
    personsFirstnameLastname = "persons?firstname={firstname}&lastname={lastname}"
    accredsId = "accreds?persid={sciperID}"
    accredsByClassPosition = "accreds?classid={classID}&positionid={positionID}&state=active"
    unitsId = "units/{unitID}"
    unitsQuery = "units?query={query}" 


class Client(APIClient):

    logger = get_pipeline_logger('api_epfl')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._unit_type_cache = {}
        self.unit_types = DEFAULT_UNIT_TYPES
        self.secondary_unit_types = DEFAULT_SECONDARY_UNIT_TYPES
        self.excluded_unit_types = DEFAULT_EXCLUDED_UNIT_TYPES

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
            # Construction de l'URL de base
            url = Endpoint.personsFirstnameLastname.format(
                firstname=firstname, lastname=lastname
            )

            # Si le nom est trop court, on ajoute strict=1
            if lastname and len(lastname) < 2:
                sep = "&" if "?" in url else "?"
                url += f"{sep}strict=1"

            result = self.get(url)

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
                self.logger.debug(
                    "EPFL API: personsFirstnameLastname query for %s %s", lastname, firstname
                )
                try:
                    results.append(attempt_query(lastname, firstname))
                except exceptions.ServerError:
                    self.logger.error(f"{lastname} {firstname} caused an EPFL API error")
                    pass

                self.logger.debug(
                    "EPFL API: personsFirstnameLastname query for %s %s", firstname, lastname
                )
                try:
                    results.append(attempt_query(firstname, lastname))
                except exceptions.ServerError:
                    self.logger.error(f"{firstname} {lastname} caused an EPFL API error")
                    pass
            else:
                self.logger.debug("EPFL API: firstname or lastname missing, skipping")

        # Always attempt personsQuery
        if query:
            # Skip si lastname trop court et firstname trop court aussi
            if lastname and len(lastname) < 2 and firstname and len(firstname) < 2:
                self.logger.warning(
                    f"Skipping personsQuery because lastname='{lastname}' "
                    f"is too short and firstname='{firstname}' has only one character."
                )
                return None

            self.logger.debug("EPFL API: personsQuery for %s", query)
            result_query = self.get(Endpoint.personsQuery.format(query=query))
            self.logger.debug(
                f"Received response for {query} from personsQuery: {result_query}"
            )
            results.append(result_query)
            self.logger.debug(f"Response for personsQuery : {result_query}.")
            # Process results based on the count
            for result in results:
                if result and result.get("count") == 1 and result.get("persons"):
                    person_record = result["persons"][0]
                    self.logger.debug("EPFL API: single match for %s", query)

                    # Vérifie le nom *seulement* si `lastname` est défini (donc utilisé dans la requête)
                    if lastname:
                        if self._clean_value(person_record["lastname"]) == self._clean_value(lastname):
                            return self._process_person_record(result, query, format)
                        else:
                            self.logger.warning(
                                f"The single record {self._clean_value(person_record['lastname'])} found does not match the requested name: {lastname}."
                            )
                            continue
                    else:
                        # Aucun lastname à vérifier, on accepte le résultat unique
                        return self._process_person_record(result, query, format)
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
                self.logger.debug("EPFL API: best candidate selected: %s", best_candidate['display'])
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
        return None

    @retry_decorator
    def fetch_person_by_sciper(self, sciper_id: str, format="epfl"):
        self.logger.debug("EPFL API: person lookup for sciper %s", sciper_id)
        result = self.get(Endpoint.personsIds.format(sciperIDs=sciper_id))
        self.logger.debug(f"Received response for {sciper_id}: {result}")
        return self._process_person_record(result, sciper_id, format)

    def fetch_persons_orcid_batch(self, scipers: list[str], batch_size: int = 100) -> dict[str, str]:
        """Fetch ORCID iDs for multiple scipers via persons?ids= in batches.

        Returns {sciper: bare_orcid_id}. Scipers without an ORCID are omitted.
        Silently skips failed batches and logs a warning.
        """
        result: dict[str, str] = {}
        for i in range(0, len(scipers), batch_size):
            batch = scipers[i:i + batch_size]
            ids_param = ",".join(str(s) for s in batch)
            try:
                response = self.get(Endpoint.personsIds.format(sciperIDs=ids_param))
                for person in response.get("persons", []):
                    sciper = str(person.get("id") or "").strip()
                    orcid_raw = (person.get("orcid") or "").strip()
                    if orcid_raw:
                        bare = orcid_raw.replace("https://orcid.org/", "").strip()
                        if bare:
                            result[sciper] = bare
            except Exception as exc:
                self.logger.warning(
                    "persons batch fetch failed (offset %d, %d scipers): %s",
                    i, len(batch), exc,
                )
        return result

    @retry_decorator
    def fetch_accred_by_class_position(
        self,
        class_id: str,
        position_id: str,
        status_id: str | None = None,
        format: str = "digest",
    ):
        self.logger.debug(
            "EPFL API: accreditation for class_id=%s position_id=%s status_id=%s",
            class_id, position_id, status_id,
        )
        url = Endpoint.accredsByClassPosition.format(classID=class_id, positionID=position_id)
        if status_id is not None:
            url += f"&statusid={status_id}"
        result = self.get(url)
        self.logger.debug("Received response for class_id=%s position_id=%s: %s", class_id, position_id, result)
        return self._process_accred_record(result, f"{class_id}_{position_id}", format)

    @retry_decorator
    def fetch_accred_by_unique_id(self, sciper_id: str, format="digest"):
        self.logger.debug("EPFL API: accreditation for sciper %s", sciper_id)
        result = self.get(Endpoint.accredsId.format(sciperID=sciper_id))
        self.logger.debug(f"Received response for {sciper_id}: {result}")
        return self._process_accred_record(result, sciper_id, format)

    @retry_decorator
    def fetch_unit_by_unique_id(self, unit_id: str, format="digest"):
        self.logger.debug("EPFL API: unit lookup for id %s", unit_id)
        result = self.get(Endpoint.unitsId.format(unitID=unit_id))
        self.logger.debug(f"Received response for {unit_id}: {result}")
        return self._process_unit_record(result, unit_id, format)

    @retry_decorator
    def fetch_unit_by_query(self, query: str, format="digest"):
        self.logger.debug("EPFL API: unit query for %s", query)
        result = self.get(Endpoint.unitsQuery.format(query=query))
        self.logger.debug(f"Received response for {query}: {result}")
        return self._process_unit_record(result, query, format)

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
        elif format == "person":
            self.logger.debug(f"Extracting person+unit information for {sciper_id}.")
            return [
                self._extract_accred_person_info(accred, accred.get("order"))
                for accred in record.get("accreds", [])
            ]
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
        self.logger.info("Extracting enriched digest person information from the record.")

        person = x["persons"][0]

        epfl_status = person.get("status", None)
        position_labelen = person.get("position", {}).get("labelen", None)
        orcid_raw = person.get("orcid")
        orcid = None
        if isinstance(orcid_raw, str):
            stripped = orcid_raw.replace("https://orcid.org/", "").strip()
            orcid = stripped if stripped else None

        record = {
            "sciper_id": person.get("id"),
            "epfl_status": epfl_status,
            "epfl_position": position_labelen,
            "epfl_orcid": orcid,
        }

        self.logger.debug(f"Extracted enriched digest record: {record}")
        return record

    def _extract_accred_person_info(self, accred, parent_order=None):
        person = accred.get("person") or accred.get("author") or {}
        sciper = person.get("sciper") or accred.get("persid") or person.get("id")

        status_labelen = (accred.get("status") or {}).get("labelen")
        class_labelen = (accred.get("class") or {}).get("labelen")
        position_labelen = (accred.get("position") or {}).get("labelen")
        validfrom = accred.get("startdate") or accred.get("validfrom")
        validto = accred.get("enddate") or accred.get("validto")

        enriched = {
            "sciper": str(sciper) if sciper is not None else None,
            "firstname": person.get("firstname"),
            "lastname": person.get("lastname"),
            "firstnameuc": person.get("firstnameuc"),
            "lastnameuc": person.get("lastnameuc"),
            "display": person.get("display"),
            "email": person.get("email"),
            "status": status_labelen,
            "class": class_labelen,
            "position": position_labelen,
            "validfrom": validfrom,
            "validto": validto,
        }
        enriched.update(self._extract_accred_units_info(accred, parent_order))
        self.logger.debug(f"Extracted accred person record: {enriched}")
        return enriched

    def _extract_accred_units_info(self, x, parent_order=None):
        self.logger.debug("Extracting units information from the accred record.")

        unit = x.get("unit", {})
        unit_id = unit.get("id")
        unit_path = unit.get("path")  # ex: "EPFL IC IINFCOM NAL"

        if unit_id is not None:
            cache_key = str(unit_id)
            if cache_key not in self._unit_type_cache:
                self._unit_type_cache[cache_key] = self.fetch_unit_by_unique_id(
                    cache_key
                )
            unit_type = self._unit_type_cache[cache_key]
        else:
            unit_type = None

        record = {
            "unit_id": str(unit_id) if unit_id is not None else None,
            "unit_name": unit.get("name"),
            "unit_label": unit.get("labelen"),
            "unit_cf": unit.get("cf"),
            "unit_path": unit_path,
            "unit_type": unit_type,
            "unit_order": parent_order,
        }

        # ✅ derived fields from unit_path
        record.update(self._extract_acronym_levels_from_path(unit_path))

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
    
    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _as_int_or_none(self, v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def _split_unit_path(self, unit_path):
        """
        Split a unit_path like "EPFL IC IINFCOM NAL" into tokens.
        Robust to None / extra spaces.
        """
        if unit_path is None:
            return []
        s = str(unit_path).strip()
        if not s:
            return []
        return [tok for tok in s.split() if tok]

    def _extract_acronym_levels_from_path(self, unit_path):
        """
        Level rules:
          - position 0 = institution (EPFL)
          - position 1 = unit_level_2
          - position 2 = unit_level_3
        """
        toks = self._split_unit_path(unit_path)
        return {
            "unit_level_2": toks[1] if len(toks) >= 2 else None,
            "unit_level_3": toks[2] if len(toks) >= 3 else None,
        }

    def dedupe_people_keep_allowed_affiliations(self, records, unit_types_override=None, excluded_unit_types_override=None):
        if not isinstance(records, list) or not records:
            return []

        unit_types_local = unit_types_override if unit_types_override is not None else self.unit_types
        secondary_local = self.secondary_unit_types
        excluded_local = excluded_unit_types_override if excluded_unit_types_override is not None else self.excluded_unit_types

        by_sciper = {}
        for r in records:
            sciper = r.get("sciper")
            if sciper is None:
                continue
            by_sciper.setdefault(str(sciper), []).append(r)

        kept = []
        for sciper, recs in by_sciper.items():
            allowed = []
            secondary = []
            fallback_order1 = []

            for r in recs:
                unit_type = r.get("unit_type")
                unit_label = r.get("unit_label")
                unit_order = r.get("unit_order")

                if self._as_int_or_none(unit_order) == 1:
                    fallback_order1.append(r)

                # Always include Professor Emeritus and PH-* unit members
                # regardless of unit type, as they are excluded by normal filters.
                is_emeritus = str(r.get("position") or "").strip().lower() == "professor emeritus"
                is_ph_unit = str(r.get("unit_name") or "").upper().startswith("PH-")
                if is_emeritus or is_ph_unit:
                    allowed.append(r)
                    continue

                if not unit_type or unit_type in (None, "", "null"):
                    continue
                if excluded_local and unit_type in excluded_local:
                    continue

                name_matches_laboratory = isinstance(unit_label, str) and re.search(
                    r"\b(laboratoire|laboratory|lab|labo)\b", unit_label, re.IGNORECASE
                )
                if unit_type in unit_types_local or name_matches_laboratory:
                    allowed.append(r)
                elif unit_type in secondary_local:
                    secondary.append(r)

            if allowed:
                kept.extend(allowed)
            elif secondary:
                secondary.sort(key=lambda x: (x.get("unit_order") is None, x.get("unit_order")))
                kept.append(secondary[0])
            elif fallback_order1:
                fallback_order1.sort(key=lambda x: (x.get("unit_order") is None, x.get("unit_order")))
                kept.append(fallback_order1[0])
            else:
                kept.append(recs[0])

        return kept

    def _join_unique(self, values, sep="||"):
        out = []
        seen = set()
        for v in values:
            if v is None:
                continue
            s = str(v).strip()
            if not s or s.lower() == "null":
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
        return sep.join(out) if out else None

    def merge_records_by_sciper_with_preferred_unit(self, records, sep="||", prefer_lowest_unit_order=True):
        if not isinstance(records, list) or not records:
            return []

        multi_fields = [
            "unit_id", "unit_name", "unit_label", "unit_cf", "unit_path",
            "unit_level_2", "unit_level_3", "unit_type", "unit_order",
            "status", "class", "position",
        ]
        single_fields = ["sciper", "firstname", "lastname", "firstnameuc", "lastnameuc", "display", "email"]

        groups = {}
        for r in records:
            sciper = r.get("sciper")
            if sciper is None:
                continue
            groups.setdefault(str(sciper), []).append(r)

        merged_rows = []
        for sciper, recs in groups.items():
            if prefer_lowest_unit_order:
                sorted_recs = sorted(
                    recs,
                    key=lambda x: (self._as_int_or_none(x.get("unit_order")) is None, self._as_int_or_none(x.get("unit_order"))),
                )
            else:
                sorted_recs = recs

            preferred = sorted_recs[0] if sorted_recs else None
            merged = {}

            for f in single_fields:
                val = None
                for r in recs:
                    v = r.get(f)
                    if v is None:
                        continue
                    s = str(v).strip()
                    if s and s.lower() != "null":
                        val = v
                        break
                merged[f] = val

            for f in multi_fields:
                merged[f] = self._join_unique((r.get(f) for r in sorted_recs), sep=sep)

            if preferred:
                merged.update({
                    "preferred_unit_id": preferred.get("unit_id"),
                    "preferred_unit_name": preferred.get("unit_name"),
                    "preferred_unit_label": preferred.get("unit_label"),
                    "preferred_unit_cf": preferred.get("unit_cf"),
                    "preferred_unit_path": preferred.get("unit_path"),
                    "preferred_unit_type": preferred.get("unit_type"),
                    "preferred_unit_order": preferred.get("unit_order"),
                    "preferred_unit_level_2": preferred.get("unit_level_2"),
                    "preferred_unit_level_3": preferred.get("unit_level_3"),
                    "preferred_validfrom": preferred.get("validfrom"),
                    "preferred_position": preferred.get("position"),
                    "preferred_class": preferred.get("class"),
                })
            else:
                merged.update({k: None for k in [
                    "preferred_unit_id", "preferred_unit_name", "preferred_unit_label",
                    "preferred_unit_cf", "preferred_unit_path", "preferred_unit_type",
                    "preferred_unit_order", "preferred_unit_level_2", "preferred_unit_level_3",
                    "preferred_validfrom", "preferred_position", "preferred_class",
                ]})

            merged_rows.append(merged)

        return merged_rows

    def _clean_value(self, formatted_name):
        formatted_name = formatted_name.lower()

        # Replace dash-like characters between initials or names with space
        formatted_name = re.sub(r"[-‐‑‒–—―⁃﹘﹣－]", " ", formatted_name)

        # Separate joined initials (e.g., J.-L. → J L)
        formatted_name = re.sub(r"\b([A-Z])\.\-?([A-Z])\.\b", r"\1 \2", formatted_name)

        # Remove remaining periods (e.g., J. → J)
        formatted_name = formatted_name.replace(".", " ")

        # Remove any leftover punctuation
        formatted_name = formatted_name.translate(str.maketrans("", "", string.punctuation))
        # Normalize whitespace
        formatted_name = re.sub(r"\s+", " ", formatted_name).strip()

        return formatted_name


ApiEpflClient = Client(
    authentication_method=api_epfl_authentication_method,
    response_handler=JsonResponseHandler,
    error_handler=ErrorHandler,
)


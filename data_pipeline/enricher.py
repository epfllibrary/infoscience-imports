"""Metadata enrichment: processors for authors and publications"""

import os
import string
import re
from concurrent.futures import ThreadPoolExecutor
import unicodedata
from unidecode import unidecode
import pandas as pd
from fuzzywuzzy import fuzz, process
import nameparser
from nameparser import HumanName
from utils import manage_logger, clean_value


from clients.api_epfl_client import ApiEpflClient
from clients.unpaywall_client import UnpaywallClient
from clients.dspace_client_wrapper import DSpaceClientWrapper
from clients.orcid_client import OrcidClient
from config import scopus_epfl_afids, unit_types, excluded_unit_types
from config import logs_dir


dspace_wrapper = DSpaceClientWrapper()


class AuthorProcessor:
    """
    This class is designed to process a DataFrame containing research publications and enrich it with information about EPFL affiliations.
    It supports processing for publications from Scopus and Web of Science (WOS) sources. For each publication, it checks if the organization
    field contains any EPFL affiliation indicators and marks the publication accordingly.

    Attributes:
        df (DataFrame): The DataFrame containing the publications to be processed.

    Methods:
        process(): Processes the DataFrame and enriches it with EPFL affiliation information.
        process_scopus(text): Processes the organization text for Scopus publications to check for EPFL affiliations.
        process_wos(text): Processes the organization text for WOS publications to check for EPFL affiliations.
    Usage
    processor = Processor(your_dataframe)
    processor.process().nameparse_authors().orcid_data_reconciliation()
    """

    def __init__(self, df):
        self.df = df

        log_file_path = os.path.join(logs_dir, "logging.log")
        self.logger = manage_logger(log_file_path)
        self._accred_cache = {}        

    def process(self, return_df=False, author_ids_to_check=None):
        """
        Process the DataFrame to detect EPFL-affiliated authors.
        Additionally, checks if any given researcher ID is present in internal_author_id
        and marks epfl_affiliation=True.

        Args:
            return_df (bool): If True, returns the processed DataFrame.
            author_ids_to_check (list, optional): List of researcher IDs to check. If any of these
                                                IDs appear in internal_author_id, epfl_affiliation=True.

        Returns:
            DataFrame or self: Processed DataFrame if return_df is True, otherwise self.
        """

        self.df = self.df.copy()  # Avoid modifying the original DataFrame

        # Step 1: Detect EPFL-affiliated authors based on organization names
        self.df["epfl_affiliation"] = self.df.apply(
            lambda row: (
                self.process_scopus(row["organizations"], check_all=True)
                if row["source"] == "scopus"
                else (
                    self.process_wos(row["organizations"])
                    if row["source"] == "wos"
                    else (
                        self.process_openalex(row["organizations"])
                        if row["source"] in ("openalex", "openalex+crossref")
                        else (
                            self.process_crossref(row["organizations"])
                            if row["source"] == "crossref"
                            else (
                                self.process_zenodo(row["organizations"])
                                if row["source"] == "zenodo"
                                else (
                                    self.process_datacite(row["organizations"])
                                    if row["source"] == "datacite"
                                    else False
                                )
                            )
                        )
                    )
                )
            ),
            axis=1,
        )

        # Step 2: Override epfl_affiliation if internal_author_id matches a given author ID
        if author_ids_to_check:
            author_ids_to_check = set(
                map(str, author_ids_to_check)
            )  # Convert list to string set for fast lookup
            self.df["epfl_affiliation"] = self.df.apply(
                lambda row: (
                    True
                    if str(row["internal_author_id"]) in author_ids_to_check
                    or str(row["orcid_id"]) in author_ids_to_check
                    else row["epfl_affiliation"]
                ),
                axis=1,
            )

        return self.df if return_df else self

    def process_scopus(self, text, check_all=False):
        """
        Checks if an EPFL affiliation is present in the 'organizations' field for Scopus.

        Args:
            text (str): The text to analyze.
            check_all (bool): If True, compares all values separated by '|'.
                            If False, only compares the first value.

        Returns:
            bool: True if an EPFL affiliation is detected, False otherwise.
        """
        if not isinstance(text, str):
            return False

        # Split the text into values separated by '|'
        values = [v.strip() for v in text.split("|")]

        # Compare based on the check_all flag
        if check_all:
            # Check all values
            return any(any(value in v for value in scopus_epfl_afids) for v in values)
        else:
            # Check only the first value
            return any(value in values[0] for value in scopus_epfl_afids)

    def _normalize_signature(self, text: str) -> str:
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.lower()
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return text.strip()

    def process_datacite(self, text):
        if not isinstance(text, str):
            return False
        norm = self._normalize_signature(text)
        pattern = re.compile(
            r"\b(?:"
            r"epfl"
            r"|ecole\s+polytechnique\s+federale\s+de\s+lausanne"
            r"|swiss\s+federal\s+institute\s+of\s+technology\s+in\s+lausanne"
            r")\b"
        )
        return bool(pattern.search(norm))

    def process_zenodo(self, text):
        if not isinstance(text, str):
            return False
        pattern = "(?:EPFL|[Pp]olytechnique [Ff].d.rale de Lausanne)"
        return bool(re.search(pattern, text))

    def process_crossref(self, text):
        if not isinstance(text, str):
            return False
        pattern = "(?:EPFL|[Pp]olytechnique [Ff].d.rale de Lausanne)"
        return bool(re.search(pattern, text))

    def process_openalex(self, text):
        if not isinstance(text, str):
            return False
        pattern = r"(02s376052|EPFL|[Pp]olytechnique [Ff].d.rale de Lausanne)"
        return bool(re.search(pattern, text, re.IGNORECASE))

    def process_wos(self, text):
        keywords = [
            "EPFL",
            "Ecole Polytechnique Federale de Lausanne",
            "Ecole Polytech Federale Lausanne",
        ]
        if not isinstance(text, str):  # Vérifie que text est bien une chaîne
            return False
        return any(
            process.extractOne(keyword, [text], scorer=fuzz.partial_ratio)[1] >= 80
            for keyword in keywords
        )

    def filter_epfl_authors(self, return_df=False):
        self.df = self.df.copy()

        self.df = self.df[self.df['epfl_affiliation']]
        return self.df if return_df else self

    def clean_authors(self, return_df=False):
        self.df = self.df.copy()

        def clean_author(author):
            parsed_name = HumanName(author)
            # Assemble name components
            formatted_name = (
                f"{parsed_name.last} {parsed_name.first} {parsed_name.middle}".strip()
            )

            # Transliterate characters to closest ASCII (e.g., ø → o, Μ → M)
            formatted_name = unidecode(formatted_name)

            # Replace dash-like characters between initials or names with space
            formatted_name = re.sub(r"[-‐‑‒–—―⁃﹘﹣－]", " ", formatted_name)

            # Separate joined initials (e.g., J.-L. → J L)
            formatted_name = re.sub(r"\b([A-Z])\.\-?([A-Z])\.\b", r"\1 \2", formatted_name)

            # Remove remaining periods (e.g., J. → J)
            formatted_name = formatted_name.replace(".", " ")

            # Remove any leftover punctuation
            formatted_name = formatted_name.translate(
                str.maketrans("", "", string.punctuation)
            )

            # Normalize whitespace
            formatted_name = re.sub(r"\s+", " ", formatted_name).strip()

            return formatted_name

        self.df["author_cleaned"] = self.df["author"].apply(clean_author)

        return self.df if return_df else self

    def nameparse_authors(self, return_df=False):
        parser = nameparser.HumanName
        self.df = self.df.copy()  # Créer une copie du DataFrame si nécessaire

        def parse_name(author_name):
            # Essayer de détecter si le format est "Nom, Prénom" ou "Prénom Nom"
            if "," in author_name:
                # Si la virgule est présente, on suppose le format "Nom, Prénom"
                parts = author_name.split(",")
                last_name = parts[0].strip()
                first_name = parts[1].strip()
            else:
                # Sinon, on suppose le format "Prénom Nom"
                parts = author_name.split()
                first_name = " ".join(parts[:-1])  # Les prénoms peuvent être multiples
                last_name = parts[-1]  # Le dernier élément est le nom de famille

            # Utiliser nameparser pour extraire le nom complet et les prénoms (y compris middle names)
            name = parser(first_name + " " + last_name)
            return name

        # Appliquer le parsing à chaque ligne
        self.df.loc[:, "nameparse_firstname"] = self.df.apply(
            lambda row: (
                " ".join(
                    [parser(row["author"]).first, parser(row["author"]).middle]
                    if row["epfl_affiliation"]
                    else None
                ).strip()
                if row["epfl_affiliation"]
                else None
            ),
            axis=1,
        )

        self.df.loc[:, "nameparse_lastname"] = self.df.apply(
            lambda row: parser(row["author"]).last if row["epfl_affiliation"] else None,
            axis=1,
        )

        # Correction des cas où le nom n'est pas bien divisé
        self.df["nameparse_lastname"] = self.df["author"].apply(
            lambda x: parse_name(x).last
        )
        self.df["nameparse_firstname"] = self.df["author"].apply(
            lambda x: " ".join([parse_name(x).first, parse_name(x).middle]).strip()
        )

        return self.df if return_df else self

    def _fetch_accred_info(self, sciper_id):
        """
        Fetch accreditation information for a person based on specific priority rules.
        Uses a local cache to avoid redundant lookups and skips units with null/empty unit_type.

        Args:
            sciper_id (str): The unique identifier of the person.

        Returns:
            tuple: (unit_id, unit_name) or (None, None) if no result is found.
        """
        if sciper_id in self._accred_cache:
            return self._accred_cache[sciper_id]

        if pd.notna(sciper_id):
            records = ApiEpflClient.fetch_accred_by_unique_id(
                sciper_id, format="digest"
            )
            self.logger.debug("Person record: %s", records)

            if isinstance(records, list) and records:
                prioritized_unit = None
                allowed_units = []
                fallback_unit = None

                for record in records:
                    unit_order = record.get("unit_order")
                    unit_type = record.get("unit_type")
                    unit_id = record.get("unit_id")
                    unit_name = record.get("unit_name")

                    # Skip units with null, empty, or excluded unit_type
                    if not unit_type or unit_type in (None, "", "null"):
                        continue
                    if (
                        excluded_unit_types is not None
                        and unit_type in excluded_unit_types
                    ):
                        continue

                    # Check if unit_name contains 'laboratoire' or 'laboratory' (case-insensitive)
                    name_matches_laboratory = (
                        isinstance(unit_name, str)
                        and re.search(r"\b(laboratoire|laboratory)\b", unit_name, re.IGNORECASE)
                    )

                    if (
                        unit_order == 1
                        and (unit_type in unit_types or name_matches_laboratory)
                        and not prioritized_unit
                    ):
                        prioritized_unit = (unit_id, unit_name)

                    if unit_type in unit_types or name_matches_laboratory:
                        allowed_units.append(
                            (unit_id, unit_name, unit_type, unit_order)
                        )

                    if unit_order == 1 and not fallback_unit:
                        fallback_unit = (unit_id, unit_name)

                if prioritized_unit:
                    self.logger.debug("Main unit retrieved: %s", prioritized_unit)
                    self._accred_cache[sciper_id] = prioritized_unit
                    return prioritized_unit

                if allowed_units:
                    allowed_units.sort(key=lambda x: x[3])
                    result = allowed_units[0][:2]
                    self.logger.debug("Main unit retrieved: %s", result)
                    self._accred_cache[sciper_id] = result
                    return result

                if fallback_unit:
                    self.logger.debug("Main unit retrieved: %s", fallback_unit)
                    self._accred_cache[sciper_id] = fallback_unit
                    return fallback_unit

            default = ("10000", "EPFL")
            self.logger.warning(
                "No authorized unit type found. Returning EPFL Unit."
            )
            self._accred_cache[sciper_id] = default
            return default

    def _query_dspace_authority(self, query):
        """
        Attempts to retrieve author information from DSpace authority service.
        If successful, returns sciper_id, epfl_api_mainunit_name, and dspace_uuid;
        otherwise, returns None values.
        """

        try:
            response = dspace_wrapper.search_authority(filter_text=query)
            self.logger.info("Querying DSpace for author %s", query)
            sciper_id = dspace_wrapper.get_sciper_from_authority(response)
            self.logger.info("Sciper %s was retrieved in DSpace for author %s", sciper_id, query)
            return sciper_id
        except Exception as e:
            self.logger.error(
                "Error querying DSpace for author %s - %s", query, e
            )

        return None

    def api_epfl_reconciliation(self, return_df=False):
        self.df = self.df.copy()  # Create a copy of the DataFrame if necessary
        cache = {}  # local cache to store results

        def make_cache_key(row):
            orcid = row.get("orcid_id")
            if orcid and str(orcid).strip():
                return f"orcid:{orcid}"

            internal_author_id = row.get("internal_author_id")
            source = row.get("source")
            if (
                internal_author_id
                and str(internal_author_id).strip()
                and source in ["scopus", "wos"]
            ):
                return f"{source}:{internal_author_id}"

            author_cleaned = row.get("author_cleaned")
            if author_cleaned and str(author_cleaned).strip():
                return f"name:{author_cleaned}"

            firstname = clean_value(row.get("nameparse_firstname", ""))
            lastname = clean_value(row.get("nameparse_lastname", ""))
            if firstname and lastname:
                return f"fullname:{firstname} {lastname}"

            return None

        def query_person(row):
            key = make_cache_key(row)
            if key and key in cache:
                self.logger.debug("Returned cached sciperId %s for key %s", cache[key], key)
                return cache[key]

            sciper_id = ""
            orcid_id = row.get("orcid_id")
            if orcid_id and pd.notna(orcid_id):
                sciper_id = self._query_dspace_authority(orcid_id)

            elif row.get("source") in ["scopus", "wos"] and pd.notna(row.get("internal_author_id")):
                identifier_map = {
                    "scopus": "person.identifier.scopus-author-id",
                    "wos": "person.identifier.rid"
                }
                sciper_id = self._query_dspace_authority(
                    f"{identifier_map[row['source']]}:({row['internal_author_id']})"
                )

            elif pd.notna(row.get("author_cleaned")):
                sciper_id = self._query_dspace_authority(row["author_cleaned"])

            if not sciper_id:
                firstname = clean_value(row.get("nameparse_firstname", ""))
                lastname = clean_value(row.get("nameparse_lastname", ""))
                sciper_id = ApiEpflClient.query_person(
                    query=row.get("author_cleaned"),
                    firstname=firstname,
                    lastname=lastname,
                    format="sciper",
                    use_firstname_lastname=True,
                )
            if key:
                cache[key] = sciper_id  # Mémoriser le résultat même si None
            return sciper_id

        self.df["sciper_id"] = self.df.apply(query_person, axis=1)

        if return_df:
            return self.df

        # Request ApiEpflClient.fetch_accred_by_unique_id for each row with a non-null sciper_id
        # Apply unified accreditation fetch
        self.df[["epfl_api_mainunit_id", "epfl_api_mainunit_name"]] = (
            self.df["sciper_id"]
            .apply(lambda s: self._fetch_accred_info(s))
            .apply(pd.Series)
        )

        return self.df if return_df else self

    def generate_dspace_uuid(self, return_df=False):
        """
        Generates DSpace UUID for each row in the DataFrame by querying a DSpace database.
        The function checks if the necessary columns ('sciper_id', 'orcid_id', 'author') exist
        before constructing queries. If the columns do not exist, the queries for those columns are skipped.
        The generated UUID is added to the 'dspace_uuid' column, and if applicable, the 'sciper_id' column is updated.

        Parameters:
        - return_df (bool): If True, the modified DataFrame is returned. If False, the instance is returned.

        Returns:
        - DataFrame or self: The modified DataFrame if return_df is True, else returns the instance.
        """

        # Create a copy of the original DataFrame to avoid modifying it directly
        self.df = self.df.copy()

        def get_dspace_data(row):
            """
            Queries the DSpace database based on available information in the row
            and returns the UUID, sciper_id, and main_affiliation if found.

            Parameters:
            - row (pd.Series): A row from the DataFrame containing the necessary fields.

            Returns:
            - tuple: A tuple containing (uuid, sciper_id, main_affiliation), or (None, None, None) if not found.
            """

            queries = []  # List to store queries for the DSpace database

            def is_valid(value):
                return pd.notna(value) and str(value).strip() != ""

            try:
                if "sciper_id" in row and is_valid(row.get("sciper_id")):
                    queries.append(f"epfl.sciperId:({row['sciper_id']})")

                if "orcid_id" in row and is_valid(row.get("orcid_id")):
                    queries.append(f"person.identifier.orcid:({row['orcid_id']})")

                if "author" in row and is_valid(row.get("author")):
                    clean_author = str(row["author"]).replace(",", "").strip()
                    queries.append(f'itemauthoritylookup:"{clean_author}"')

                if (
                    "source" in row and row["source"] == "scopus" and
                    "internal_author_id" in row and is_valid(row.get("internal_author_id"))
                ):
                    queries.append(f"person.identifier.scopus-author-id:({row['internal_author_id']})")

                if (
                    "source" in row and row["source"] == "wos" and
                    "internal_author_id" in row and is_valid(row.get("internal_author_id"))
                ):
                    queries.append(f"person.identifier.rid:({row['internal_author_id']})")

                # Iterate through each query
                for query in queries:
                    self.logger.info("Find person in DSpace with query: %s", query)
                    try:
                        if query:
                            result = dspace_wrapper.find_person(query=query)
                            if isinstance(result, dict) and all(k in result for k in ["uuid", "sciper_id"]):
                                self.logger.info("Found DSpace UUID: %s", result["uuid"])
                                self.logger.info("Found DSpace sciper_id: %s", result["sciper_id"])
                                return result["uuid"], result["sciper_id"]
                            else:
                                self.logger.warning("Unexpected result format for query '%s': %s", query, result)
                    except Exception as e:
                        self.logger.error("Exception while querying DSpace for query '%s': %s", query, str(e))

            except Exception as outer_e:
                self.logger.error("Unexpected error while building or executing DSpace queries: %s", str(outer_e))

            return None, None  # Return default if no valid result

        def update_row(row):
            original_sciper = row.get("sciper_id")
            uuid, found_sciper = get_dspace_data(row)
            # Update sciper if missing
            if pd.isna(original_sciper) and found_sciper:
                row["sciper_id"] = found_sciper
                # Only reconcile accreditation for newly found sciper
                uid, uname = self._fetch_accred_info(found_sciper)
                row["epfl_api_mainunit_id"] = uid
                row["epfl_api_mainunit_name"] = uname
            # Set DSpace UUID always
            row["dspace_uuid"] = uuid
            return row

        # Apply the update_row function to each row in the DataFrame
        self.df = self.df.apply(update_row, axis=1)

        # Return either the modified DataFrame or the instance based on the return_df flag
        return self.df if return_df else self

    ##### Unused #####################
    def orcid_data_reconciliation(self, return_df=False):
        self.df = self.df.copy()

        for index, row in self.df.iterrows():
            orcid_id = row['orcid_id']
            if pd.notna(orcid_id) and "|" not in orcid_id:
                print(row)
                response = OrcidClient.fetch_record_by_unique_id(orcid_id)

                if isinstance(response, dict):
                    for key, value in response.items():
                        if key != "orcid_id":  # Skip the orcid_id key
                            self.df.at[index, f'orcid_{key}'] = value
                else:
                    # If response is not a dict, fill with None
                    for key in response.keys() if isinstance(response, dict) else []:
                        self.df.at[index, f'orcid_{key}'] = None
            else:
                # If orcid_id is None, fill the new columns with None
                for key in self.df.columns:
                    if key.startswith('orcid_'):
                        self.df.at[index, key] = None

        return self.df if return_df else self


class PublicationProcessor:

    def __init__(self, df):
        self.df = df
        log_file_path = os.path.join(logs_dir, "logging.log")
        self.logger = manage_logger(log_file_path)

    def fetch_unpaywall_data(self, doi):
        return UnpaywallClient.fetch_by_doi(doi, format="best-oa-location")

    def process(self, return_df=True):
        self.df = self.df.copy()
        self.df["upw_is_oa"] = False

        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.fetch_unpaywall_data, self.df['doi'].dropna()))

        for index, result in zip(self.df.index, results):
            if result is not None:
                self.df.at[index, "upw_is_oa"] = bool(result.get("is_oa"))
                self.df.at[index, 'upw_oa_status'] = result.get('oa_status')
                self.df.at[index, "upw_license"] = result.get("license")
                self.df.at[index, "upw_version"] = result.get("version")
                self.df.at[index, 'upw_pdf_urls'] = result.get('pdf_urls')
                self.df.at[index, "upw_valid_pdf"] = result.get("valid_pdf")
            else:
                self.df.at[index, "upw_is_oa"] = None
                self.df.at[index, 'upw_oa_status'] = None
                self.df.at[index, "upw_license"] = None
                self.df.at[index, "upw_version"] = None
                self.df.at[index, 'upw_pdf_urls'] = None
                self.df.at[index, "upw_valid_pdf"] = None
                self.logger.warning("No unpaywall data returned for DOI %s.", self.df.at[index, 'doi'])

        return self.df if return_df else self

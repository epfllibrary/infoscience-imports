"""Metadata enrichment: processors for authors and publications"""

import os
import string
import re
from datetime import datetime
from collections import Counter
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
        pattern = r"(02s376052|EPFL|[Pp]olytechnique [Ff].d.rale de Lausanne|02hdt9m26|Swiss Data Science Center)"
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
                    unit_label = record.get("unit_label")

                    # Skip units with null, empty, or excluded unit_type
                    if not unit_type or unit_type in (None, "", "null"):
                        continue
                    if (
                        excluded_unit_types is not None
                        and unit_type in excluded_unit_types
                    ):
                        continue

                    # Check if unit_label contains 'laboratoire' or 'laboratory' (case-insensitive)
                    name_matches_laboratory = isinstance(unit_label, str) and re.search(
                        r"\b(laboratoire|laboratory|lab|labo)\b",
                        unit_label,
                        re.IGNORECASE,
                    )

                    if (
                        unit_order == 1
                        and (unit_type in unit_types or name_matches_laboratory)
                        and not prioritized_unit
                    ):
                        prioritized_unit = (unit_id, unit_name, unit_type)

                    if unit_type in unit_types or name_matches_laboratory:
                        allowed_units.append(
                            (unit_id, unit_name, unit_type, unit_order)
                        )

                    if unit_order == 1 and not fallback_unit:
                        fallback_unit = (unit_id, unit_name, unit_type)

                main_unit = None

                if prioritized_unit:
                    main_unit = prioritized_unit
                elif allowed_units:
                    allowed_units.sort(key=lambda unit: unit[3])
                    main_unit = allowed_units[0][:3]
                elif fallback_unit:
                    main_unit = fallback_unit

                if main_unit:
                    self.logger.debug("Main unit retrieved: %s", main_unit)
                    self._accred_cache[sciper_id] = main_unit
                    return main_unit

            default = ("10000", "EPFL", "Ecole")
            self.logger.warning(
                "No authorized unit type found. Returning EPFL Unit."
            )
            self._accred_cache[sciper_id] = default
            return default

    def _infer_unit_from_dspace_facets(self, sciper_id: str, year: int, facet="unitOrLab"):
        """
        Infer the most likely affiliation unit from DSpace publications using facet aggregation.

        This method uses DSpace's internal faceting (sorted by count by default) to deduce
        an author's most probable unit for a given year.

        Args:
            sciper_id (str): The EPFL unique identifier of the author.
            year (int): The publication year to scope the query.
            facet (str): The facet to query (default: 'unitOrLab').

        Todo:
            - Add logic when member is flagged as 'former' in DSpace.
            - Add logic to handle cases where EPFL is declared as "Hôte" or "Hors EPFL" in accred.
            - Add logic to handle cases where api_epfl_mainunit is "EPFL".

        Returns:
            str: The most frequent unit label (e.g. 'lasur', 'lphe'), or None if not found.
        """
        if not sciper_id or pd.isna(year):
            return None

        year = int(year)
        ranges = [
            (year - 2, year),
            (year - 3, year + 1),
            (year - 5, year + 1),
        ]

        unit_counter = Counter()

        for start_year, end_year in ranges:
            query = (
                f"cris.virtual.sciperId:({sciper_id}) "
                f"AND (dateIssued.year:[{start_year} TO {end_year}]) "
                f"AND (entityType:(Publication) -types:(doctoral thesis))"
            )
            try:
                facet_values = dspace_wrapper.client.get_facet_values(
                    facet_name=facet, query=query, configuration="researchoutputs", size=5
                )
            except Exception as e:
                self.logger.error(
                    "Error fetching facet values for sciper %s: %s", sciper_id, str(e)
                )
                continue

            if facet_values:
                top_value = facet_values[0]
                label = top_value.get("label", "")
                count = top_value.get("count", 0)
                if label and count > 3:
                    unit_label = label.upper()
                    self.logger.info(
                        "Inferred unit for %s (%s): %s (%d publications, from %d–%d)",
                        sciper_id,
                        year,
                        unit_label,
                        count,
                        start_year,
                        end_year,
                    )
                    return unit_label

        return None

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

    def reconcile_authors(self, return_df=False):
        self.df = self.df.copy()
        cache = {}

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

        def get_dspace_data(row):
            queries = []

            def is_valid(value):
                return pd.notna(value) and str(value).strip() != ""

            if is_valid(row.get("sciper_id")):
                queries.append(f"epfl.sciperId:({row['sciper_id']})")
            if is_valid(row.get("orcid_id")):
                queries.append(f"person.identifier.orcid:({row['orcid_id']})")
            if is_valid(row.get("author")):
                clean_author = str(row["author"]).replace(",", "").strip()
                queries.append(f'itemauthoritylookup:"{clean_author}"')
            if row.get("source") == "scopus" and is_valid(row.get("internal_author_id")):
                queries.append(
                    f"person.identifier.scopus-author-id:({row['internal_author_id']})"
                )
            if row.get("source") == "wos" and is_valid(row.get("internal_author_id")):
                queries.append(f"person.identifier.rid:({row['internal_author_id']})")

            for query in queries:
                self.logger.info("Find person in DSpace with query: %s", query)
                try:
                    result = dspace_wrapper.find_person(query=query)
                    if isinstance(result, dict) and all(
                        k in result for k in ["uuid", "sciper_id"]
                    ):
                        return result["uuid"], result["sciper_id"]
                except Exception as e:
                    self.logger.error(
                        "Error querying DSpace for query '%s': %s", query, str(e)
                    )

            return None, None

        def query_and_enrich_person(row):
            """
            Enriches author metadata by reconciling identifiers (sciper, ORCID, internal IDs) 
            and inferring their most likely EPFL unit affiliation at the time of publication.

            This function performs the following steps:
            1. Tries to find the author in DSpace to get the sciper ID and internal UUID.
            2. Uses the sciper or name information to query the EPFL API for additional metadata.
            3. Retrieves unit information from EPFL API (current accreditation).
            4. Infers the most likely unit of affiliation at the time of publication using DSpace facet data.
            5. Compares both units (API vs guessed) to determine concordance.
            6. Chooses the most reliable unit as final_mainunit (priority: guessed over API).

            Args:
                row (pd.Series): A row from the DataFrame representing one author/publication entry.

            Returns:
                dict: Enriched author information, including:
                    - sciper_id, epfl_orcid, epfl_status, epfl_position
                    - epfl_api_mainunit_id / name / type
                    - dspace_uuid
                    - guessing_mainunit
                    - mainunit_match (bool)
                    - final_mainunit (str)
            """
            key = make_cache_key(row)
            if key in cache:
                self.logger.debug("Returned cached data for key %s", key)
                return cache[key]

            # Initialize result container
            result = {
                "sciper_id": None,
                "epfl_status": None,
                "epfl_position": None,
                "epfl_orcid": None,
                "epfl_api_mainunit_id": None,
                "epfl_api_mainunit_name": None,
                "epfl_api_mainunit_type": None,
                "dspace_uuid": None,
                "guessing_mainunit": None,
                "mainunit_match": None,
                "final_mainunit": None,
            }

            # Step 1: Query DSpace for sciper and uuid
            uuid, dspace_sciper = get_dspace_data(row)
            result["dspace_uuid"] = uuid
            sciper_id = dspace_sciper or row.get("sciper_id")

            # Step 2: Query EPFL API (by sciper if possible, fallback to name)
            if sciper_id:
                person_info = ApiEpflClient.query_person(
                    query=sciper_id, format="digest", use_firstname_lastname=False
                )
            else:
                firstname = clean_value(row.get("nameparse_firstname", ""))
                lastname = clean_value(row.get("nameparse_lastname", ""))
                person_info = ApiEpflClient.query_person(
                    query=row.get("author_cleaned"),
                    firstname=firstname,
                    lastname=lastname,
                    format="digest",
                    use_firstname_lastname=True,
                )

            # Step 3: Populate EPFL metadata if available
            if isinstance(person_info, dict):
                result.update({
                    "sciper_id": person_info.get("sciper_id"),
                    "epfl_orcid": person_info.get("epfl_orcid"),
                    "epfl_status": person_info.get("epfl_status"),
                    "epfl_position": person_info.get("epfl_position"),
                })

                if person_info.get("sciper_id"):
                    uid, uname, utype = self._fetch_accred_info(person_info["sciper_id"])
                    result.update({
                        "epfl_api_mainunit_id": uid,
                        "epfl_api_mainunit_name": uname,
                        "epfl_api_mainunit_type": utype,
                    })

            # Step 4: Guess unit at publication date from DSpace facets
            year = row.get("year")
            if result.get("sciper_id") and year:
                guessed_unit = self._infer_unit_from_dspace_facets(result["sciper_id"], year)
                result["guessing_mainunit"] = guessed_unit

                api_unit = result.get("epfl_api_mainunit_name")
                if guessed_unit:
                    if api_unit:
                        result["mainunit_match"] = api_unit.strip().upper() == guessed_unit.strip().upper()
                    else:
                        result["mainunit_match"] = False

            # Step 5: Select final_mainunit based on best guess at time of publication
            current_year = datetime.now().year
            publication_year = row.get("year")

            # Convert and compare publication year safely
            try:
                publication_year = int(publication_year)
            except (ValueError, TypeError):
                publication_year = None

            # Prioritize based on year
            if publication_year and abs(publication_year - current_year) <= 1:
                result["final_mainunit"] = (
                    result["epfl_api_mainunit_name"] or result["guessing_mainunit"]
                )
            else:
                result["final_mainunit"] = (
                    result["guessing_mainunit"] or result["epfl_api_mainunit_name"]
                )

            cache[key] = result
            return result

        enrichment_df = self.df.apply(query_and_enrich_person, axis=1, result_type="expand")
        self.df = pd.concat([self.df, enrichment_df], axis=1)

        return self.df if return_df else self

class PublicationProcessor: 

    def __init__(self, df, unpaywall_format="best-oa-location"):
        self.df = df
        self.unpaywall_format = unpaywall_format 
        log_file_path = os.path.join(logs_dir, "logging.log")
        self.logger = manage_logger(log_file_path)

    def fetch_unpaywall_data(self, doi):
        return UnpaywallClient.fetch_by_doi(doi, format=self.unpaywall_format)

    def process(self, return_df=True):
        self.df = self.df.copy()
        self.df["upw_is_oa"] = False
        self.df["upw_is_oa"] = self.df["upw_is_oa"].astype("boolean")  # Nullable boolean

        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.fetch_unpaywall_data, self.df["doi"].dropna()))

        for index, result in zip(self.df.index, results):
            if result is not None:
                self.df.at[index, "upw_is_oa"] = bool(result.get("is_oa"))
                self.df.at[index, "upw_oa_status"] = result.get("oa_status")
                self.df.at[index, "journal_is_oa"] = result.get("journal_is_oa")
                self.df.at[index, "journal_is_in_doaj"] = result.get("journal_is_in_doaj")
                self.df.at[index, "upw_license"] = result.get("license")
                self.df.at[index, "upw_version"] = result.get("version")
                self.df.at[index, "upw_host"] = result.get("host_type")
                self.df.at[index, "upw_oai_id"] = result.get("pmh_id")


                if self.unpaywall_format == "best-oa-location":
                    self.df.at[index, "upw_pdf_urls"] = result.get("pdf_urls")
                    self.df.at[index, "upw_valid_pdf"] = result.get("valid_pdf")
                else:
                    self.df.at[index, "upw_pdf_urls"] = None
                    self.df.at[index, "upw_valid_pdf"] = None
            else:
                self.df.at[index, "upw_is_oa"] = pd.NA
                self.df.at[index, "upw_oa_status"] = None
                self.df.at[index, "journal_is_oa"] = None
                self.df.at[index, "journal_is_in_doaj"] = None
                self.df.at[index, "upw_license"] = None
                self.df.at[index, "upw_version"] = None
                self.df.at[index, "upw_host"] = None
                self.df.at[index, "upw_oai_id"] = None
                self.df.at[index, "upw_pdf_urls"] = None
                self.df.at[index, "upw_valid_pdf"] = None
                self.logger.warning(
                    "No unpaywall data returned for DOI %s.", self.df.at[index, "doi"]
                )

        return self.df if return_df else self

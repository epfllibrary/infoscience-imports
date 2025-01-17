"""Metadata enrichment: processors for authors and publications"""

import string
import time
import re
import os

import pandas as pd
from fuzzywuzzy import fuzz, process
import nameparser
from nameparser import HumanName
from utils import manage_logger, remove_accents, clean_value

from clients.api_epfl_client import ApiEpflClient
from clients.unpaywall_client import UnpaywallClient
from clients.dspace_client_wrapper import DSpaceClientWrapper
from clients.services_istex_client import ServicesIstexClient
from clients.orcid_client import OrcidClient
from config import scopus_epfl_afids, unit_types
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
          _process_scopus(text): Processes the organization text for Scopus publications to check for EPFL affiliations.
          _process_wos(text): Processes the organization text for WOS publications to check for EPFL affiliations.
    Usage
    processor = Processor(your_dataframe)
    processor.process().nameparse_authors().services_istex_orcid_reconciliation().orcid_data_reconciliation()
    """
    def __init__(self, df):
        self.df = df

        log_file_path = os.path.join(logs_dir, "enriching_authors.log")
        self.logger = manage_logger(log_file_path)

    def process(self, return_df=False):

        self.df = self.df.copy()
        for index, row in self.df.iterrows():
            if row['source'] == 'scopus':
                self.df.at[index, 'epfl_affiliation'] = self._process_scopus(row['organizations'])
            elif row['source'] == 'wos':
                self.df.at[index, 'epfl_affiliation'] = self._process_wos(row['organizations'])
            elif row['source'] == 'openalex':
                self.df.at[index, 'epfl_affiliation'] = self._process_openalex(row['organizations'])    
            elif row['source'] == 'zenodo':
                self.df.at[index, 'epfl_affiliation'] = self._process_zenodo(row['organizations'])
            else:
                # Default to False for unknown sources
                self.logger.error(f"Unknown source: {row['source']}")
                self.df.at[index, 'epfl_affiliation'] = False
        return self.df if return_df else self

    def _process_scopus(self, text, check_all=False):
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

    def _process_zenodo(self, text):
        if not isinstance(text, str):
            return False
        pattern = "(?:EPFL|[Pp]olytechnique [Ff].d.rale de Lausanne)"
        return bool(re.search(pattern, text))

    def _process_openalex(self, text):
        if not isinstance(text, str):
            return False
        pattern = r"(02s376052|EPFL|École Polytechnique Fédérale de Lausanne)"
        return bool(re.search(pattern, text, re.IGNORECASE))

    def _process_wos(self, text):
        keywords = ["EPFL", "Ecole Polytechnique Federale de Lausanne"]
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

    # def clean_authors(self, return_df=False):
    #     self.df = self.df.copy()  # Create a copy of the DataFrame if necessary

    #     # Function to clean author names
    #     def clean_author(author):
    #         author = author.lower()
    #         author = author.translate(
    #             str.maketrans(string.punctuation, " " * len(string.punctuation))
    #         )
    #         author = remove_accents(author)
    #         author = author.encode("ascii", "ignore").decode("utf-8")
    #         return author

    #     # Apply the cleaning function to the 'authors' column
    #     self.df["author_cleaned"] = self.df["author"].apply(clean_author)

    #     return self.df if return_df else self

    def clean_authors(self, return_df=False):
        self.df = self.df.copy()

        def clean_author(author):
            parsed_name = HumanName(author)

            formatted_name = (
                f"{parsed_name.last} {parsed_name.first} {parsed_name.middle} ".strip()
            )

            formatted_name = formatted_name.translate(
                str.maketrans("", "", string.punctuation)
            )
            formatted_name = clean_value(formatted_name)
            formatted_name = remove_accents(formatted_name)
            formatted_name = formatted_name.encode("ascii", "ignore").decode("utf-8")
            formatted_name = " ".join(formatted_name.split())

            return formatted_name

        # Appliquer la fonction de nettoyage à la colonne 'author'
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

    def _query_dspace_authority(self, query):
        """
        Attempts to retrieve author information from DSpace authority service.
        If successful, returns sciper_id, epfl_api_mainunit_name, and dspace_uuid;
        otherwise, returns None values.
        """

        try:
            response = dspace_wrapper.search_authority(filter_text=query)
            self.logger.info(f"Querying DSpace for author {query}")
            sciper_id = dspace_wrapper.get_sciper_from_authority(response)
            self.logger.info(f"Sciper {sciper_id} was retrieved in DSpace for author {query}")
            return sciper_id
        except Exception as e:
            self.logger.error(
                f"Error querying DSpace for author {query} - {e}"
            )

        return None    

    def api_epfl_reconciliation(self, return_df=False):
        self.df = self.df.copy()  # Create a copy of the DataFrame if necessary

        def query_person(row):
            # Attempt to retrieve author using ORICD first
            orcid_id = row.get("orcid_id")
            if orcid_id and pd.notna(orcid_id):
                sciper_id = self._query_dspace_authority(orcid_id)
                if sciper_id:
                    return sciper_id

            # Attempt to retrieve author info from DSpace by name
            # Construct the query from the 'author_cleaned' column
            query = row.get("author_cleaned")
            if query and pd.notna(query):
                sciper_id = self._query_dspace_authority(query)
                if sciper_id:
                    return sciper_id

            firstname = row["nameparse_firstname"]
            if firstname:
                firstname = clean_value(firstname)

            lastname = row["nameparse_lastname"]
            if lastname:
                lastname = clean_value(lastname)

            # Call the query_person method with the appropriate parameters
            return ApiEpflClient.query_person(
                query=query,
                firstname=firstname,
                lastname=lastname,
                format="sciper",
                use_firstname_lastname=True,
            )

        # Query the ApiEpflClient for each cleaned author and store the sciper_id
        # Pass the entire row to the query_person function

        self.df["sciper_id"] = self.df.apply(query_person, axis=1)

        # Optionally return the modified DataFrame if required
        if return_df:
            return self.df

        # Function to fetch accreditation info and store in new columns
        def fetch_accred_info(sciper_id):
            """
            Fetch accreditation information for a person based on specific priority rules.

            Args:
                sciper_id (str): The unique identifier of the person.
                unit_types (list): List of allowed unit_types.

            Returns:
                tuple: (unit_id, unit_name) or (None, None) if no result is found.
            """
            if pd.notna(sciper_id):
                records = ApiEpflClient.fetch_accred_by_unique_id(sciper_id, format="digest")
                self.logger.debug(f"Person record: {records}")

                if isinstance(records, list) and records:
                    # Step 1: Initialize variables to track results for each condition
                    prioritized_unit = None  # For unit_order == 1 and allowed unit_type
                    allowed_units = []       # Collect allowed units for priority sorting
                    fallback_unit = None     # For unit_order == 1 regardless of unit_type

                    # Iterate through records and classify them
                    for record in records:
                        unit_order = record.get('unit_order')
                        unit_type = record.get('unit_type')
                        unit_id = record.get('unit_id')
                        unit_name = record.get('unit_name')

                        if unit_order == 1 and unit_type in unit_types and not prioritized_unit:
                            prioritized_unit = (unit_id, unit_name)

                        if unit_type in unit_types:
                            allowed_units.append((unit_id, unit_name, unit_type))

                        if unit_order == 1 and not fallback_unit:
                            fallback_unit = (unit_id, unit_name)

                    # Step 2: Return the highest priority result available
                    if prioritized_unit:
                        self.logger.debug(f"Main unit retrieved: {prioritized_unit}")
                        return prioritized_unit

                    if allowed_units:
                        # Sort allowed units by the priority in unit_types
                        allowed_units.sort(key=lambda x: unit_types.index(x[2]))
                        self.logger.debug(
                            f"Main unit retrieved: {allowed_units[0][:2]}"
                        )

                        return allowed_units[0][:2]  # Return (unit_id, unit_name)

                    if fallback_unit:
                        self.logger.debug(f"Main unit retrieved: {fallback_unit}")
                        return fallback_unit

                    # Step 3: If no conditions above apply, return the first record in the list
                    self.logger.warning("No authorized unit type found. Returning the first record.")
                    first_record = records[0]
                    return first_record['unit_id'], first_record['unit_name']

            return None, None

        # Request ApiEpflClient.fetch_accred_by_unique_id for each row with a non-null sciper_id
        self.df[['epfl_api_mainunit_id', 'epfl_api_mainunit_name']] = self.df['sciper_id'].apply(
            lambda sciper_id: fetch_accred_info(sciper_id)
        ).apply(pd.Series)

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
            and returns the UUID and associated sciper_id if found.

            Parameters:
            - row (pd.Series): A row from the DataFrame containing the necessary fields.

            Returns:
            - tuple: A tuple containing the UUID and sciper_id, or (None, None) if no result is found.
            """

            queries = []  # List to store queries for the DSpace database

            # Only generate query if the column exists in the row and the value is not NaN
            if "sciper_id" in row and pd.notna(row.get("sciper_id")):
                queries.append(f"epfl.sciperId:({row['sciper_id']})")

            if "orcid_id" in row and pd.notna(row.get("orcid_id")):
                queries.append(f"person.identifier.orcid:({row['orcid_id']})")

            if "author" in row and pd.notna(row.get("author")):
                queries.append(f"itemauthoritylookup:\"{row['author'].replace(',', '')}\"")

            # Iterate through each query and execute if valid
            for query in queries:
                if query:  # Ensure the query is not None
                    result = dspace_wrapper.find_person(
                        query=query
                    )  # Call the dspace_wrapper to search
                    if result:  # If a result is found, return the UUID and sciper_id
                        return result["uuid"], result["sciper_id"], result["main_affiliation"]

            return None, None, None  # Return None if no result is found for any of the queries

        def update_row(row):
            """
            Updates the row with the DSpace UUID and potentially the sciper_id.

            Parameters:
            - row (pd.Series): A row from the DataFrame to be updated.

            Returns:
            - pd.Series: The updated row with the DSpace UUID and sciper_id if found.
            """

            # Get the DSpace UUID and associated sciper_id for the row
            uuid, found_sciper_id, main_affiliation = get_dspace_data(row)

            # If the sciper_id is empty and a sciper_id is found, update the column
            if pd.isna(row.get("sciper_id")) and found_sciper_id:
                row["sciper_id"] = found_sciper_id

            if pd.isna(row.get("epfl_api_mainunit_name")) and main_affiliation:
                row["epfl_api_mainunit_name"] = main_affiliation

            # Update the row with the DSpace UUID
            row["dspace_uuid"] = uuid
            return row

        # Apply the update_row function to each row in the DataFrame
        self.df = self.df.apply(update_row, axis=1)

        # Return either the modified DataFrame or the instance based on the return_df flag
        return self.df if return_df else self

    ##### Inutilisé #####################
    def services_istex_orcid_reconciliation(self, return_df=False):
        def fetch_orcid(row):
            # Request ORCID ID without condition
            orcid_id = ServicesIstexClient.get_orcid_id(firstname=row['nameparse_firstname'],
                                                        lastname=row['nameparse_lastname'])
            time.sleep(5)

            # Update 'orcid_id' if the returned value is not None and the original is empty
            if orcid_id is not None and pd.isna(row['orcid_id']):
                return orcid_id
            return row['orcid_id']  # Return the existing value if it's not empty

        # Fill the 'orcid_id' column with the fetched ORCID IDs where applicable
        self.df['orcid_id'] = self.df.apply(fetch_orcid, axis=1)
        return self.df if return_df else self

    ##### Inutilisé #####################
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
        log_file_path = os.path.join(logs_dir, "enriching_publications.log")
        self.logger = manage_logger(log_file_path)

    def process(self, return_df=True):
        self.df = self.df.copy()

        for index, row in self.df.iterrows():
            if pd.notna(row['doi']):
                unpaywall_data = UnpaywallClient.fetch_by_doi(row['doi'], format="best-oa-location")
                if unpaywall_data is not None:
                    self.df.at[index, 'upw_is_oa'] = unpaywall_data.get('is_oa')
                    self.df.at[index, 'upw_oa_status'] = unpaywall_data.get('oa_status')
                    self.df.at[index, "upw_license"] = unpaywall_data.get("license")
                    self.df.at[index, "upw_version"] = unpaywall_data.get("version")
                    self.df.at[index, 'upw_pdf_urls'] = unpaywall_data.get('pdf_urls')
                    self.df.at[index, "upw_valid_pdf"] = unpaywall_data.get("valid_pdf")
                else:
                    self.logger.warning(f"No unpaywall data returned for DOI {row['doi']}.")
        return self.df if return_df else self

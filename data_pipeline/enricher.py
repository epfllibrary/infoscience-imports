import pandas as pd
from fuzzywuzzy import fuzz, process
import nameparser
import string
from clients.api_epfl_client import ApiEpflClient
from clients.unpaywall_client import UnpaywallClient
from clients.dspace_client_wrapper import DSpaceClientWrapper
from clients.services_istex_client import ServicesIstexClient
from clients.orcid_client import OrcidClient
import time
import re

from config import scopus_epfl_afids, unit_types
from utils import manage_logger

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
        self.logger = manage_logger("./logs/enriching_authors.log") 

    def process(self, return_df=False):

        self.df = self.df.copy()

        for index, row in self.df.iterrows():
            if row['source'] == 'scopus':
                self.df.at[index, 'epfl_affiliation'] = self._process_scopus(row['organizations'])
            elif row['source'] == 'wos':
                self.df.at[index, 'epfl_affiliation'] = self._process_wos(row['organizations'])
            elif row['source'] == 'zenodo':
                self.df.at[index, 'epfl_affiliation'] = self._process_zenodo(row['organizations'])
            else:
                # Default to False for unknown sources
                print(f"Unknown source: {row['source']}")
                self.df.at[index, 'epfl_affiliation'] = False
        return self.df if return_df else self

    def _process_scopus(self, text):
        return any(value in text for value in scopus_epfl_afids)

    def _process_zenodo(self, text):
        if text is None:
            return False
        else:
            pattern = "(?:EPFL|[Pp]olytechnique [Ff].d.rale de Lausanne)"
            return bool(re.search(pattern, text))

    def _process_wos(self, text):
        keywords = ["EPFL", "Ecole Polytechnique Federale de Lausanne"]
        return any(process.extractOne(keyword, [text], scorer=fuzz.partial_ratio)[1] >= 80 for keyword in keywords)

    def filter_epfl_authors(self, return_df=False):
        self.df = self.df.copy()

        self.df = self.df[self.df['epfl_affiliation']]
        return self.df if return_df else self

    def clean_authors(self, return_df=False):

        self.df = self.df.copy()  # Create a copy of the DataFrame if necessary

        # Function to clean author names
        def clean_author(author):
            author = author.lower()  # Convert to lowercase
            author = author.translate(str.maketrans('', '', string.punctuation))  # Remove punctuation
            author = author.encode('ascii', 'ignore').decode('utf-8')  # Remove accents
            return author

        # Apply the cleaning function to the 'authors' column
        self.df['author_cleaned'] = self.df['author'].apply(clean_author)

        return self.df if return_df else self

    def nameparse_authors(self, return_df=False):
        parser = nameparser.HumanName
        self.df = self.df.copy()  # Create a copy of the DataFrame if necessary
        self.df.loc[:, 'nameparse_firstname'] = self.df.apply(
            lambda row: parser(row['author']).first if row['epfl_affiliation'] else None, axis=1
        )
        self.df.loc[:, 'nameparse_lastname'] = self.df.apply(
            lambda row: parser(row['author']).last if row['epfl_affiliation'] else None, axis=1
        )
        return self.df if return_df else self

    def api_epfl_reconciliation(self, return_df=False):

        self.df = self.df.copy()  # Create a copy of the DataFrame if necessary

        def query_person(row):
            # Construct the query from the 'author_cleaned' column
            query = row['author_cleaned']
            firstname = row['nameparse_firstname']
            lastname = row['nameparse_lastname']

            # Call the query_person method with the appropriate parameters
            return ApiEpflClient.query_person(
                query=query,
                firstname=firstname,
                lastname=lastname,
                format="sciper",
                use_firstname_lastname=True
            )

        # Query the ApiEpflClient for each cleaned author and store the sciper_id
        self.df.loc[:, 'sciper_id'] = self.df['author_cleaned'].apply(ApiEpflClient.query_person)

        # Function to fetch accreditation info and store in new columns
        def fetch_accred_info(sciper_id):
            if pd.notna(sciper_id):
                records = ApiEpflClient.fetch_accred_by_unique_id(sciper_id, format="digest")
                if isinstance(records, list):
                    for record in records:
                        if record.get('unit_type') in unit_types:
                            return record['unit_id'], record['unit_name']
                   
                    # Si aucun type d'unité autorisé n'est trouvé, retourner le premier enregistrement
                    self.logger.warning("No authorized unit type found. Returning the first record.")
                    first_record = records[0]  # Obtenir le premier enregistrement
                    return first_record['unit_id'], first_record['unit_name']

            return None, None

        # Request ApiEpflClient.fetch_accred_by_unique_id for each row with a non-null sciper_id
        self.df[['epfl_api_mainunit_id', 'epfl_api_mainunit_name']] = self.df['sciper_id'].apply(
            lambda sciper_id: fetch_accred_info(sciper_id)
        ).apply(pd.Series)

        return self.df if return_df else self

    def generate_dspace_uuid(self, return_df=False):
        self.df = self.df.copy()
        dspace_wrapper = DSpaceClientWrapper()
        self.df['dspace_uuid'] = self.df.apply(
            lambda row: dspace_wrapper.find_person(
                query=f"(epfl.sciperId:{row['sciper_id']})" if pd.notna(row['sciper_id']) else f"(title:{row['author_cleaned']})"
            ),
            axis=1
        )
        return self.df if return_df else self

    ##### Inutilisé #####################
    def services_istex_orcid_reconciliation(self, return_df=False):
        def fetch_orcid(row):
            # Request ORCID ID without condition
            orcid_id = ServicesIstexClient.get_orcid_id(firstname=row['nameparse_firstname'], lastname=row['nameparse_lastname'])
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
        self.logger = manage_logger("./logs/enriching_publications.log")

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

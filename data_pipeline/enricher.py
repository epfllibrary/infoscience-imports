import pandas as pd
from fuzzywuzzy import fuzz, process
import string
from clients.api_epfl_client import ApiEpflClient
import nameparser
from clients.services_istex_client import ServicesIstexClient
from clients.orcid_client import OrcidClient

import time
import mappings
import logging


class Processor:
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

    def process(self, return_df=False):
        
        self.df = self.df.copy()
        
        for index, row in self.df.iterrows():
            if row['source'] == 'scopus':
                self.df.at[index, 'epfl_affiliation'] = self._process_scopus(row['organizations'])
            elif row['source'] == 'wos':
                self.df.at[index, 'epfl_affiliation'] = self._process_wos(row['organizations'])
            else:
                print(f"Unknown source: {row['source']}")
                self.df.at[index, 'epfl_affiliation'] = False  # Default to False for unknown sources
        return self.df if return_df else self
    
    def _process_scopus(self, text):
        scopus_epfl_afids = mappings.scopus_epfl_afids
        return any(value in text for value in scopus_epfl_afids)

    def _process_wos(self, text):
        keywords = ["EPFL", "Ecole Polytechnique Federale de Lausanne"]
        return any(process.extractOne(keyword, [text], scorer=fuzz.partial_ratio)[1] >= 80 for keyword in keywords)
    
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
    
    def api_epfl_reconciliation(self, return_df=False):
        
        self.df = self.df.copy()  # Create a copy of the DataFrame if necessary
        
        # Query the ApiEpflClient for each cleaned author and store the sciper_id
        self.df.loc[:, 'sciper_id_by_fullname'] = self.df['author_cleaned'].apply(ApiEpflClient.query_person)
        
        # Function to fetch accreditation info and store in new columns
        def fetch_accred_info(sciper_id):
            if pd.notna(sciper_id):
                record = ApiEpflClient.fetch_accred_by_unique_id(sciper_id, format="mainUnit")
                if isinstance(record, dict) and 'unit_id' in record and 'unit_name' in record:
                    return record['unit_id'], record['unit_name']
            return None, None

        # Request ApiEpflClient.fetch_accred_by_unique_id for each row with a non-null sciper_id
        self.df[['epfl_api_mainunit_id', 'epfl_api_mainunit_name']] = self.df['sciper_id_by_fullname'].apply(
            lambda sciper_id: fetch_accred_info(sciper_id)
        ).apply(pd.Series)
        
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
    
    def services_istex_orcid_reconciliation(self, return_df=False):
        def fetch_orcid(row):
            if row['epfl_affiliation'] and 'api.epfl.ch' in row['sciper_id_by_fullname']:
                orcid_id =  ServicesIstexClient.get_orcid_id(firstName=row['nameparse_firstname'], lastName=row['nameparse_lastname'])
                time.sleep(5)
                return orcid_id
            return None

        self.df['orcid_orcid_id'] = self.df.apply(fetch_orcid, axis=1)
        return self.df if return_df else self
    
    def orcid_data_reconciliation(self, return_df=False):
        
        self.df = self.df.copy()
        
        for index, row in self.df.iterrows():
            orcid_id = row['orcid_id'] if pd.notna(row['orcid_id']) else row['orcid_orcid_id']
            if orcid_id is not None:
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
        
        
        
        
        

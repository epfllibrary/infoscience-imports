import pandas as pd
from fuzzywuzzy import fuzz
from mappings import source_order
from clients.dspace_client_wrapper import DSpaceClientWrapper
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

class DataFrameProcessor:
    def __init__(self, *dfs):
        self.dataframes = dfs
        self.logger = logging.getLogger(__name__)
        
    def _generate_unique_id(self, row, existing_ids):
        if pd.notna(row['doi']):
            return row['doi']
        
        title_pubyear = row['title'] + str(row['pubyear'])    
        # Check for fuzzy matches against existing IDs
        for existing_id in existing_ids:
            if fuzz.token_set_ratio(title_pubyear, existing_id) > 80:  # Adjust threshold as needed
                logging.info(
                 f"Déjà présente dans Infoscience {existing_ids}"
                )
                return existing_id  # Return the existing DOI if a match is found
    
        return title_pubyear  # Return the concatenated title and pubyear if no match is found

    def deduplicate_dataframes(self):
        """
        Deduplicate the source dataframes.
        """
        # Combine the input dataframes into one
        combined_df = pd.concat(self.dataframes, ignore_index=True)
        # Create a unique identifier for rows where 'doi' is missing
        existing_ids = []
        combined_df['unique_id'] = combined_df.apply(
             lambda row: self._generate_unique_id(row, existing_ids), axis=1 
        )        
        
        # Sort the combined dataframe to prioritize 'scopus' and 'wos' sources in case of duplicates
        combined_df['source'] = pd.Categorical(combined_df['source'], categories=source_order, ordered=True)
        combined_df.sort_values(by=['unique_id', 'source'], ascending=[True, True], inplace=True)
        
        # Drop duplicates based on the 'unique_id' column, keeping the first occurrence
        deduplicated_df = combined_df.drop_duplicates(subset='unique_id', keep='first')
        
        # Drop the helper column 'unique_id'    
        deduplicated_df = deduplicated_df.copy()  # Create a copy to avoid SettingWithCopyWarning
        deduplicated_df.drop(columns=['unique_id'], inplace=True)  # This modifies deduplicated_df in place
        
        return deduplicated_df
    
    def deduplicate_infoscience(self,df):
        """
        Deduplicate on existing Infoscience publications.
        """
        self.logger.info(f"- Processus de dédoublonnage avec Infoscience")
        wrapper = DSpaceClientWrapper()
        
        # Apply the DSpaceClientWrapper.find_publication_duplicate function to each row
        df['is_duplicate'] = df.apply(
            lambda row: wrapper.find_publication_duplicate(row),
            axis=1
        )
        #df.to_csv("test.csv", index=False,encoding="utf-8")
        # Filter the dataframe to keep only rows where 'is_duplicate' is False
        filtered_df = df[df['is_duplicate'] == False].drop(columns=['is_duplicate'])
        # keep the unloaded duplicates for memory
        duplicates_df = df[df['is_duplicate'] == True].drop(columns=['is_duplicate'])
        return filtered_df, duplicates_df
        
    def generate_main_dataframes(self, df):
        # Step 1: Add an incremental row_id to the DataFrame
        df['row_id'] = range(1, len(df) + 1)
        new_rows = []

        # Iterate through each row in the DataFrame
        for _, row in df.iterrows():
            row_id = row['row_id']
            source = row['source']
            authors = row['authors']

            for author_data in authors:
                new_row = {
                    'row_id': row_id,
                    'source': source,
                    'author': author_data.get('author', None),
                    'orcid_id': author_data.get('orcid_id', None),
                    'internal_author_id': author_data.get('internal_author_id', None),
                    'organizations': author_data.get('organizations', None),
                    'suborganization': author_data.get('suborganization', None)
                }
                new_rows.append(new_row)

        df_authors = pd.DataFrame(new_rows)
        df_metadata = df.drop(columns=['authors'])
        
        return df_metadata, df_authors
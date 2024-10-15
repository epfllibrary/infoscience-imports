import pandas as pd
from fuzzywuzzy import fuzz
from mappings import source_order
from clients.dspace_client_wrapper import DSpaceClientWrapper
from utils import manage_logger
import re
import string


class DataFrameProcessor:
    def __init__(self, *dfs):
        self.dataframes = dfs
        self.logger = manage_logger("./logs/deduplicate.log")

    def clean_title(title):
        # Remove HTML tags
        title = re.sub(r"<[^>]+>", "", title)
        # Replace non-alphanumeric characters (excluding whitespace) with spaces
        title = re.sub(r"[^\w\s]", " ", title)
        # Reduce multiple spaces to a single space and strip leading/trailing spaces
        title = re.sub(r"\s+", " ", title).strip()
        # Normalize title by converting to lowercase and removing punctuation
        title = title.lower()
        title = title.translate(str.maketrans("", "", string.punctuation))
        return title    

    def _generate_unique_ids(self, row, existing_ids):
        title = row["title"]
        pubyear = row["pubyear"]

        # Remove HTML tags
        title = re.sub(r"<[^>]+>", "", title)

        # Replace non-alphanumeric characters (excluding whitespace) with spaces
        title = re.sub(r"[^\w\s]", " ", title)

        # Reduce multiple spaces to a single space and strip leading/trailing spaces
        title = re.sub(r"\s+", " ", title).strip()

        # Normalize title by converting to lowercase and removing punctuation
        title = title.lower()
        title = title.translate(str.maketrans("", "", string.punctuation))

        # Generate unique IDs based on DOI and title+pubyear
        doi_id = row["doi"] if pd.notna(row["doi"]) else None
        title_pubyear_id = title + str(pubyear)

        # Check for fuzzy matches against existing IDs for title+pubyear
        for existing_id, existing_pubyear in existing_ids:
            if (
                fuzz.token_set_ratio(title, existing_id) > 85
                and abs(pubyear - existing_pubyear) <= 1
            ):
                return doi_id if doi_id else title_pubyear_id

        # Return a tuple of both IDs
        return doi_id, title_pubyear_id

    def deduplicate_dataframes(self):
        """
        Deduplicate the source dataframes.
        """
        # Combine the input dataframes into one
        combined_df = pd.concat(self.dataframes, ignore_index=True)

        # Create a unique identifier for rows
        existing_ids = []
        combined_df["dedup_keys"] = combined_df.apply(
            lambda row: self._generate_unique_ids(row, existing_ids), axis=1
        )

        # Unpack the unique_id tuple
        combined_df[["doi_id", "title_pubyear_id"]] = pd.DataFrame(
            combined_df["dedup_keys"].tolist(), index=combined_df.index
        )

        # Drop the helper column 'unique_id'
        combined_df.drop(columns=["dedup_keys"], inplace=True)

        # Sort the combined dataframe to prioritize 'scopus' and 'wos' sources in case of duplicates
        source_order = ["wos", "scopus"]  # Define the source order if not already done
        combined_df["source"] = pd.Categorical(
            combined_df["source"], categories=source_order, ordered=True
        )
        combined_df.sort_values(
            by=["doi_id", "title_pubyear_id", "source"],
            ascending=[True, True, True],
            inplace=True,
        )

        # Drop duplicates based on 'doi_id', keeping the first occurrence
        deduplicated_df = combined_df.drop_duplicates(
            subset=["doi_id"], keep="first"
        )

        # Drop duplicates based on 'title_pubyear_id', keeping the first occurrence
        deduplicated_df = deduplicated_df.drop_duplicates(
            subset=["title_pubyear_id"], keep="first"
        )

        # Drop the helper columns
        deduplicated_df = (
            deduplicated_df.copy()
        )  # Create a copy to avoid SettingWithCopyWarning
        deduplicated_df.drop(columns=["doi_id", "title_pubyear_id"], inplace=True)

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
        # df.to_csv("test.csv", index=False,encoding="utf-8")
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

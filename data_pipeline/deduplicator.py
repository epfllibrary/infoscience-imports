import re
import string
import os

import pandas as pd
from fuzzywuzzy import fuzz
from config import source_order
from clients.dspace_client_wrapper import DSpaceClientWrapper
from utils import manage_logger
from config import logs_dir


class DataFrameProcessor:
    def __init__(self, *dfs):
        self.dataframes = dfs
        log_file_path = os.path.join(logs_dir, "deduplicate.log")
        self.logger = manage_logger(log_file_path)

    def clean_title(self, title):
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
        Deduplicate the source dataframes, retaining the 'authors' column from the line to keep.
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

        # Drop the helper column 'dedup_keys'
        combined_df.drop(columns=["dedup_keys"], inplace=True)

        # Sort the combined dataframe to prioritize 'scopus' and 'wos' sources in case of duplicates
        combined_df["source"] = pd.Categorical(
            combined_df["source"], categories=source_order, ordered=True
        )
        combined_df.sort_values(
            by=["doi_id", "title_pubyear_id", "source"],
            ascending=[True, True, True],
            inplace=True,
        )

        # Define a function to merge complementary information
        def merge_complementary_info(group):
            # Start with the first row as the base (prioritized by sorting)
            base_row = group.iloc[0].copy()

            # Keep the 'authors' column from the first row
            base_authors = base_row.get("authors", None)

            # Iterate through other rows and fill missing information in the base row
            for _, row in group.iloc[1:].iterrows():
                for col in group.columns:
                    if col == "authors":  # Skip 'authors' column for merging
                        continue
                    # Check if the current cell in the base row is empty
                    if pd.isna(base_row[col]) or base_row[col] in [None, ""]:
                        # Only update if the current row has a non-empty value
                        if not pd.isna(row[col]) and row[col] not in [None, ""]:
                            base_row[col] = row[col]

            # Restore the original 'authors' from the prioritized row
            if "authors" in group.columns:
                base_row["authors"] = base_authors

            return base_row

        # Process duplicates based on 'doi_id'
        deduplicated_df = (
            combined_df.groupby("doi_id", as_index=False)
            .apply(merge_complementary_info)
            .reset_index(drop=True)
        )

        # Process duplicates based on 'title_pubyear_id'
        deduplicated_df = (
            deduplicated_df.groupby("title_pubyear_id", as_index=False)
            .apply(merge_complementary_info)
            .reset_index(drop=True)
        )

        # Drop the helper columns
        deduplicated_df.drop(columns=["doi_id", "title_pubyear_id"], inplace=True)

        return deduplicated_df

    def deduplicate_infoscience(self, df):
        """
        Deduplicate on existing Infoscience publications.
        """
        self.logger.info(f"- Processus de dédoublonnage avec Infoscience")
        wrapper = DSpaceClientWrapper()

        # Apply the DSpaceClientWrapper.find_publication_duplicate function to each row
        df["is_duplicate"] = df.apply(
            lambda row: wrapper.find_publication_duplicate(row), axis=1
        )
        # df.to_csv("test.csv", index=False,encoding="utf-8")
        # Filter the dataframe to keep only rows where 'is_duplicate' is False
        filtered_df = df[df["is_duplicate"] == False].drop(columns=["is_duplicate"])
        # keep the unloaded duplicates for memory
        duplicates_df = df[df["is_duplicate"] == True].drop(columns=["is_duplicate"])
        return filtered_df, duplicates_df

    def generate_main_dataframes(self, df):
        # Step 1: Add an incremental row_id to the DataFrame
        df["row_id"] = range(1, len(df) + 1)
        new_rows = []

        # Iterate through each row in the DataFrame
        for _, row in df.iterrows():
            row_id = row["row_id"]
            source = row["source"]
            authors = row["authors"]

            for author_data in authors:
                new_row = {
                    "row_id": row_id,
                    "source": source,
                    "author": author_data.get("author", None),
                    "orcid_id": author_data.get("orcid_id", None),
                    "internal_author_id": author_data.get("internal_author_id", None),
                    "organizations": author_data.get("organizations", None),
                    "suborganization": author_data.get("suborganization", None),
                }
                new_rows.append(new_row)

        df_authors = pd.DataFrame(new_rows)
        df_metadata = df.drop(columns=["authors"])

        return df_metadata, df_authors

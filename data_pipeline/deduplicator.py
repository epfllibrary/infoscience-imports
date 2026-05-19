"""Deduplicator processors for harvested publications"""

import json
import re
import string
import os
import pandas as pd
from rapidfuzz import fuzz
from config import logs_dir, source_order
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import classify_record_type
from utils import get_pipeline_logger

logger = get_pipeline_logger("deduplicator")

class DataFrameProcessor:
    def __init__(self, *dfs):
        self.dataframes = dfs
        self.logger = logger

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

    def _generate_unique_ids(self, row):
        """Generate a (doi_id, title_pubyear_id) tuple for a single row.

        The previous implementation accepted an `existing_ids` list intended
        for cross-row fuzzy matching, but that list was never populated inside
        the pandas apply() call (side-effect in apply is not supported).
        The actual deduplication relies on groupby + merge_complementary_info
        downstream, so this method now simply computes the two canonical keys.
        """
        title = row["title"]
        pubyear = row["pubyear"]

        # Normalise title
        title = re.sub(r"<[^>]+>", "", title)
        title = re.sub(r"[^\w\s]", " ", title)
        title = re.sub(r"\s+", " ", title).strip().lower()
        title = title.translate(str.maketrans("", "", string.punctuation))

        doi_val = row.get("doi", pd.NA)
        doi_id = doi_val if (pd.notna(doi_val) and str(doi_val).strip()) else pd.NA
        title_pubyear_id = title + str(pubyear)

        return doi_id, title_pubyear_id

    @staticmethod
    def _merge_complementary_info(group: pd.DataFrame) -> pd.Series:
        """Merge a group of duplicate rows into one, keeping the first row's authors."""
        base_row = group.iloc[0].copy()
        base_authors = base_row.get("authors", None)
        for _, row in group.iloc[1:].iterrows():
            for col in group.columns:
                if col == "authors":
                    continue
                if pd.isna(base_row[col]) or base_row[col] in [None, ""]:
                    if not pd.isna(row[col]) and row[col] not in [None, ""]:
                        base_row[col] = row[col]
        if "authors" in group.columns:
            base_row["authors"] = base_authors
        return base_row

    @staticmethod
    def _groupby_non_empty(df: pd.DataFrame, key: str):
        """Split df into rows with/without a valid value for key."""
        s = df[key]
        mask = s.notna() & s.astype(str).str.strip().ne("")
        return df[mask], df[~mask]

    def _dedup_by_title_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """Type-aware title+year deduplication.

        Rules (applied in priority order):
        - dataset × non-dataset with same title+year → keep both (different entities)
        - preprint × published with same title+year  → keep published only
        - two datasets or two preprints              → normal dedup (keep best source)
        """
        df_with_key, df_no_key = self._groupby_non_empty(df, "title_pubyear_id")
        if df_with_key.empty:
            return df

        result_rows = []
        for _, group in df_with_key.groupby("title_pubyear_id", sort=False):
            if len(group) == 1:
                result_rows.append(group.iloc[0])
                continue

            types = group.apply(classify_record_type, axis=1)
            has_dataset   = (types == "dataset").any()
            has_non_ds    = (types != "dataset").any()
            has_preprint  = (types == "preprint").any()
            has_published = (types == "published").any()

            if has_dataset and has_non_ds:
                # Different entities — dedup within each sub-type separately
                for mask in [types == "dataset", types != "dataset"]:
                    sub = group[mask]
                    if not sub.empty:
                        result_rows.append(self._merge_complementary_info(sub))
            elif has_preprint and has_published:
                # Preprint superseded by published version — keep published
                self.logger.debug(
                    "Cross-source dedup: dropping preprint superseded by published (%s)",
                    group["title_pubyear_id"].iloc[0],
                )
                result_rows.append(
                    self._merge_complementary_info(group[types != "preprint"])
                )
            else:
                result_rows.append(self._merge_complementary_info(group))

        if not result_rows:
            return df_no_key.reset_index(drop=True)

        return pd.concat(
            [pd.DataFrame(result_rows), df_no_key], ignore_index=True
        )

    def deduplicate_dataframes(self):
        """
        Deduplicate the source dataframes, retaining the 'authors' column from the line to keep.
        """
        combined_df = pd.concat(self.dataframes, ignore_index=True)

        combined_df[["doi_id", "title_pubyear_id"]] = pd.DataFrame(
            combined_df.apply(self._generate_unique_ids, axis=1).tolist(),
            index=combined_df.index,
        )
        combined_df["doi_id"] = combined_df["doi_id"].replace(
            {None: pd.NA, "": pd.NA, "None": pd.NA}
        )

        combined_df["source"] = pd.Categorical(
            combined_df["source"], categories=source_order, ordered=True
        )
        combined_df.sort_values(
            by=["doi_id", "title_pubyear_id", "source"],
            ascending=[True, True, True],
            inplace=True,
        )

        # DOI-based dedup (type-agnostic — same DOI = same work)
        df_with_key, df_no_key = self._groupby_non_empty(combined_df, "doi_id")
        if not df_with_key.empty:
            dedup_doi = (
                df_with_key.groupby("doi_id", as_index=False)
                .apply(self._merge_complementary_info)
                .reset_index(drop=True)
            )
            deduplicated_df = pd.concat([dedup_doi, df_no_key], ignore_index=True)
        else:
            deduplicated_df = combined_df.copy()

        # Title+year dedup (type-aware)
        deduplicated_df = self._dedup_by_title_year(deduplicated_df)

        deduplicated_df.drop(columns=["doi_id", "title_pubyear_id"], inplace=True)
        return deduplicated_df

    def deduplicate_infoscience(self, df):
        """Deduplicate against existing Infoscience publications (type-aware).

        Returns ``(filtered_df, duplicates_df)``.
        ``filtered_df`` carries a ``dedup_note`` column (None for most records,
        ``"supersedes_preprint"`` or ``"cross_type_doi"`` for flagged ones).
        """
        self.logger.info("Running Infoscience deduplication (type-aware)")
        wrapper = DSpaceClientWrapper()

        def _check(row):
            is_dup, note, flagged_info = wrapper.find_publication_duplicate_typed(row)
            flagged_pub = json.dumps(flagged_info) if flagged_info else None
            return pd.Series({
                "is_duplicate":      is_dup,
                "dedup_note":        note,
                "flagged_publication": flagged_pub,
            })

        result = df.apply(_check, axis=1)
        result.index = df.index
        df = df.copy()
        df["is_duplicate"]       = result["is_duplicate"]
        df["dedup_note"]         = result["dedup_note"]
        df["flagged_publication"] = result["flagged_publication"]

        filtered_df   = df[df["is_duplicate"] == False].drop(columns=["is_duplicate"]).copy()
        # Keep dedup_note and flagged_publication in duplicates so that discarded
        # preprints (published_version_exists) carry the reference to the existing item.
        duplicates_df = df[df["is_duplicate"] == True].drop(columns=["is_duplicate"]).copy()
        return filtered_df, duplicates_df

    def deduplicate_infoscience_enhanced(self, df):
        self.logger.debug("Falling back to metadata-based duplicate detection")
        wrapper = DSpaceClientWrapper()

        results = df.apply(
            lambda row: wrapper.find_duplicate_enhanced(row), axis=1, result_type="expand"
        )

        # S'assurer que les index sont bien alignés
        results.index = df.index

        # Fusionner proprement
        df_enhanced = pd.concat([df, results], axis=1)

        # Filtrer et retourner les deux sous-ensembles
        filtered_df = (
            df_enhanced[df_enhanced["is_duplicate"] == False]
            .drop(columns=["is_duplicate"])
            .copy()
        )
        duplicates_df = (
            df_enhanced[df_enhanced["is_duplicate"] == True]
            .drop(columns=["is_duplicate"])
            .copy()
        )

        return filtered_df, duplicates_df

    def generate_main_dataframes(self, df):
        # Step 1: Add an incremental row_id to the DataFrame
        df["row_id"] = range(1, len(df) + 1)
        new_rows = []

        # Iterate through each row in the DataFrame
        for _, row in df.iterrows():
            row_id = row["row_id"]
            source = row["source"]
            year = row["pubyear"]
            authors = row["authors"]

            for author_data in authors:
                new_row = {
                    "row_id": row_id,  # Ensure row_id is the first key
                    "source": source,
                    "year": year,
                    "role": author_data.get("role", None),
                    "openalex_is_corresponding": author_data.get(
                        "is_corresponding", None
                    ),
                    "author": author_data.get("author", None),
                    "orcid_id": author_data.get("orcid_id", None),
                    "internal_author_id": author_data.get("internal_author_id", None),
                    "organizations": author_data.get("organizations", None),
                    "suborganization": author_data.get("suborganization", None),
                }
                new_rows.append(new_row)

        df_authors = pd.DataFrame(new_rows)

        # --- Normalisation des organizations UNIQUEMENT pour la dédup ---
        def normalize_orgs(orgs):
            if pd.isna(orgs) or orgs is None:
                return None
            parts = [p.strip() for p in str(orgs).split("|") if p.strip()]
            if not parts:
                return None
            parts = sorted(set(parts))  # enlève doublons + trie
            return "|".join(parts)

        df_authors["organizations_norm"] = df_authors["organizations"].apply(normalize_orgs)

        # Définition de ce qui constitue un doublon d’auteur pour une même publication
        subset_cols = [
            "row_id",
            "author",
            "orcid_id",
            "internal_author_id",
            "role",
            # "openalex_is_corresponding",
            # "organizations_norm",
        ]

        df_authors = (
            df_authors
            .drop_duplicates(subset=subset_cols, keep="first")
            .reset_index(drop=True)
        )

        # On enlève la colonne technique, les données originales restent inchangées
        df_authors = df_authors.drop(columns=["organizations_norm"])

        # Garder row_id en première colonne
        author_cols = ["row_id"] + [col for col in df_authors.columns if col != "row_id"]
        df_authors = df_authors[author_cols]

        # DataFrame des métadonnées (sans la liste 'authors')
        df_metadata = df.drop(columns=["authors"])
        metadata_cols = ["row_id"] + [col for col in df_metadata.columns if col != "row_id"]
        df_metadata = df_metadata[metadata_cols]

        return df_metadata, df_authors

import marimo

__generated_with = "0.14.12"
app = marimo.App(width="medium")


@app.cell
def _():
    # ╭──────────────────────────────────────────────────────────────╮
    # │ Notebook de curation des publications Infoscience            │
    # │ reconciliées avec les métadonnées d'OpenAlex                 │
    # ╰──────────────────────────────────────────────────────────────╯
    # 
    # Objectif :
    # Concolider les publications Infoscience avec les métadonnées d'OpenAlex. Filtrer selon les types de publication retenus pour l'ETH Domain Bibliometric Study et calculer le statut open access selon les même règles que le swiss oa repository monitor : https://oamonitor.ch/wiki/open-access-typology-2/

    # ╭──────────────╮
    # │ Données      │
    # ╰──────────────╯
    # Les jeux de données utilisés sont générés en amont via le notebook enrich_with_openalex.py :
    # - infoscience_2013-2025_openalex_reconciled.csv : publications extraites d'Infoscience et enrichies avec l'API d'OpenAlex

    # ╭──────────────╮
    # │ Résultat     │
    # ╰──────────────╯
    # Deux dataframes :
    # - Publications 2024-2023 (journal articles, conference papers et reviews) + status présentes dans Infoscience et réconciliées avec OpenAlex
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from pandas import ExcelWriter
    import numpy as np
    import ast
    from datetime import datetime
    return ast, datetime, np, pd


@app.cell
def _(ast, np, pd):
    # Fusionner les deux DataFrames
    df = pd.read_csv("./eth_bibliometric/raw_data/2013-2025_infoscience_openalex_reconciled.csv")

    # --- Extract 'issued_year' from flexible date formats
    df['year'] = df['issued'].astype(str).str.extract(r'^(\d{4})').astype(float).astype('Int64')

    # --- Remove entries with missing or empty 'handle'
    df = df[~df['handle'].isna() & (df['handle'].str.strip() != '')]

    # --- Map 'type' to simplified 'publication_type'
    type_mapping = {
        "text::book/monograph": "book",
        "text::book/monograph::book part or chapter": "book_part",
        "text::conference output::conference proceedings::conference paper": "conference_paper",
        "text::journal::journal article": "journal_article",
        "text::journal::journal article::data paper": "journal_article",
        "text::journal::journal article::research article": "journal_article",
        "text::journal::journal article::review article": "review",
        "text::objet présenté à une conférence::actes de conférence::article dans une conférence/papier de conférence": "conference_paper",
        "text::revue::article de revue": "journal_article",
        "text::revue::article de revue::article de revue scientifique": "journal_article",
        "text::revue::article de revue::article de synthèse": "review",
        "text::conference output::conference proceedings": "book",
    }
    # Apply the mapping to create a new column 'publication_type'
    df['publication_type'] = df['type'].map(type_mapping).fillna("other")

    # Refine type mapping of 'publication_type' using openalex data

    # 2. Extraire le prefixe DOI (avant le slash)
    df['doi_prefix'] = df['openalex_doi'].apply(lambda x: x.split('/')[0] if isinstance(x, str) and '/' in x else '')

    # Rule 1: book-chapter + doi prefix == 10.1007 → conference_paper
    mask = (df['openalex_doctype'] == 'book-chapter') & (df['doi_prefix'] == '10.1007')
    df.loc[mask, 'publication_type'] = 'conference_paper'

    # Rule 2: book-chapter + other prefix → book-chapter
    mask = (df['openalex_doctype'] == 'book-chapter') & (df['doi_prefix'] != '10.1007')
    df.loc[mask, 'publication_type'] = 'book-chapter'

    # Rule 3: journal-article + openalex_type == review → review
    mask = (df['openalex_doctype'] == 'journal-article') & (df['openalex_openalex_type'] == 'review')
    df.loc[mask, 'publication_type'] = 'review'

    # ---------------------------------------------
    # Filter rows to keep only selected publication types
    # ---------------------------------------------
    df = df[df['publication_type'].isin(['journal_article', 'conference_paper', 'review'])]

    # --- Map 'version' and 'legacy_version'to simplified 'resource_version'
    def resolve_resource_version(row):
        """
        Combines version, legacy_version, and openalex_best_oa_version
        into a single, prioritized version string.
        """

        # Mapping of COAR version URIs to readable names
        version_map = {
            "http://purl.org/coar/version/c_71e4c1898caa6e32": "submittedVersion",
            "http://purl.org/coar/version/c_970fb48d4fbd8a85": "publishedVersion",
            "http://purl.org/coar/version/c_ab4af688f83e57aa": "acceptedVersion",
            "http://purl.org/coar/version/c_be7fb7dd8ff6fe43": "copyright",
            "http://purl.org/coar/version/c_e19f295774971610": "correctedVersion"
        }

        # Translate version URIs to readable values
        def normalize(val):
            if pd.isna(val) or not val:
                return None
            val = str(val).strip()
            return version_map.get(val, val)  # map if possible, else keep as-is

        # List of candidate values (in priority order)
        candidates = [
            normalize(row.get('version')),
            normalize(row.get('legacy_version')),
            normalize(row.get('openalex_best_oa_version'))
        ]

        # Priority order
        priority = [
            'publishedVersion',
            'acceptedVersion',
            'submittedVersion',
            'copyright',
            'correctedVersion'
        ]

        # Filter out empties and duplicates
        unique = []
        seen = set()
        for v in candidates:
            if v and v not in seen:
                unique.append(v)
                seen.add(v)

        # Return the best (highest priority) version available
        for choice in priority:
            if choice in unique:
                return choice

        return None  # Nothing valid found

    df['resource_version'] = df.apply(resolve_resource_version, axis=1)


    # --- Map 'license' and 'openalex_best_oa_license' to simplified 'license_condition'
    license_map = {
        "apache license": "other-oa",
        "cc by": "cc-by",
        "cc-by": "cc-by",
        "cc-by-nc": "cc-by-nc",
        "cc by nc": "cc-by-nc",
        "cc by nc nd": "cc-by-nc-nd",
        "cc-by-nc-nd": "cc-by-nc-nd",
        "cc by-nc": "cc-by-nc",
        "cc by-nc-nd": "cc-by-nc-nd",
        "cc-by-nc-sa": "cc-by-nc-sa",
        "cc by nc sa": "cc-by-nc-sa",
        "cc by-nd": "cc-by-nd",
        "cc-by-nd": "cc-by-nd",
        "cc by-sa": "cc-by-sa",
        "cc-by-sa": "cc-by-sa",
        "cc0": "cc0",
        "cc by 4.0": "cc-by",
        "creative commons attribution 4.0 international": "cc-by",
        "copyright": "copyright",
        "gnu-gpl": "gpl-v3",
        "gpl-v3": "gpl-v3",
        "mit": "mit",
        "mit license": "mit",
        "open access": "other-oa",
        "optica open access publishing agreement": "publisher-specific-oa",
        "optica publishing group under the terms of the optica open access publishing agreement": "publisher-specific-oa",
        "other-oa": "other-oa",
        "public-domain": "public-domain",
        "publisher-specific-oa": "publisher-specific-oa",
        "": "n/a",
        "none": "n/a",
        "null": "n/a"
    }

    # Step 2: Define license priority order (highest to lowest)
    license_priority = [
        "cc-by", "cc-by-nc", "cc-by-nc-nd", "cc-by-nc-sa",
        "cc-by-nd", "cc-by-sa", "gpl-v3", "mit",
        "other-oa", "public-domain", "publisher-specific-oa", "n/a"
    ]

    # Step 3: Normalization helper
    def normalize_license(val):
        if pd.isna(val):
            return "n/a"
        val = str(val).strip().lower()
        return license_map.get(val, val)

    # Step 4: Build final 'license_condition' value
    def build_license_condition(row):
        val1 = normalize_license(row.get('license', ''))
        val2 = normalize_license(row.get('openalex_best_oa_license', ''))

        # Remove duplicates and empty values
        combined = []
        for val in [val1, val2]:
            if val and val not in combined:
                combined.append(val)

        # Return the most prioritized valid value
        for lic in license_priority:
            if lic in combined:
                return lic

        return "n/a"

    # Step 5: Apply the function
    df['license_condition'] = df.apply(build_license_condition, axis=1)

    # ---------------------------------------------
    # Initialize final columns (default to 'Closed')
    # ---------------------------------------------
    df['oa_status'] = 'Closed'
    df['oa_type'] = 'Closed'

    # Boolean mask to track which rows have already been assigned an OA status
    already_set = pd.Series(False, index=df.index)

    # ---------------------------------------------
    # 1. DIAMOND OA
    # ---------------------------------------------
    diamond_mask = (
        ((df['access-level'] == 'openaccess') | (df['openalex_best_oa_is_oa'] == True)) &
        (df['openalex_oa_status'].str.lower() == 'diamond') &
        (~already_set)
    )
    df.loc[diamond_mask, 'oa_status'] = 'Open'
    df.loc[diamond_mask, 'oa_type'] = 'Diamond'
    already_set |= diamond_mask  # Mark rows as processed


    # ---------------------------------------------
    # 2. GOLD OA
    # ---------------------------------------------
    gold_mask = (
        ((df['access-level'] == 'openaccess') | (df['openalex_best_oa_is_oa'] == True)) &
        (
            (df['openalex_oa_status'].str.lower() == 'gold') |
            (
                (df['type'].isin([
                'text::book/monograph',
                'text::book/monograph::book part or chapter',
                'text::conference output::conference proceedings'
                ])) &
                (
                    df['license_condition'].isin([
                        'cc-by', 'cc-by-nc', 'cc-by-nc-nd', 'cc-by-nc-sa',
                        'cc-by-nd', 'cc-by-sa', 'cc0', 'public-domain'
                    ])
                )
            )
        ) &
        (~already_set)
    )
    df.loc[gold_mask, 'oa_status'] = 'Open'
    df.loc[gold_mask, 'oa_type'] = 'Gold'
    already_set |= gold_mask

    # ---------------------------------------------
    # 3. HYBRID OA
    # ---------------------------------------------
    hybrid_mask = (
        ((df['access-level'] == 'openaccess') | (df['openalex_best_oa_is_oa'] == True)) &
        (
            df['license_condition'].isin([
                'cc-by', 'cc-by-nc', 'cc-by-nc-nd', 'cc-by-nc-sa',
                'cc-by-nd', 'cc-by-sa', 'cc0', 'public-domain'
            ])
        ) &
        (
            (df['resource_version'] == "publishedVersion")
        ) &
        (~df['openalex_primary_source_type'].isin([None, np.nan, 'NULL', 'repository'])) &
        (~already_set)
    )
    df.loc[hybrid_mask, 'oa_status'] = 'Open'
    df.loc[hybrid_mask, 'oa_type'] = 'Hybrid'
    already_set |= hybrid_mask

    # ---------------------------------------------
    # 4. GREEN OA
    # ---------------------------------------------
    green_mask = (
        (
            (df['openalex_best_oa_is_oa'] == True) & (df['openalex_primary_source_type'] == 'repository') |
            (df['access-level'] == 'openaccess')
        ) &
        (
            (df['resource_version'] == "acceptedVersion") |
            (df['resource_version'] == "publishedVersion")
        ) &
        (~already_set)
    )
    df.loc[green_mask, 'oa_status'] = 'Open'
    df.loc[green_mask, 'oa_type'] = 'Green'
    already_set |= green_mask



    # ---------------------------------------------
    # Reorder columns to place the new ones at the end
    # ---------------------------------------------
    column_order = [col for col in df.columns if col not in ['oa_status', 'oa_type']]
    df = df[column_order + ['oa_status', 'oa_type']]

    # ---------------------------------------------
    # Create Published in column
    # ---------------------------------------------
    def build_published_in(row):
        """
        Builds the 'Published in' field from publishedin, journalorseries, and openalex_primary_container_title.
        Handles both stringified lists and plain strings.
        Cleans duplicates and joins with '||' separator.
        """
        def to_list(val):
            if pd.isna(val) or val in ['', '[]', None]:
                return []
            val = val.strip()
            # If it's a stringified list (e.g., "['A', 'B']"), parse it
            if val.startswith('[') and val.endswith(']'):
                try:
                    return ast.literal_eval(val)
                except Exception:
                    return []
            return [val]  # Otherwise treat as a single string

        # Parse fields into lists
        publishedin_list = to_list(row.get('publishedin', ''))
        journalorseries_list = to_list(row.get('journalorseries', ''))

        # Combine and deduplicate while preserving order
        combined = publishedin_list + journalorseries_list
        seen = set()
        unique = []
        for item in combined:
            item = str(item).strip()
            if item and item not in seen:
                unique.append(item)
                seen.add(item)

        # Fallback if nothing valid found
        if not unique and pd.notna(row.get('openalex_primary_container_title')):
            return row['openalex_primary_container_title'].strip()

        return '||'.join(unique) if unique else None

    df['Published in'] = df.apply(build_published_in, axis=1)

    # ---------------------------------------------
    # Create ISSN(s) or ISBN(s) column
    # ---------------------------------------------
    def build_isbn_issn(row):
        """
        Builds the 'ISBN or ISSN(s)' column by concatenating issn, isbn,
        openalex_primary_issn and openalex_primary_issn_l.
        Handles:
        - stringified lists,
        - plain strings,
        - values already separated by '||' or ','.
        Removes duplicates while preserving order.
        """
        import ast

        def to_list(val):
            if pd.isna(val) or val in ['', '[]', None]:
                return []
            val = str(val).strip()
            # Case 1: list encoded as string
            if val.startswith('[') and val.endswith(']'):
                try:
                    return ast.literal_eval(val)
                except Exception:
                    return []
            # Case 2: multiple values joined by || or ,
            for sep in ['||', ',']:
                if sep in val:
                    return [v.strip() for v in val.split(sep) if v.strip()]
            # Case 3: simple value
            return [val]

        # Combine all sources
        fields = ['issn', 'isbn', 'openalex_primary_issn', 'openalex_primary_issn_l']
        combined = []
        for field in fields:
            combined += to_list(row.get(field, ''))

        # Remove duplicates while preserving order
        seen = set()
        unique = []
        for item in combined:
            if item and item not in seen:
                unique.append(item)
                seen.add(item)

        return '||'.join(unique) if unique else None

    # Application
    df['ISBN or ISSN(s)'] = df.apply(build_isbn_issn, axis=1)

    # ---------------------------------------------
    # Rename columns according to the specified mapping
    # ---------------------------------------------
    df = df.rename(columns={
        'handle': 'internal_id',
        'authors': 'authors_all',
        'publisher': 'publisher',
        'doi': "doi",
        'authors_all': "authors",
        'openalex_openalex_id': "openalex_id",
        'type': 'type',
        'journalorseries': 'journal',
        'openalex_doctype': 'openalex_crossref_type',
        # 'license': 'oaire.licenseCondition',
        # 'version': 'oaire.version',
        # 'legacy_version': 'epfl.publication.version', 
    })

    # ---------------------------------------------
    # Create Publisher column
    # ---------------------------------------------
    def get_publisher(row):
        pub = row.get('publisher', '')
        if pd.notna(pub) and str(pub).strip():
            return str(pub).strip()
        fallback = row.get('openalex_primary_host_org', '')
        return str(fallback).strip() if pd.notna(fallback) else None

    df['publisher'] = df.apply(get_publisher, axis=1)

    df.to_csv("./eth_bibliometric/processed_data/2013-2025_infoscience_with_final_oa.csv", index=False)
    df
    return (df,)


@app.cell
def _(datetime, df):
    # ---------------------------------------------
    # Build the final dataframes
    # ---------------------------------------------
    ordered_columns = [
        "title",
        "uuid",
        "internal_id",
        "doi",
        "openalex_id",
        "year",
        "authors_all",
        "authors_institution",
        "orcid_institution",
        "journal",
        "publication_type",
        "oa_status",
        "oa_type",
        "type",
        "license_condition",
        "resource_version",
        "issn",
        "isbn",
        "issued",
        "access-level",
        "openalex_internal_id",
        "openalex_openalex_type",
        "openalex_crossref_type",
        "openalex_primary_container_title",
        "openalex_primary_host_org",
        "openalex_primary_issn",
        "openalex_primary_issn_l",
        "openalex_oa_is_oa",
        "openalex_oa_status",
        "openalex_best_oa_container_title",
        "openalex_best_oa_host_org",
        "openalex_best_oa_is_in_doaj",
        "openalex_best_oa_is_oa",
        "openalex_best_oa_license",
        "openalex_best_oa_source_type",
        "openalex_best_oa_version"
    ]

    # Check for missing columns (useful to debug renaming mistakes)
    missing = [c for c in ordered_columns if c not in df.columns]
    if missing:
        print("Missing columns in df:", missing)

    # Create the final dataframe (columns not found will raise a KeyError unless you reindex)
    final_df = df.reindex(columns=ordered_columns)

    # Build one file by year
    creation_date_str = datetime.today().strftime('%Y-%m-%d')
    # saved_files = []

    # for year in final_df['year'].dropna().unique():
    #     year_str = str(year)
    #     subset = final_df[final_df['year'] == year]
    #     filename = f"./eth_bibliometric/{year_str}_eth_bibliometric_dataset_epfl_{creation_date_str}.csv"
    #     subset.to_csv(filename, index=False)
    #     saved_files.append(filename)

    # print("Saved files :", saved_files)

    # (Optionnel) Export
    # final_df.to_csv(f"./eth_bibliometric/eth_bibliometric_dataset_epfl_{creation_date_str}.csv", index=False)

    # Aperçu
    final_df
    return (final_df,)


@app.cell
def _(final_df):
    # ---------------------------------------------
    # Export by year range
    # ---------------------------------------------

    def export_by_year_range(df, start_year, end_year, filename_prefix="eth_bibliometric_dataset_epfl_repository"):
        """
        Exporte les données du DataFrame entre deux années données (inclusives)
        basées sur la colonne 'year'. Sauvegarde dans un fichier CSV.

        Args:
            df (pd.DataFrame): Le DataFrame contenant les données filtrées.
            start_year (int): L'année de début (inclus).
            end_year (int): L'année de fin (inclus).
            filename_prefix (str): Préfixe du nom du fichier.

        Returns:
            str: Le nom du fichier généré.
        """
        import pandas as pd
        from datetime import datetime

        # Filtrage par plage d'années
        filtered = df[
            (df['year'].notna()) &
            (df['year'] >= start_year) &
            (df['year'] <= end_year)
        ]

        # Construire le nom du fichier
        date_str = datetime.today().strftime('%Y-%m-%d')
        filename = f"./eth_bibliometric/curated_data/{start_year}-{end_year}_{filename_prefix}_{date_str}.csv"

        # Sauvegarder
        filtered.to_csv(filename, index=False)
        return filename

    file_created = export_by_year_range(final_df, 2013, 2024)
    return


if __name__ == "__main__":
    app.run()

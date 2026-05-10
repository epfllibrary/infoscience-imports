import marimo

__generated_with = "0.14.12"
app = marimo.App(width="medium")


@app.cell
def _():
    # ╭─────────────────────────────────────────────────────────────────────────────╮
    # │ Notebook de curation pour cross-checked publications Infoscience/OpenAlex   │
    # ╰─────────────────────────────────────────────────────────────────────────────╯
    # 
    # Objectif :
    # Objectif :
    # Comparaison des publications issues d'Infoscience et d'OpenAlex selon les critères de la 
    # ETH Domain Bibliometric Study :
    # - Types de publication : articles de journaux, actes de conférences, revues
    # - Statut : évalué par les pairs (peer-reviewed)
    #
    # Objectif : vérifier la cohérence globale entre les deux sources et identifier les publications 
    # qui ne correspondent pas aux critères ou qui sont absentes dans l'une ou l'autre.
    # Pour les publications non alignées, une vérification  est effectuée pour évaluer 
    # si elles pourraient néanmoins être incluses dans l’analyse. Ces cas sont souvent dus à 
    # des métadonnées incomplètes ou erronées dans Infoscience.


    # ╭──────────────╮
    # │ Données      │
    # ╰──────────────╯
    # Les jeux de données utilisés sont générés en amont via les notebooks enrich_with_openalex.py et harvest_from_openalex.py  :
    # - 2013-2025_openalex_in_infoscience.csv : publications OpenAlex présentes dans Infoscience
    # - 2013-2025_infoscience_with_final_oa.csv :  publications extraites d'Infoscience et enrichies avec métadonnées d'OpenAlex

    # ╭──────────────╮
    # │ Résultat     │
    # ╰──────────────╯
    # Deux dataframes :
    # - Nouvelles publications infoscience candidates pour l'ETH Domain Bibliometric Study : 2024-2023 (journal articles, conference papers et reviews) 
    return


@app.cell
def _():
    import sys
    import os
    import re
    import marimo as mo
    import pandas as pd
    from pandas import ExcelWriter
    import numpy as np
    import ast
    from datetime import datetime
    sys.path.append(os.path.abspath(".."))
    return ast, datetime, np, pd


@app.cell
def _(pd):
    # Charger les deux fichiers
    df_openalex = pd.read_csv("./eth_bibliometric/processed_data/2013-2025_openalex_in_infoscience.csv")
    df_raw = pd.read_csv("./eth_bibliometric/processed_data/2013-2025_infoscience_with_final_oa.csv")
    return df_openalex, df_raw


@app.cell
def _(df_openalex, df_raw):
    # Nettoyer les colonnes de jointure
    df_openalex["openalex_id"] = df_openalex["openalex_id"].str.strip()
    df_raw["openalex_id"] = df_raw["openalex_id"].str.strip()
    return


@app.cell
def _(df_raw):
    # Renommer dans df_raw pour préparer la fusion
    df_raw_renamed = df_raw.rename(columns={
        "internal_id": "handle"
    })

    # Garder uniquement les colonnes nécessaires
    df_raw_filtered = df_raw_renamed[["openalex_id", "uuid", "handle"]]
    return (df_raw_filtered,)


@app.cell
def _(df_openalex, df_raw_filtered, pd):
    # ---------------------------------------------
    # Comparaison des deus dataframes OpenAlex et 
    # Infoscience
    # ---------------------------------------------

    df_augmented = pd.merge(df_openalex, df_raw_filtered, on="openalex_id", how="left")
    # Ajouter une colonne pour repérer les non-matches
    df_augmented["match_found"] = df_augmented["uuid"].notna()
    # Aperçu
    df_augmented
    return (df_augmented,)


@app.cell
def _(df_augmented):
    # ---------------------------------------------
    # Publications OpenAlex non incluses dans l'extraction
    # Infoscience initiale
    # ---------------------------------------------
    df_no_match = df_augmented[df_augmented["match_found"] == False].copy()
    df_no_match = df_no_match.drop(columns=["uuid", "handle"])


    # ---------------------------------------------
    # On relance une réconciliation avec Infoscience
    # pour vérifier si ces dernières existent dans 
    # le dépot institutionnel
    # ---------------------------------------------
    from data_pipeline.deduplicator import DataFrameProcessor
    from clients.dspace_client_wrapper import DSpaceClientWrapper
    processor = DataFrameProcessor()
    df_no_match
    return df_no_match, processor


@app.cell
def _(df_no_match, processor):
    df_notexists, df_exists = processor.deduplicate_infoscience_enhanced(df_no_match)
    return (df_exists,)


@app.cell
def _(df_exists):
    df_exists[["doi", "dc_identifier_doi"]]
    return


@app.cell
def _(df_exists):
    df_exists[['title', 'dc_title']]
    return


@app.cell
def _(df_exists, pd):
    from rapidfuzz.fuzz import ratio

    def is_different(a, b, threshold=90, length_ratio_threshold=0.5):
        if pd.isna(a) or pd.isna(b):
            return False
    
        a_str, b_str = str(a), str(b)

        similarity = ratio(a_str, b_str)
        len_ratio = min(len(a_str), len(b_str)) / max(len(a_str), len(b_str))

        return similarity < threshold or len_ratio < length_ratio_threshold

    # Masque combiné : on exclut si title/dc_title sont trop différents
    # OU si doi/dc_identifier_doi sont tous les deux présents ET trop différents
    mask_keep = df_exists.apply(
        lambda row: not (
            is_different(row.get("title"), row.get("dc_title")) or
            (
                pd.notna(row.get("doi")) and pd.notna(row.get("dc_identifier_doi")) and
                is_different(row.get("doi"), row.get("dc_identifier_doi"), threshold=95, length_ratio_threshold=0.8)
            )
        ),
        axis=1
    )

    # Filtrer avec ce masque
    df_exists_cleaned = df_exists[mask_keep].copy()
    df_exists_cleaned[['doi','dc_identifier_doi','title', 'dc_title']]
    return (df_exists_cleaned,)


@app.cell
def _(df_exists_cleaned, np, pd):
    # ---------------------------------------------
    # A partir des publications réconciliées avec
    # Infosicence, on identifie celles qui pourraient 
    # être candidates pour l'étude ETH
    # ---------------------------------------------


    # 1. Nettoyage des colonnes (en minuscules pour éviter les erreurs de casse)
    df_exists_cleaned["doctype"] = df_exists_cleaned["doctype"].fillna("").str.strip().str.lower()
    df_exists_cleaned["openalex_type"] = (
        df_exists_cleaned["openalex_type"].fillna("").str.strip().str.lower()
    )
    df_exists_cleaned["primary_version"] = (
        df_exists_cleaned["primary_version"].fillna("").str.strip().str.lower()
    )
    df_exists_cleaned["primary_source_type"] = (
        df_exists_cleaned["primary_source_type"].fillna("").str.strip().str.lower()
    )
    df_exists_cleaned["doi"] = df_exists_cleaned["doi"].fillna("").str.strip()

    # 2. Extraire le prefixe DOI (avant le slash)
    df_exists_cleaned["doi_prefix"] = df_exists_cleaned["doi"].apply(
        lambda x: x.split("/")[0] if "/" in x else ""
    )

    # 3. Initialiser la colonne par défaut
    df_exists_cleaned["publication_type"] = "unknown"

    # 4. Appliquer les règles dans l’ordre donné

    # Rule 1: book-chapter + doi prefix == 10.1007 → conference_paper
    mask = (df_exists_cleaned["doctype"] == "book-chapter") & (
        df_exists_cleaned["doi_prefix"] == "10.1007"
    )
    df_exists_cleaned.loc[mask, "publication_type"] = "conference_paper"

    # Rule 2: book-chapter + other prefix → book-chapter
    mask = (df_exists_cleaned["doctype"] == "book-chapter") & (
        df_exists_cleaned["doi_prefix"] != "10.1007"
    )
    df_exists_cleaned.loc[mask, "publication_type"] = "book-chapter"

    # Rule 3: journal-article + openalex_type == review → review
    mask = (df_exists_cleaned["doctype"] == "journal-article") & (
        df_exists_cleaned["openalex_type"] == "review"
    )
    df_exists_cleaned.loc[mask, "publication_type"] = "review"

    # Rule 4: journal-article + openalex_type == article → journal_article
    mask = (df_exists_cleaned["doctype"] == "journal-article") & (
        df_exists_cleaned["openalex_type"] == "article"
    )
    df_exists_cleaned.loc[mask, "publication_type"] = "journal_article"

    # Rule 5: proceedings-article → conference_paper
    mask = df_exists_cleaned["doctype"] == "proceedings-article"
    df_exists_cleaned.loc[mask, "publication_type"] = "conference_paper"

    # Rule 6: submitted version → posted-content
    mask = df_exists_cleaned["primary_version"] == "submittedversion"
    df_exists_cleaned.loc[mask, "publication_type"] = "posted-content"

    # Rule 7: primary_source_type == repository → posted-content
    mask = df_exists_cleaned["primary_source_type"] == "repository"
    df_exists_cleaned.loc[mask, "publication_type"] = "posted-content"

    # Rule 8: primary_source_type == ebook platform → book-chapter
    mask = df_exists_cleaned["primary_source_type"] == "ebook platform"
    df_exists_cleaned.loc[mask, "publication_type"] = "book-chapter"

    # Rule 9: primary_source_type == conference OR book series → conference_paper
    mask = df_exists_cleaned["primary_source_type"].isin(["conference", "book series"])
    df_exists_cleaned.loc[mask, "publication_type"] = "conference_paper"


    # Nettoyer la colonne publication_type (au cas où)
    df_exists_cleaned["publication_type"] = (
        df_exists_cleaned["publication_type"].fillna("").astype(str).str.strip().str.lower()
    )

    # Définir les types bibliométriques acceptés
    bibliometric_types = ["journal_article", "conference_paper", "review"]

    # Mettre à True uniquement les publications correspondant à ces types
    df_exists_cleaned["eth_bibliometric"] = df_exists_cleaned["publication_type"].isin(
        bibliometric_types
    )

    # --- Map 'best_oa_version' to 'version'
    def resolve_resource_version(row):
        version_map = {
            "http://purl.org/coar/version/c_71e4c1898caa6e32": "submittedVersion",
            "http://purl.org/coar/version/c_970fb48d4fbd8a85": "publishedVersion",
            "http://purl.org/coar/version/c_ab4af688f83e57aa": "acceptedVersion",
            "http://purl.org/coar/version/c_be7fb7dd8ff6fe43": "copyright",
            "http://purl.org/coar/version/c_e19f295774971610": "correctedVersion",
        }

        def normalize(val):
            if pd.isna(val) or not val:
                return None
            val = str(val).strip()
            return version_map.get(val, val)

        candidates = [normalize(row.get("best_oa_version"))]

        priority = [
            "publishedVersion",
            "acceptedVersion",
            "submittedVersion",
            "copyright",
            "correctedVersion",
        ]
        seen = set()
        unique = [v for v in candidates if v and not (v in seen or seen.add(v))]

        for choice in priority:
            if choice in unique:
                return choice

        return None


    df_exists_cleaned["version"] = df_exists_cleaned.apply(resolve_resource_version, axis=1)


    # --- Normalize license strings
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
        "null": "n/a",
    }

    license_priority = [
        "cc-by",
        "cc-by-nc",
        "cc-by-nc-nd",
        "cc-by-nc-sa",
        "cc-by-nd",
        "cc-by-sa",
        "gpl-v3",
        "mit",
        "other-oa",
        "public-domain",
        "publisher-specific-oa",
        "n/a",
    ]


    def normalize_license(val):
        if pd.isna(val):
            return "n/a"
        val = str(val).strip().lower()
        return license_map.get(val, val)


    def build_license_condition(row):
        val = normalize_license(row.get("best_oa_license", ""))
        return val if val in license_priority else "n/a"


    df_exists_cleaned["license"] = df_exists_cleaned.apply(build_license_condition, axis=1)


    # ---------------------------------------------
    # Initialize OA columns
    # ---------------------------------------------
    df_exists_cleaned["oa_status"] = "Closed"
    df_exists_cleaned["oa_type"] = "Closed"
    already_set = pd.Series(False, index=df_exists_cleaned.index)

    # DIAMOND OA
    diamond_mask = (
        (df_exists_cleaned["best_oa_is_oa"] == True)
        & (df_exists_cleaned["oa_status"].str.lower() == "diamond")
        & (~already_set)
    )
    df_exists_cleaned.loc[diamond_mask, "oa_status"] = "Open"
    df_exists_cleaned.loc[diamond_mask, "oa_type"] = "Diamond"
    already_set |= diamond_mask

    # GOLD OA
    gold_mask = (
        (df_exists_cleaned["best_oa_is_oa"] == True)
        & (
            (df_exists_cleaned["oa_status"].str.lower() == "gold")
            | (
                (df_exists_cleaned["publication_type"] == "book-chapter")
                & (
                    df_exists_cleaned["license"].isin(
                        [
                            "cc-by",
                            "cc-by-nc",
                            "cc-by-nc-nd",
                            "cc-by-nc-sa",
                            "cc-by-nd",
                            "cc-by-sa",
                            "cc0",
                            "public-domain",
                        ]
                    )
                )
            )
        )
        & (~already_set)
    )
    df_exists_cleaned.loc[gold_mask, "oa_status"] = "Open"
    df_exists_cleaned.loc[gold_mask, "oa_type"] = "Gold"
    already_set |= gold_mask

    # HYBRID OA
    hybrid_mask = (
        (df_exists_cleaned["best_oa_is_oa"] == True)
        & (
            df_exists_cleaned["license"].isin(
                [
                    "cc-by",
                    "cc-by-nc",
                    "cc-by-nc-nd",
                    "cc-by-nc-sa",
                    "cc-by-nd",
                    "cc-by-sa",
                    "cc0",
                    "public-domain",
                ]
            )
        )
        & (df_exists_cleaned["version"] == "publishedVersion")
        & (~df_exists_cleaned["primary_source_type"].isin([None, np.nan, "NULL", "repository"]))
        & (~already_set)
    )
    df_exists_cleaned.loc[hybrid_mask, "oa_status"] = "Open"
    df_exists_cleaned.loc[hybrid_mask, "oa_type"] = "Hybrid"
    already_set |= hybrid_mask

    # GREEN OA
    green_mask = (
        (df_exists_cleaned["best_oa_is_oa"] == True)
        & (df_exists_cleaned["primary_source_type"] == "repository")
        & (df_exists_cleaned["version"].isin(["acceptedVersion", "publishedVersion"]))
        & (~already_set)
    )
    df_exists_cleaned.loc[green_mask, "oa_status"] = "Open"
    df_exists_cleaned.loc[green_mask, "oa_type"] = "Green"
    already_set |= green_mask
    df_exists_cleaned
    return


@app.cell
def _(df_exists_cleaned):
    # ---------------------------------------------
    # On supprime ce qui n'est pas identifié comme 
    # peer-reviewed ou EPFL dans Infosicence
    # ---------------------------------------------
    df_clean = df_exists_cleaned[~((df_exists_cleaned["epfl_peerreviewed"] == "NON-REVIEWED") & (df_exists_cleaned["epfl_writtenat"] == "OTHER"))]
    df_clean
    return (df_clean,)


@app.cell
def _(ast, datetime, pd):
    # ---------------------------------------------
    # Fonction pour construire les deux dataframes finaux
    # ---------------------------------------------

    def export_bibliometric_dataframe(df, filename_prefix="eth_bibliometric_dataset_epfl", start_year=None, end_year=None):
        """
        Transforme, trie et exporte un DataFrame bibliométrique au format CSV standardisé,
        avec option de filtrage par année.

        - Réordonne les colonnes
        - Trie les données par année (ascendant)
        - Filtre selon les années si spécifiées
        - Sauvegarde un CSV avec un timestamp
        - Retourne le DataFrame final

        Args:
            df (pd.DataFrame): Le DataFrame à exporter
            filename_prefix (str): Préfixe du nom de fichier exporté
            start_year (int, optional): Année de début pour filtrer
            end_year (int, optional): Année de fin pour filtrer

        Returns:
            pd.DataFrame: Le DataFrame réorganisé, filtré, trié et exporté
        """
        df = df.rename(columns={
            'internal_id': 'openalex_url',
            'handle': 'internal_id',
            'pubyear': 'year',
            'primary_container_title': 'journal',
            'authors': 'authors_all',
        })


        # Colonnes attendues
        ordered_columns = [
            "title",
            "doi",
            "internal_id",
            "openalex_id",
            "year",
            "authors_all",
            "journal",
            "publication_type",
            "oa_status",
            "oa_type",
        ]

        # Vérifier les colonnes manquantes
        missing = [col for col in ordered_columns if col not in df.columns]
        if missing:
            print("⚠️ Colonnes manquantes :", missing)

        # Reindexer selon l’ordre souhaité
        final_df = df.reindex(columns=ordered_columns)

        # Nettoyage de la colonne 'authors' pour ne garder que les noms
        def extract_author_names(author_str):
            try:
                authors_list = ast.literal_eval(author_str)
                return [author.get('author') for author in authors_list if 'author' in author]
            except Exception:
                return []

        if 'authors_all' in final_df.columns:
            final_df['authors_all'] = final_df['authors_all'].apply(extract_author_names)

        # Convertir 'year' en numérique pour filtrage et tri
        final_df['year'] = pd.to_numeric(final_df['year'], errors='coerce')

        # Appliquer le filtrage par années si fourni
        if start_year is not None:
            final_df = final_df[final_df['year'] >= start_year]
        if end_year is not None:
            final_df = final_df[final_df['year'] <= end_year]

        # Trier par année croissante
        final_df = final_df.sort_values(by='year', ascending=True)

        # Générer le nom de fichier avec la date
        creation_date_str = datetime.today().strftime('%Y-%m-%d')
        filename = f"./eth_bibliometric/curated_data/{start_year}-{end_year}_{filename_prefix}-exceptions_{creation_date_str}.csv"

        # Exporter au format CSV
        final_df.to_csv(filename, index=False)
        print(f"✅ Exporté : {filename}")

        return final_df
    return (export_bibliometric_dataframe,)


@app.cell
def _(df_clean, export_bibliometric_dataframe):
    final_df = export_bibliometric_dataframe(df_clean, filename_prefix="eth_bibliometric_dataset_epfl_repository", start_year=2014, end_year=2023)
    final_df
    return


@app.cell
def _(pd):
    df1 = pd.read_csv('./eth_bibliometric/curated_data/2013-2024_eth_bibliometric_dataset_epfl_repository_2025-07-31.csv')
    df2 = pd.read_csv('./eth_bibliometric/curated_data/2014-2023_eth_bibliometric_dataset_epfl_openalex-only_2025-07-31.csv')
    df3 = pd.read_csv('./eth_bibliometric/curated_data/2014-2023_eth_bibliometric_dataset_epfl_repository-exceptions_2025-07-31.csv')

    # Liste des colonnes à utiliser pour la fusion
    merge_cols = ['title', 'internal_id', 'doi', 'openalex_id', 'year', 'authors_all',
                  'authors_institution', 'orcid_institution', 'journal', 'publication_type',
                  'oa_status', 'oa_type']

    # Harmoniser : ajouter colonnes manquantes et filtrer les colonnes voulues
    dfs = []
    for df in [df1, df2, df3]:
        for col in merge_cols:
            if col not in df.columns:
                df[col] = pd.NA  # ajoute colonne manquante avec NaN
        dfs.append(df[merge_cols])  # réorganise et filtre

    # Concaténer les DataFrames
    df_concatene = pd.concat(dfs, ignore_index=True)

    # Suppression des doublons sur internal_id, doi, openalex_id — seulement si la valeur est non nulle
    for col in ['internal_id', 'doi', 'openalex_id']:
        df_concatene = df_concatene[~(
            df_concatene[col].notna() &
            df_concatene.duplicated(subset=[col], keep='first')
        )]

    # Sauvegarde
    df_concatene.to_csv('./eth_bibliometric/curated_data/2014-2023_eth_bibliometric_dataset_epfl_final.csv', index=False)

    print(f"Concaténation terminée. Nombre de lignes : {df_concatene.shape[0]}")

    df_concatene
    return


if __name__ == "__main__":
    app.run()

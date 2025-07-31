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
    # Fusion (gauche) pour inclure toutes les lignes du fichier openalex
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
    df_no_match = df_augmented[df_augmented["match_found"] == False]
    df_no_match
    return (df_no_match,)


@app.cell
def _(df_augmented, df_no_match):
    # ---------------------------------------------
    # On relance une réconciliation avec Infoscience
    # pour vérifier si ces dernières existent dans 
    # le dépot institutionnel
    # ---------------------------------------------
    from data_pipeline.deduplicator import DataFrameProcessor
    from clients.dspace_client_wrapper import DSpaceClientWrapper

    processor = DataFrameProcessor()
    df_nomatch = df_augmented[df_augmented["match_found"] == False].copy()
    df_nomatch.drop(columns=["uuid", "handle"], inplace=True)
    to_import_df, duplicates_df = processor.deduplicate_infoscience_enhanced(df_no_match)
    return duplicates_df, to_import_df


@app.cell
def _(duplicates_df):
    duplicates_df
    return


@app.cell
def _(to_import_df):
    to_import_df
    return


@app.cell
def _(duplicates_df, np, pd):
    # ---------------------------------------------
    # A partir des publications réconciliées avec
    # Infosicence, on identifie celles qui pourraient 
    # être candidates pour l'étude ETH
    # ---------------------------------------------

    df_new = duplicates_df.copy()
    # 1. Nettoyage des colonnes (en minuscules pour éviter les erreurs de casse)
    df_new["doctype"] = df_new["doctype"].fillna("").str.strip().str.lower()
    df_new["openalex_type"] = (
        df_new["openalex_type"].fillna("").str.strip().str.lower()
    )
    df_new["primary_version"] = (
        df_new["primary_version"].fillna("").str.strip().str.lower()
    )
    df_new["primary_source_type"] = (
        df_new["primary_source_type"].fillna("").str.strip().str.lower()
    )
    df_new["doi"] = df_new["doi"].fillna("").str.strip()

    # 2. Extraire le prefixe DOI (avant le slash)
    df_new["doi_prefix"] = df_new["doi"].apply(
        lambda x: x.split("/")[0] if "/" in x else ""
    )

    # 3. Initialiser la colonne par défaut
    df_new["publication_type"] = "unknown"

    # 4. Appliquer les règles dans l’ordre donné

    # Rule 1: book-chapter + doi prefix == 10.1007 → conference_paper
    mask = (df_new["doctype"] == "book-chapter") & (
        df_new["doi_prefix"] == "10.1007"
    )
    df_new.loc[mask, "publication_type"] = "conference_paper"

    # Rule 2: book-chapter + other prefix → book-chapter
    mask = (df_new["doctype"] == "book-chapter") & (
        df_new["doi_prefix"] != "10.1007"
    )
    df_new.loc[mask, "publication_type"] = "book-chapter"

    # Rule 3: journal-article + openalex_type == review → review
    mask = (df_new["doctype"] == "journal-article") & (
        df_new["openalex_type"] == "review"
    )
    df_new.loc[mask, "publication_type"] = "review"

    # Rule 4: journal-article + openalex_type == article → journal_article
    mask = (df_new["doctype"] == "journal-article") & (
        df_new["openalex_type"] == "article"
    )
    df_new.loc[mask, "publication_type"] = "journal_article"

    # Rule 5: proceedings-article → conference_paper
    mask = df_new["doctype"] == "proceedings-article"
    df_new.loc[mask, "publication_type"] = "conference_paper"

    # Rule 6: submitted version → posted-content
    mask = df_new["primary_version"] == "submittedversion"
    df_new.loc[mask, "publication_type"] = "posted-content"

    # Rule 7: primary_source_type == repository → posted-content
    mask = df_new["primary_source_type"] == "repository"
    df_new.loc[mask, "publication_type"] = "posted-content"

    # Rule 8: primary_source_type == ebook platform → book-chapter
    mask = df_new["primary_source_type"] == "ebook platform"
    df_new.loc[mask, "publication_type"] = "book-chapter"

    # Rule 9: primary_source_type == conference OR book series → conference_paper
    mask = df_new["primary_source_type"].isin(["conference", "book series"])
    df_new.loc[mask, "publication_type"] = "conference_paper"


    # Nettoyer la colonne publication_type (au cas où)
    df_new["publication_type"] = (
        df_new["publication_type"].fillna("").astype(str).str.strip().str.lower()
    )

    # Définir les types bibliométriques acceptés
    bibliometric_types = ["journal_article", "conference_paper", "review"]

    # Mettre à True uniquement les publications correspondant à ces types
    df_new["eth_bibliometric"] = df_new["publication_type"].isin(
        bibliometric_types
    )


    # Réorganiser les colonnes pour publication_type (2e) et eth_bibliometric (3e)
    cols = df_new.columns.tolist()

    # On les retire s’ils existent déjà dans la liste (par sécurité)
    for col in ["publication_type", "eth_bibliometric"]:
        if col in cols:
            cols.remove(col)

    # Réinsertion aux bons indices
    cols = [cols[0], "publication_type", "eth_bibliometric"] + cols[1:]

    # Réaffectation de l'ordre au DataFrame
    df_new = df_new[cols]

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


    df_new["version"] = df_new.apply(resolve_resource_version, axis=1)


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


    df_new["license"] = df_new.apply(build_license_condition, axis=1)


    # ---------------------------------------------
    # Initialize OA columns
    # ---------------------------------------------
    df_new["oa_status"] = "Closed"
    df_new["oa_type"] = "Closed"
    already_set = pd.Series(False, index=df_new.index)

    # DIAMOND OA
    diamond_mask = (
        (df_new["best_oa_is_oa"] == True)
        & (df_new["oa_status"].str.lower() == "diamond")
        & (~already_set)
    )
    df_new.loc[diamond_mask, "oa_status"] = "Open"
    df_new.loc[diamond_mask, "oa_type"] = "Diamond"
    already_set |= diamond_mask

    # GOLD OA
    gold_mask = (
        (df_new["best_oa_is_oa"] == True)
        & (
            (df_new["oa_status"].str.lower() == "gold")
            | (
                (df_new["publication_type"] == "book-chapter")
                & (
                    df_new["license"].isin(
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
    df_new.loc[gold_mask, "oa_status"] = "Open"
    df_new.loc[gold_mask, "oa_type"] = "Gold"
    already_set |= gold_mask

    # HYBRID OA
    hybrid_mask = (
        (df_new["best_oa_is_oa"] == True)
        & (
            df_new["license"].isin(
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
        & (df_new["version"] == "publishedVersion")
        & (~df_new["primary_source_type"].isin([None, np.nan, "NULL", "repository"]))
        & (~already_set)
    )
    df_new.loc[hybrid_mask, "oa_status"] = "Open"
    df_new.loc[hybrid_mask, "oa_type"] = "Hybrid"
    already_set |= hybrid_mask

    # GREEN OA
    green_mask = (
        (df_new["best_oa_is_oa"] == True)
        & (df_new["primary_source_type"] == "repository")
        & (df_new["version"].isin(["acceptedVersion", "publishedVersion"]))
        & (~already_set)
    )
    df_new.loc[green_mask, "oa_status"] = "Open"
    df_new.loc[green_mask, "oa_type"] = "Green"
    already_set |= green_mask

    # Reorder OA columns at the end
    cols = [c for c in df_new.columns if c not in ["oa_status", "oa_type"]]
    df_new = df_new[cols + ["oa_status", "oa_type"]]

    return (df_new,)


@app.cell
def _(df_new):
    # ---------------------------------------------
    # On supprime ce qui n'est pas identifié comme 
    # peer-reviewed ou EPFL dans Infosicence
    # ---------------------------------------------
    df_clean = df_new[~((df_new["epfl_peerreviewed"] == "NON-REVIEWED") & (df_new["epfl_writtenat"] == "OTHER"))]
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
        filename = f"./eth_bibliometric/{start_year}-{end_year}_{filename_prefix}-exceptions_{creation_date_str}.csv"

        # Exporter au format CSV
        final_df.to_csv(filename, index=False)
        print(f"✅ Exporté : {filename}")

        return final_df
    return (export_bibliometric_dataframe,)


@app.cell
def _(df_clean, export_bibliometric_dataframe):
    final_df = export_bibliometric_dataframe(df_clean, filename_prefix="eth_bibliometric_dataset_epfl_repository", start_year=2014, end_year=2023)
    return


if __name__ == "__main__":
    app.run()

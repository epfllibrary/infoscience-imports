import marimo

__generated_with = "0.14.12"
app = marimo.App(width="medium")


@app.cell
def _():
    # ╭──────────────────────────────────────────────────────────────╮
    # │ Notebook de curation des publications OpenAlex non déposées  │
    # │ dans Infoscience avec au moins un auteur EPFL identifié      │
    # ╰──────────────────────────────────────────────────────────────╯
    # 
    # Objectif :
    # Identifier les publications présentes dans OpenAlex mais absentes d'Infoscience,
    # et dont au moins un auteur a été reconnu comme affilié à l'EPFL via les référentiels internes.
    # Cela permet de repérer les publications manquantes dans le dépôt institutionnel
    # et d'améliorer la complétude des données bibliométriques de l'EPFL.

    # ╭──────────────╮
    # │ Données      │
    # ╰──────────────╯
    # Les jeux de données utilisés sont générés en amont via le notebook harvest_from_openalex.py :
    # - 2013-2025_openalex_not_in_infoscience.csv : publications OpenAlex non présentes dans Infoscience
    # - 2013-2025_openalex_with_all_authors.csv : Liste des auteurs extraits au moment de l'extraction des publications EPFL depuis OpenAlex
    # - 2013-2025_openalex_not_in_infoscience_epfl_authors.csv : Liste des auteurs EPFL reconciliés pour les publications EPFL issues d'OpenAlex mais non trouvées dans Infoscience

    # ╭──────────────╮
    # │ Résultat     │
    # ╰──────────────╯
    # Deux dataframes :
    # - Publications 2024-2023 (journal articles, conference papers et reviews) dont au moins un auteur EPFL a été identifié
    # - Publications 2024-2023 (journal articles, conference papers et reviews) dont aucun auteur EPFL a été identifié
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from pandas import ExcelWriter
    import numpy as np
    import ast
    from datetime import datetime
    return datetime, np, pd


@app.cell
def _(np, pd):
    # Load openalex data
    df_main = pd.read_csv("./eth_bibliometric/processed_data/2013-2025_openalex_not_in_infoscience.csv")

    # Nettoyage des colonnes (en minuscules pour éviter les erreurs de casse)
    df_main['doctype'] = df_main['doctype'].fillna('').str.strip().str.lower()
    df_main['openalex_type'] = df_main['openalex_type'].fillna('').str.strip().str.lower()
    df_main['primary_version'] = df_main['primary_version'].fillna('').str.strip().str.lower()
    df_main['primary_source_type'] = df_main['primary_source_type'].fillna('').str.strip().str.lower()
    df_main['doi'] = df_main['doi'].fillna('').str.strip()

    # ---------------------------------------------
    # Construction colonne publication_type
    # ---------------------------------------------

    #  Extraire le prefixe DOI (avant le slash)
    df_main['doi_prefix'] = df_main['doi'].apply(lambda x: x.split('/')[0] if '/' in x else '')

    # Initialiser la colonne publication_type par défaut
    df_main['publication_type'] = 'unknown'

    # Appliquer les règles dans l’ordre donné
    # Rule 1: book-chapter + doi prefix == 10.1007 → conference_paper
    mask = (df_main['doctype'] == 'book-chapter') & (df_main['doi_prefix'] == '10.1007')
    df_main.loc[mask, 'publication_type'] = 'conference_paper'

    # Rule 2: book-chapter + other prefix → book-chapter
    mask = (df_main['doctype'] == 'book-chapter') & (df_main['doi_prefix'] != '10.1007')
    df_main.loc[mask, 'publication_type'] = 'book-chapter'

    # Rule 3: journal-article + openalex_type == review → review
    mask = (df_main['doctype'] == 'journal-article') & (df_main['openalex_type'] == 'review')
    df_main.loc[mask, 'publication_type'] = 'review'

    # Rule 4: journal-article + openalex_type == article → journal_article
    mask = (df_main['doctype'] == 'journal-article') & (df_main['openalex_type'] == 'article')
    df_main.loc[mask, 'publication_type'] = 'journal_article'

    # Rule 5: proceedings-article → conference_paper
    mask = (df_main['doctype'] == 'proceedings-article')
    df_main.loc[mask, 'publication_type'] = 'conference_paper'

    # Rule 6: submitted version → posted-content
    mask = (df_main['primary_version'] == 'submittedversion')
    df_main.loc[mask, 'publication_type'] = 'posted-content'

    # Rule 7: primary_source_type == repository → posted-content
    mask = (df_main['primary_source_type'] == 'repository')
    df_main.loc[mask, 'publication_type'] = 'posted-content'

    # Rule 8: primary_source_type == ebook platform → book-chapter
    mask = (df_main['primary_source_type'] == 'ebook platform')
    df_main.loc[mask, 'publication_type'] = 'book-chapter'

    # Rule 9: primary_source_type == conference OR book series → conference_paper
    mask = df_main['primary_source_type'].isin(['conference', 'book series'])
    df_main.loc[mask, 'publication_type'] = 'conference_paper'

    # ---------------------------------------------
    # Filtrer selon les publication types définis
    # ---------------------------------------------

    # Nettoyer la colonne publication_type (au cas où)
    df_main['publication_type'] = df_main['publication_type'].fillna('').astype(str).str.strip().str.lower()

    # Définir les types bibliométriques acceptés
    bibliometric_types = ['journal_article', 'conference_paper', 'review']

    # Mettre à True uniquement les publications correspondant à ces types
    df_main['eth_bibliometric'] = df_main['publication_type'].isin(bibliometric_types)

    # Réorganiser les colonnes pour publication_type (2e) et eth_bibliometric (3e)
    cols = df_main.columns.tolist()

    # On les retire s’ils existent déjà dans la liste (par sécurité)
    for col in ['publication_type', 'eth_bibliometric']:
        if col in cols:
            cols.remove(col)

    # Réinsertion aux bons indices
    cols = [cols[0], 'publication_type', 'eth_bibliometric'] + cols[1:]

    # Réaffectation de l'ordre au DataFrame
    df_main = df_main[cols]


    # ---------------------------------------------
    # Calculer les colonnes oa_status et oa_type
    # ---------------------------------------------

    # --- Map 'best_oa_version' to 'version'

    def resolve_resource_version(row):
        version_map = {
            "http://purl.org/coar/version/c_71e4c1898caa6e32": "submittedVersion",
            "http://purl.org/coar/version/c_970fb48d4fbd8a85": "publishedVersion",
            "http://purl.org/coar/version/c_ab4af688f83e57aa": "acceptedVersion",
            "http://purl.org/coar/version/c_be7fb7dd8ff6fe43": "copyright",
            "http://purl.org/coar/version/c_e19f295774971610": "correctedVersion"
        }

        def normalize(val):
            if pd.isna(val) or not val:
                return None
            val = str(val).strip()
            return version_map.get(val, val)

        candidates = [normalize(row.get('best_oa_version'))]

        priority = ['publishedVersion', 'acceptedVersion', 'submittedVersion', 'copyright', 'correctedVersion']
        seen = set()
        unique = [v for v in candidates if v and not (v in seen or seen.add(v))]

        for choice in priority:
            if choice in unique:
                return choice

        return None

    df_main['version'] = df_main.apply(resolve_resource_version, axis=1)


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
        "null": "n/a"
    }

    license_priority = [
        "cc-by", "cc-by-nc", "cc-by-nc-nd", "cc-by-nc-sa",
        "cc-by-nd", "cc-by-sa", "gpl-v3", "mit",
        "other-oa", "public-domain", "publisher-specific-oa", "n/a"
    ]

    def normalize_license(val):
        if pd.isna(val):
            return "n/a"
        val = str(val).strip().lower()
        return license_map.get(val, val)

    def build_license_condition(row):
        val = normalize_license(row.get('best_oa_license', ''))
        return val if val in license_priority else "n/a"

    df_main['license'] = df_main.apply(build_license_condition, axis=1)

    # Initialize OA columns
    df_main['oa_status'] = 'Closed'
    df_main['oa_type'] = 'Closed'
    already_set = pd.Series(False, index=df_main.index)

    # DIAMOND OA
    diamond_mask = (
        (df_main['best_oa_is_oa'] == True) &
        (df_main['oa_status'].str.lower() == 'diamond') &
        (~already_set)
    )
    df_main.loc[diamond_mask, 'oa_status'] = 'Open'
    df_main.loc[diamond_mask, 'oa_type'] = 'Diamond'
    already_set |= diamond_mask

    # GOLD OA
    gold_mask = (
        (df_main['best_oa_is_oa'] == True) &
        (
            (df_main['oa_status'].str.lower() == 'gold') |
            (
                (df_main['publication_type'] == 'book-chapter') &
                (df_main['license'].isin([
                    'cc-by', 'cc-by-nc', 'cc-by-nc-nd', 'cc-by-nc-sa',
                    'cc-by-nd', 'cc-by-sa', 'cc0', 'public-domain'
                ]))
            )
        ) &
        (~already_set)
    )
    df_main.loc[gold_mask, 'oa_status'] = 'Open'
    df_main.loc[gold_mask, 'oa_type'] = 'Gold'
    already_set |= gold_mask

    # HYBRID OA
    hybrid_mask = (
        (df_main['best_oa_is_oa'] == True) &
        (df_main['license'].isin([
            'cc-by', 'cc-by-nc', 'cc-by-nc-nd', 'cc-by-nc-sa',
            'cc-by-nd', 'cc-by-sa', 'cc0', 'public-domain'
        ])) &
        (df_main['version'] == 'publishedVersion') &
        (~df_main['primary_source_type'].isin([None, np.nan, 'NULL', 'repository'])) &
        (~already_set)
    )
    df_main.loc[hybrid_mask, 'oa_status'] = 'Open'
    df_main.loc[hybrid_mask, 'oa_type'] = 'Hybrid'
    already_set |= hybrid_mask

    # GREEN OA
    green_mask = (
        (df_main['best_oa_is_oa'] == True) &
        (df_main['primary_source_type'] == 'repository') &
        (df_main['version'].isin(['acceptedVersion', 'publishedVersion'])) &
        (~already_set)
    )
    df_main.loc[green_mask, 'oa_status'] = 'Open'
    df_main.loc[green_mask, 'oa_type'] = 'Green'
    already_set |= green_mask

    # Reorder OA columns at the end
    cols = [c for c in df_main.columns if c not in ['oa_status', 'oa_type']]
    df_main = df_main[cols + ['oa_status', 'oa_type']]

    df_main

    return (df_main,)


@app.cell
def _(pd):
    # Load openalex authors
    df_authors = pd.read_csv("./eth_bibliometric/processed_data/2013-2025_openalex_with_all_authors.csv")
    df_authors
    return (df_authors,)


@app.cell
def _(pd):
    # Load EPFL reconciled openalex authors
    df_epfl_authors = pd.read_csv("./eth_bibliometric/processed_data/2013-2025_openalex_not_in_infoscience_epfl_authors.csv")
    df_epfl_authors
    return (df_epfl_authors,)


@app.cell
def _(df_authors, df_epfl_authors, df_main, pd):
    # ---------------------------------------------
    # Enrichissement du dataframe retained publications avec auteurs EPFL
    # ---------------------------------------------

    # Étape 1 — Filtrer les auteurs EPFL avec sciper_id renseigné
    epfl_reconciled = df_epfl_authors[df_epfl_authors['sciper_id'].notnull() & (df_epfl_authors['sciper_id'] != '')]

    # Étape 2 — Récupérer les row_id des publications concernées
    valid_row_ids = epfl_reconciled['row_id'].unique()

    # Étape 3 — Filtrer df_main selon :
    #    - au moins un auteur EPFL réconcilié
    #    - et eth_bibliometric == True
    df_filtered = df_main[
        (df_main['row_id'].isin(valid_row_ids)) &
        (df_main['eth_bibliometric'] == True)
    ].copy()

    # Étape 4 — Créer les colonnes d'auteurs
    # Liste complète des auteurs pour chaque publication
    authors_all = df_authors.groupby('row_id')['author'].apply(list)

    # Liste des auteurs EPFL avec SCIPER pour chaque publication
    authors_epfl = epfl_reconciled.groupby('row_id')['author'].apply(list)

    # ORCID des auteurs EPFL avec SCIPER
    orcids_epfl = epfl_reconciled.groupby('row_id')['orcid_id'].apply(
        lambda x: [oid for oid in x if pd.notnull(oid) and oid != '']
    )

    # Étape 5 — Ajout des colonnes au dataframe filtré
    df_filtered['authors_all'] = df_filtered['row_id'].map(authors_all).apply(lambda x: x if isinstance(x, list) else [])
    df_filtered['authors_institution'] = df_filtered['row_id'].map(authors_epfl).apply(lambda x: x if isinstance(x, list) else [])
    df_filtered['orcid_institution'] = df_filtered['row_id'].map(orcids_epfl).apply(lambda x: x if isinstance(x, list) else [])

    df_filtered = df_filtered.rename(columns={
        'pubyear': 'year',
        'primary_container_title': 'journal'
    })

    df_filtered
    return df_filtered, epfl_reconciled


@app.cell
def _(df_authors, df_epfl_authors, df_main, epfl_reconciled, pd):
    # ---------------------------------------------
    # Enrichissement du dataframe rejected publications avec auteurs
    # ---------------------------------------------

    # 1. Identifie les row_id avec au moins un auteur EPFL réconcilié
    reconciled_row_ids = epfl_reconciled['row_id'].unique()

    # 2. Créer le DataFrame df_rejected avec deux critères combinés :
    #    - publications sans auteur EPFL réconcilié
    #    - OU publications non bibliométriques
    df_rejected = df_main[
        (~df_main['row_id'].isin(reconciled_row_ids)) |  # pas d'auteur réconcilié
        (df_main['eth_bibliometric'] == False)           # ou non bibliométrique
    ].copy()

    # 3. Créer la liste de tous les auteurs
    rej_authors_all = df_authors.groupby('row_id')['author'].apply(list)

    # 4. Trouver les auteurs EPFL non réconciliés
    epfl_unreconciled = df_epfl_authors[df_epfl_authors['sciper_id'].isnull() | (df_epfl_authors['sciper_id'] == '')]
    authors_epfl_unreconciled = epfl_unreconciled.groupby('row_id')['author'].apply(list)

    # ORCID des auteurs EPFL avec SCIPER
    orcids_epfl_unreconciled = epfl_unreconciled.groupby('row_id')['orcid_id'].apply(
        lambda x: [oid for oid in x if pd.notnull(oid) and oid != '']
    )

    # 5. Ajouter les colonnes d'auteurs
    df_rejected['authors_all'] = df_rejected['row_id'].map(rej_authors_all).apply(lambda x: x if isinstance(x, list) else [])
    df_rejected['authors_institution'] = df_rejected['row_id'].map(authors_epfl_unreconciled).apply(lambda x: x if isinstance(x, list) else [])
    df_rejected['orcid_institution'] = df_rejected['row_id'].map(orcids_epfl_unreconciled).apply(lambda x: x if isinstance(x, list) else [])

    df_rejected = df_rejected.rename(columns={
        'pubyear': 'year',
        'primary_container_title': 'journal'
    })

    df_rejected
    return (df_rejected,)


@app.cell
def _(datetime, pd):
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
        # Colonnes attendues
        ordered_columns = [
            "title",
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
            "license",
            "version",
        ]

        # Vérifier les colonnes manquantes
        missing = [col for col in ordered_columns if col not in df.columns]
        if missing:
            print("⚠️ Colonnes manquantes :", missing)

        # Reindexer selon l’ordre souhaité
        final_df = df.reindex(columns=ordered_columns)

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
        filename = f"./eth_bibliometric/curated_data/{start_year}-{end_year}_{filename_prefix}_{creation_date_str}.csv"

        # Exporter au format CSV
        final_df.to_csv(filename, index=False)
        print(f"✅ Exporté : {filename}")

        return final_df
    return (export_bibliometric_dataframe,)


@app.cell
def _(df_filtered, df_rejected, export_bibliometric_dataframe):
    final_df = export_bibliometric_dataframe(df_filtered, filename_prefix="eth_bibliometric_dataset_epfl_openalex-only", start_year=2014, end_year=2023)
    final_rejected = export_bibliometric_dataframe(df_rejected, filename_prefix="eth_bibliometric_dataset_epfl_openalex-only-discarded", start_year=2014, end_year=2023)
    return


if __name__ == "__main__":
    app.run()

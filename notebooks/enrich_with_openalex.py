import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    import os
    import re

    import pandas as pd
    sys.path.append(os.path.abspath(".."))
    from data_pipeline.enricher import OpenAlexProcessor

    return OpenAlexProcessor, pd, re


@app.cell
def _():
    from dspace.dspace_rest_client.client import DSpaceClient
    return (DSpaceClient,)


@app.cell
def _(DSpaceClient):
    d = DSpaceClient()
    authenticated = d.authenticate()
    if not authenticated:
        print('Error logging in! Giving up.')
        exit(1)
    return (d,)


@app.cell
def _(re):
    def extract_clean_doi(raw_doi: str) -> str:
        """
        Nettoie une chaîne potentiellement URL-ifiée en DOI pur.

        Args:
            raw_doi (str): DOI brut ou en URL (ex: "https://doi.org/10.1103/physrevd.111.l091101")

        Returns:
            str: DOI normalisé (ex: "10.1103/physrevd.111.l091101")
        """
        if not isinstance(raw_doi, str):
            return ""

        # Supprime les préfixes d'URL si présents
        cleaned = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", raw_doi.strip(), flags=re.IGNORECASE)

        # Optionnel : valider que le résultat commence bien par "10."
        return cleaned if cleaned.startswith("10.") else ""
    return (extract_clean_doi,)


@app.cell
def _(extract_clean_doi, pd):
    def get_items(d, query, size=100, max_pages=1):
        """
        Fetch and structure research outputs (publications) linked to a specific organizational unit.

        Parameters:
        d (object): DSpace API client object with a `search_objects` method.
        unit_uuid (str): The UUID of the unit/authority to search publications for.
        size (int): Number of results to fetch (default: 100).

        Returns:
        pd.DataFrame: A DataFrame containing structured metadata for each publication.
        """
        # Build the search query
        query = f"{query}"
        configuration = "researchoutputs"
        sort_order = "dc.date.accessioned,DESC"

        # Execute the search
        routputs = d.search_objects(
            query=query,
            page=0,
            size=size,
            sort=sort_order,
            dso_type="item",
            configuration=configuration,
            max_pages=max_pages
        )

        # Parse the search results into structured records
        researchoutput = []

        for r in routputs:
            authors_with_authority = [
                x.get('value') for x in r.metadata.get('dc.contributor.author', [])
                if x.get('authority') is not None
            ]

            doi = r.metadata.get('dc.identifier.doi', [{}])[0].get('value')
            researchoutput.append({
                'uuid': r.uuid,
                'title': r.metadata.get('dc.title', [{}])[0].get('value'),
                'authors_all': set(x.get('value') for x in r.metadata.get('dc.contributor.author', [{}])),
                'authorsid_institution': set(x.get('value') for x in r.metadata.get('cris.virtual.sciperId', [{}])),
                'authors_institution': authors_with_authority,
                'orcid_institution': [
        x.get('value') for x in r.metadata.get('cris.virtual.orcid', [])
        if x.get('value') and x.get('value') != "#PLACEHOLDER_PARENT_METADATA_VALUE#"
    ],
                'type': r.metadata.get('dc.type', [{}])[0].get('value'),
                'doi': extract_clean_doi(doi),
                'access-level': r.metadata.get('datacite.rights', [{}])[0].get('value'),
                'legacy_version': r.metadata.get('datacite.rights', [{}])[0].get('value'),
                'version': r.metadata.get('oaire.version', [{}])[0].get('value'),
                'license': r.metadata.get('oaire.licenseCondition', [{}])[0].get('value'),
                'journal': r.metadata.get('dc.relation.journal', [{}])[0].get('value'),
                'issns': [
        x.get('value') for x in r.metadata.get('dc.relation.issn', [])
        if x.get('value') is not None
    ],
                'publisher': r.metadata.get('dc.publisher', [{}])[0].get('value'),
                'container': r.metadata.get('dc.relation.ispartof', [{}])[0].get('value'),
                'series': r.metadata.get('dc.relation.ispartofseries', [{}])[0].get('value'),
                'volume': r.metadata.get('oaire.citation.volume', [{}])[0].get('value'),
                'issue': r.metadata.get('oaire.citation.issue', [{}])[0].get('value'),
                'artnum': r.metadata.get('oaire.citation.articlenumber', [{}])[0].get('value'),
                'issued': r.metadata.get('dc.date.issued', [{}])[0].get('value'),
                'created': r.metadata.get('dc.date.created', [{}])[0].get('value'),
            })

        # Convert to DataFrame
        return pd.DataFrame(researchoutput)
    return (get_items,)


@app.cell
def _(d, get_items):
    query ="dateIssued.year:2023 (types_authority:(*c_6501) OR types_authority:(*c_2df8fbb1) OR types_authority:(*c_5794) OR types_authority:(*c_dcae04bc) OR types_authority:(*c_c94f) OR types_authority:(*c_beb9)) AND epfl.peerreviewed:REVIEWED epfl.writtenAt:EPFL"
    # query ="dc.identifier.doi:(10.1126/sciadv.adt7195) (types:(conference) OR types:(journal))"
    df = get_items(d, query, size=100, max_pages=None)
    df
    return (df,)


@app.cell
def _(OpenAlexProcessor, df):
    processor = OpenAlexProcessor(df)
    df_enriched = processor.process(return_df=True)
    df_enriched
    return


if __name__ == "__main__":
    app.run()

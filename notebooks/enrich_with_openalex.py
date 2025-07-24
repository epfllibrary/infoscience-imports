import marimo

__generated_with = "0.14.12"
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
        Extracts and normalizes a DOI from a raw string that may be URL-encoded.

        Args:
            raw_doi (str): A raw DOI string, potentially in URL form 
                           (e.g., "https://doi.org/10.1103/physrevd.111.l091101").

        Returns:
            str: A normalized DOI string (e.g., "10.1103/physrevd.111.l091101"), 
                 or an empty string if the input is invalid or improperly formatted.
        """
        if not isinstance(raw_doi, str):
            return ""

        # Remove DOI URL prefixes if present
        cleaned = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", raw_doi.strip(), flags=re.IGNORECASE)

        # Ensure the cleaned string starts with a valid DOI prefix
        return cleaned if cleaned.startswith("10.") else ""
    return (extract_clean_doi,)


@app.cell
def _(extract_clean_doi, pd):
    def get_items(d, query, size=100, max_pages=1):
        """
        Fetch and structure research outputs (publications) linked to a specific organizational unit.

        Parameters:
        d (object): DSpace API client object with a `search_objects` method.
        query (str): The search query to filter results.
        size (int): Number of results to fetch per page (default: 100).
        max_pages (int): Maximum number of result pages to retrieve (default: 1).

        Returns:
        pd.DataFrame: A DataFrame containing structured metadata for each publication.
        """
        # Build the search configuration
        query = f"{query}"
        configuration = "researchoutputs"
        sort_order = "dc.date.accessioned,DESC"

        # Execute the search query using the DSpace API
        routputs = d.search_objects(
            query=query,
            page=0,
            size=size,
            sort=sort_order,
            dso_type="item",
            configuration=configuration,
            max_pages=max_pages
        )

        researchoutput = []

        for r in routputs:
            # Authors affiliated with the institution (with authority ID)
            authors_with_authority = [
                x.get('value') for x in r.metadata.get('dc.contributor.author', [])
                if x.get('authority') is not None
            ]

            # DOI cleanup
            doi = r.metadata.get('dc.identifier.doi', [{}])[0].get('value')

            # Parse series information (may contain multiple values)
            raw_series_entries = [x.get('value') for x in r.metadata.get('dc.relation.ispartofseries', []) if x.get('value')]
            series_titles = []
            series_volumes = []

            for raw in raw_series_entries:
                if ";" in raw:
                    parts = [p.strip() for p in raw.split(';', 1)]
                    if len(parts) == 2:
                        series_titles.append(parts[0])
                        series_volumes.append(parts[1])
                    else:
                        series_titles.append(parts[0])
                else:
                    series_titles.append(raw.strip())

            # Combine volumes from series and oaire.citation.volume
            main_volume = r.metadata.get('oaire.citation.volume', [{}])[0].get('value')
            all_volumes = [v for v in [main_volume] if v] + series_volumes
            final_volume = "; ".join(all_volumes) if all_volumes else None

            # Construct journal_series from journal and parsed series titles
            journal_titles = [x.get('value') for x in r.metadata.get('dc.relation.journal', []) if x.get('value')]
            journal_series = list(set(journal_titles + series_titles))

            researchoutput.append({
                'uuid': r.uuid,
                'handle': r.handle,
                'doi': extract_clean_doi(doi),
                'title': r.metadata.get('dc.title', [{}])[0].get('value'),

                # Authors and institutional identifiers
                'authors': [x.get('value') for x in r.metadata.get('dc.contributor.author', [{}]) if x.get('value')],
                'authorsid_institution': [x.get('value') for x in r.metadata.get('cris.virtual.sciperId', [{}]) if x.get('value')],
                'authors_institution': authors_with_authority,
                'orcid_institution': [
                    x.get('value') for x in r.metadata.get('cris.virtual.orcid', [])
                    if x.get('value') and x.get('value') != "#PLACEHOLDER_PARENT_METADATA_VALUE#"
                ],

                # Bibliographic information
                'type': r.metadata.get('dc.type', [{}])[0].get('value'),
                'publishedin': r.metadata.get('dc.relation.ispartof', [{}])[0].get('value'),
                'journalorseries': journal_series,
                'issn': list(set(
                    [x.get('value') for x in r.metadata.get('dc.relation.issn', []) if x.get('value')] +
                    [x.get('value') for x in r.metadata.get('dc.relation.serieissn', []) if x.get('value')]
                )),
                'isbn': list(set(
                    [x.get('value') for x in r.metadata.get('dc.identifier.isbn', []) if x.get('value')] +
                    [x.get('value') for x in r.metadata.get('dc.relation.isbn', []) if x.get('value')]
                )),
                'publisher': r.metadata.get('dc.publisher', [{}])[0].get('value'),

                # Citation metadata
                'volume': final_volume,
                'issue': r.metadata.get('oaire.citation.issue', [{}])[0].get('value'),
                'artnum': r.metadata.get('oaire.citation.articlenumber', [{}])[0].get('value'),
                'issued': r.metadata.get('dc.date.issued', [{}])[0].get('value'),
                'created': r.metadata.get('dc.date.created', [{}])[0].get('value'),
                'available': r.metadata.get('dc.date.available', [{}])[0].get('value'),


                # Access rights and versioning
                'access-level': r.metadata.get('datacite.rights', [{}])[0].get('value'),
                'embargo': r.metadata.get('datacite.available', [{}])[0].get('value'),
                'license': r.metadata.get('oaire.licenseCondition', [{}])[0].get('value'),
                'version': r.metadata.get('epfl.publication.version', [{}])[0].get('value'),
                'legacy_version': r.metadata.get('oaire.version', [{}])[0].get('value'),            	
            })

        # Convert structured results to a DataFrame
        return pd.DataFrame(researchoutput)

    return (get_items,)


@app.cell
def _(d, get_items):
    query ="(dateIssued.year:2021 OR dateIssued.year:2022 OR dateIssued.year:2023 OR dateIssued.year:2024) (types:(book) OR types_authority:(*c_6501) OR types_authority:(*c_2df8fbb1) OR types_authority:(*c_5794) OR types_authority:(*c_dcae04bc) OR types_authority:(*c_c94f) OR types_authority:(*c_beb9) OR types_authority:(*c_f744)) AND epfl.peerreviewed:REVIEWED epfl.writtenAt:EPFL"

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

import marimo

__generated_with = "0.13.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import sys
    import os
    import pandas as pd
    sys.path.append(os.path.abspath(".."))
    from data_pipeline.enricher import PublicationProcessor
    return PublicationProcessor, pd


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
def _(pd):
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
            researchoutput.append({
                'uuid': r.uuid,
                'title': r.metadata.get('dc.title', [{}])[0].get('value'),
                'type': r.metadata.get('dc.type', [{}])[0].get('value'),
                'doi': r.metadata.get('dc.identifier.doi', [{}])[0].get('value'),
                'access-level': r.metadata.get('datacite.rights', [{}])[0].get('value'),
                'journal': r.metadata.get('dc.relation.journal', [{}])[0].get('value'),
                'partOf': r.metadata.get('dc.relation.ispartof', [{}])[0].get('value'),
                'issued': r.metadata.get('dc.date.issued', [{}])[0].get('value'),
                'created': r.metadata.get('dc.date.created', [{}])[0].get('value'),
            })

        # Convert to DataFrame
        return pd.DataFrame(researchoutput)
    return (get_items,)


@app.cell
def _(d, get_items):
    query ="datacite.rights:(metadata-only) dateIssued.year:2025 dc.identifier.doi:* (types:(conference) OR types:(journal))"
    df = get_items(d, query, size=100, max_pages=None)
    df
    return (df,)


@app.cell
def _(PublicationProcessor, df):
    processor = PublicationProcessor(df)
    df_enriched = processor.process(return_df=True)
    df_enriched
    return


if __name__ == "__main__":
    app.run()

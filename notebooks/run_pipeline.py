import marimo

__generated_with = "0.12.4"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""# Import from External Sources to Infoscience""")
    return


@app.cell
def _():
    import sys
    import os
    sys.path.append(os.path.abspath(".."))
    from data_pipeline.main import main
    from data_pipeline.reporting import GenerateReports
    return GenerateReports, main, os, sys


@app.cell
def _():
    start = "2025-05-01"
    end = "2025-05-13"

    # author_ids = ["DYK-7080-2022", "23008979400"]
    author_ids = None

    # wos_query = ("AI=(DYK-7080-2022) AND OG=(Ecole Polytechnique Federale de Lausanne) AND PY=2024")
    wos_query = None

    # scopus_query = "AU-ID ( 23008979400 ) AND AFFIL ( ecole polytechnique federale de lausanne ) AND PUBYEAR > 2023"
    scopus_query = None

    # crossref_query = "EPFL"
    crossref_query = None

    # openalex_query = "authorships.institutions.lineage:i5124864"
    openalex_query = None
    return (
        author_ids,
        crossref_query,
        end,
        openalex_query,
        scopus_query,
        start,
        wos_query,
    )


@app.cell
def _(
    author_ids,
    crossref_query,
    end,
    main,
    openalex_query,
    scopus_query,
    start,
    wos_query,
):
    custom_queries = {}

    if wos_query:
        custom_queries["wos"] = wos_query
    if scopus_query:
        custom_queries["scopus"] = scopus_query
    if crossref_query:
        custom_queries["crossref"] = crossref_query
    if openalex_query:
        custom_queries["openalex"] = openalex_query

    queries = custom_queries if custom_queries else {}

    results = main(start_date=start, end_date=end, queries=queries, authors_ids=author_ids)
    return custom_queries, queries, results


if __name__ == "__main__":
    app.run()

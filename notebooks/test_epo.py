import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    import pandas as pd
    import json


    sys.path.append(os.path.abspath(".."))
    return (pd,)


@app.cell
def _():
    from clients.epo_ops_client import EPOClient

    client = EPOClient()
    print("Client initialisé:", client)
    return (client,)


@app.cell
def _(client):
    rec = client.fetch_record_by_unique_id("US20260027354", format="digest")
    rec
    return


@app.cell
def _(client):
    cql = '(pa all "ECOLE POLYTECHNIQUE FED LAUSANNE EPFL" AND pd>20251231) NOT cl any "US"'
    ids = client.fetch_ids(cql=cql, per_page=5, max_records=10, range_begin=1)
    ids
    return (ids,)


@app.cell
def _(client):
    client.last_response.text
    return


@app.cell
def _(client, ids):
    recs = client.fetch_records_by_ids(ids, format="ifs3")
    return (recs,)


@app.cell
def _(recs):
    recs
    return


@app.cell
def _(client, pd):
    recs_ifs3 = client.fetch_records(
        cql='(pa all "ECOLE POLYTECH* FED LAUSANNE*" AND pd>20241231) NOT cl any "US"',
        format="digest",
        per_page=50,
        max_records=None,
        group_by_family=True,

    )
    recs_ifs3
    df = pd.DataFrame(recs_ifs3)
    df
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

# This software is licenced under the BSD 3-Clause licence
# available at https://opensource.org/licenses/BSD-3-Clause
# and described in the LICENCE file in the root of this project

"""
Example using the dspace.py API client library 
"""
from dspace.client import DSpaceClient

# Instantiate DSpace client
d = DSpaceClient()

# Authenticate against the DSpace client
authenticated = d.authenticate()
if not authenticated:
    print(f'Error logging in! Giving up.')
    exit(1)

## Retrieve record from wos
wos_id = "WOS:000760256600001"
external_records = d.fetch_external_records(source="wos", query=wos_id)
print(external_records)
print(len(external_records))

## Detect duplicate
query = f"(itemidentifier:{str(wos_id[4:]).strip()})"
dsos = d.search_objects(
    query=query, page=0, size=1, dso_type="item", configuration="researchoutputs"
)
for dso in dsos:
    print(dso.metadata.get("dc.title"))

## Create workspace from external source
collection_id = "8a8d3310-6535-4d3a-90b6-2a4428097b5b"
response = d.create_workspaceitem_from_external_source("wos", wos_id, collection_id)

workspace_id = response.get("id")
print(workspace_id)

## Update workspace item
units = [{"acro": "SISB-AIR"}, {"acro": "SISB-SOAR"}]
sponsorships = []
for unit in units:
    sponsorships.append(
        {
            "value": unit.get("acro"),
            "language": None,
            "authority": f"will be referenced::ACRONYM::{unit.get('acro')}",
            "securityLevel": 0,
            "confidence": 400,
            "place": 0,
        }
    )

patch_operations = [
    {
        "op": "add",
        "path": "/sections/article_details/dc.language.iso",
        "value": [
            {
                "value": "en",
                "language": None,
                "authority": None,
                "display": "English",
                "securityLevel": 0,
                "confidence": -1,
                "place": 0,
                "otherInformation": None,
            }
        ],
    },
    {
        "op": "add",
        "path": "/sections/article_details/dc.description.sponsorship",
        "value": sponsorships,
    },
    {
        "op": "add",
        "path": "/sections/article_details/epfl.peerreviewed",
        "value": [
            {
                "value": "REVIEWED",
                "language": None,
                "authority": None,
                "display": "REVIEWED",
                "securityLevel": 0,
                "confidence": -1,
                "place": 0,
                "otherInformation": None,
            }
        ],
    },
    {
        "op": "add",
        "path": "/sections/article_details/epfl.writtenAt",
        "value": [
            {
                "value": "EPFL",
                "language": None,
                "authority": None,
                "display": "EPFL",
                "securityLevel": 0,
                "confidence": -1,
                "place": 0,
                "otherInformation": None,
            }
        ],
    },
    {"op": "add", "path": "/sections/license/granted", "value": "true"},
]

update_response = d.update_workspaceitem(workspace_id, patch_operations) 

## GET FULL TEXT FROM UPW
if update_response:
    ft = d.import_unpaywall_fulltext(workspace_id)
    if ft:
        print("import unpaywall réussie")
    else:
        print("Échec de l'import unpaywall.")
    ## PASS DRAFT TO WORKFLOW
    wf_response = d.create_workflowitem(workspace_id)

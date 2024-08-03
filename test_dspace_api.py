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


wos_id = "WOS:000817300200003"
collection_id = "89c8823b-78c9-45c0-8ba8-b381922ee0a5"
query = f"(itemidentifier:{str(wos_id[4:]).strip()})"


dsos = d.search_objects(
    query=query, page=0, size=1, dso_type="item", configuration="researchoutputs"
)
for dso in dsos:
    print(dso.metadata.get("dc.title"))

response = d.create_workspaceitem_from_external_source("wos", wos_id, collection_id)

workspace_id = response.get("id")
print(workspace_id)

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


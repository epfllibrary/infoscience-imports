# Infoscience imports documentation

## General workflow : Modular Python Scripts

The data pipeline is breaked down into separate Python scripts, each responsible for a specific task

1. **Harvesting**: Regularly fetch publications from Wos and Scopus based on a publication date range.
2. **Deduplication**: Merge the fetched data into a single dataframe with unique dedupicated metadata.
   Deduplicate on the existing publicatioins in Infoscience
3. **Enrichment**: Separate authors and affiliations into a second dataframe and enrich it with local laboratory and author informations using api.epfl.ch 
4. **Integration**: Push the new metadata in Infoscience using the Dspace API client to create/enrich new works and persons entities.


**`main.py`** : acts as an orchestrator


### Python scripts

They are stored in data_pipeline folder.

1. `harvester.py`: Fetch publications from different sources (Wos and Scopus for the moment). 
   Each source is harvested with a dedicated client, one client by sources in the `clients` folder. They all are runned in a `Harvester` class that can be easily extended to support multiple sources. This approach allows to separate the harvesting logic from the source-specific implementation details.
2. `deduplicator.py`: Merge and deduplicate the fetched data.

   The final dataframe contains following metadata :

   `source` : source KB (wos, scopus)

   `internal_id`: publication Id in the source KB (WOS:xxxx, SCOPUS_ID:xxxx)

   `doi`

   `title`

   `doi`

   `doctype`: the doctype in the source KB

   `pubyear`

   `ifs3_doctype`: the Infoscience doctype

   `ifs3_collection_id`: the Infoscience collection Id (depending on doctype)

   `authors, and affiliations`

3. `enricher.py`: Enrich the authors and affiliations dataframe with local laboratory information.
4. `integrator.py`: Push the metadata into Dspace-CRIS using the Dspace API client.

**`main.py`** : chains the operations.
**Contains the deafult queries for the external sources. These default queries can be overwritten 

### Test the Python scripts

In `run_pipeline.ipynb`

## Detailed pipeline

```
data_pipeline/
│
├── harvester.py
│   ├── class Harvester
│   │   ├── def __init__(self, source_name)
│   │   ├── def fetch_and_parse_publications(self)
│   │   └── def harvest(self)
│   │
│   └── class WosHarvester(Harvester)
│   |   ├── def __init__(self)
│   |   └── def fetch_and_parse_publications(self)
│   |
│   └── class ScopusHarvester(Harvester)
│       ├── def __init__(self)
│       └── def fetch_and_parse_publications(self)
│
├── deduplicator.py
│   ├── class DataFrameProcessor
│       ├── def __init__(self)
│       ├── def deduplicate_dataframes(self)
│       └── def deduplicate_infoscience(self, df)
|       ├── def generate_main_dataframes(self,df)
│
└── main.py
    ├── def main()

```

### Clients

## Mappings

All mappings are in `mappings.py`

Internal script used (one shot) to create the mapping dictionary between Infoscience collection labels and Infoscience collection id

```
url = "https://infoscience.epfl.ch/server/api/core/collections"
params = {"page":0, "size": 25}
response = requests.get(url, params=params).json()
#[{"collection_uuid":x["uuid"],"entity_type":x["metadata"]["dc.title"][0]["value"]} for x in response["_embedded"]["collections"]]
collections_mapping = {}
for x in response["_embedded"]["collections"]:
    collections_mapping[x["metadata"]["dc.title"][0]["value"]] = x["uuid"]
collections_mapping
```

Returns

```
{'Patents': 'ce5a1b89-cfb3-40eb-bdd2-dcb021e755b7',
 'Projects': '49ec7e96-4645-4bc0-a015-ba4b81669bbc',
 'Teaching Materials': 'c7e018d4-2349-46dd-a8a4-c32cf5f5f9a1',
 'Images, Videos, Interactive resources, and Design': '329f8cd3-dc1a-4228-9557-b27366d71d41',
 'Newspaper, Magazine, or Blog post': '971cc7fa-b177-46e3-86a9-cfac93042e9d',
 'Funding': '8b185e36-0f99-4669-9a46-26a19d4f3eab',
 'Other': '0066acb2-d5c0-49a0-b273-581df34961cc',
 'Datasets and Code': '33a1cd32-7980-495b-a2bb-f34c478869d8',
 'Student works': '305e3dad-f918-48f6-9309-edbeb7cced14',
 'Units': 'bc85ee71-84b0-4f78-96a1-bab2c50b7ac9',
 'Contents': 'e8dea11e-a080-461b-82ee-6d9ab48404f3',
 'Virtual collections': '78f331d1-ee55-48ef-bddf-508488493c90',
 'EPFL thesis': '4af344ef-0fb2-4593-a234-78d57f3df621',
 'Reports, Documentation, and Standards': 'd5ec2987-2ee5-4754-971b-aca7ab4f9ab7',
 'Preprints and Working Papers': 'd8dada3a-c4bd-4c6f-a6d7-13f1b4564fa4',
 'Books and Book parts': '1a71fba2-2fc5-4c02-9447-f292e25ce6c1',
 'Persons': '6acf237a-90d7-43e2-82cf-c3591e50c719',
 'Events': '6e2af01f-8b92-461e-9d08-5e1961b9a97b',
 'Conferences, Workshops, Symposiums, and Seminars': 'e91ecd9f-56a2-4b2f-b7cc-f03e03d2643d',
 'Journals': '9ada82da-bb91-4414-a480-fae1a5c02d1c',
 'Journal articles': '8a8d3310-6535-4d3a-90b6-2a4428097b5b'}
```





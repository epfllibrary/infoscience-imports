# Infoscience imports documentation

## General workflow : Modular Python Scripts based on tailored Python clients

The data pipeline is breaked down into separate Python scripts, each responsible for a specific task

1. **Harvesting**: Regularly fetch publications from Wos and Scopus based on a publication date range.
2. **Deduplication**: Merge the fetched data into a single dataframe with unique dedupicated metadata.
   Deduplicate on the existing publicatioins in Infoscience
3. **Enrichment**: Separate authors and affiliations into a second dataframe and enrich it with local laboratory and author informations using api.epfl.ch 
4. **Loading**: Push the new metadata in Infoscience using the Dspace API client to create/enrich new works and persons entities.


**`main.py`** : acts as an orchestrator

### Python scripts

Located in `./data_pipeline` folder.

1. `harvester.py`: Fetch publications from different sources (Wos and Scopus for the moment). 
   Each source is harvested with a dedicated client, one client by sources in the `clients` folder. They all are runned in a `Harvester` class that can be easily extended to support multiple sources. This approach allows to separate the harvesting logic from the source-specific implementation details.
2. `deduplicator.py`: Merge and deduplicate the fetched data.

   The final dataframe contains following metadata :

   `source` : source KB (wos, scopus)

   `internal_id`: publication Id in the source KB (WOS:xxxx, eid)

   `doi`

   `title`

   `doi`

   `doctype`: the doctype in the source KB

   `pubyear`

   `ifs3_doctype`: the Infoscience doctype

   `ifs3_collection_id`: the Infoscience collection Id (depending on doctype)

   `authors, and affiliations`

3. `enricher.py`: Enrich the authors and affiliations dataframe with local laboratory information (for authors) and Unpaywall OA attributes (for publications).
4. `loader.py`: Push the metadata into Dspace-CRIS using the Dspace API client.

### Orchestrator 

**`main.py`** in `./data_pipeline` folder : chains the operations.

**Contains the default queries for the external sources. These default queries can be overwritten by passing new queries as parameter of the main function**

For example :

```
df_metadata, df_authors, df_epfl_authors, df_unloaded = main(start_date="2023-01-01", end_date="2023-12-31")
```

Or

```
custom_queries = {
    "wos": "OG=(Your Custom Query for WOS)",
    "scopus": "AF-ID(Your Custom Scopus ID)",
    "openalex": "YOUR_CUSTOM_OPENALEX_QUERY",
    "zenodo": "YOUR_CUSTOM_ZENODO_QUERY"
}
df_metadata, df_authors, df_epfl_authors, df_unloaded = main(start_date="2023-01-01", end_date="2023-12-31", queries=custom_queries)
```

### Clients

Located in `./clients` folder.

Each source of metadata is harvested and parsed by a specific client, before the data being processed in the python scripts.

1. **wos_client_v2.py**: contains the WosClient with all methods to parse the results of the WoS search API
2. **scopus_client_v2.py**: contains the ScopusClient with all methods to parse the results of the Scopus search API
3. **api_epfl_client.py** : contains the ApiEpflClient for local EPFL informations retrieving (author sciper Id, accreds and units)
4. **unpaywall_client.py** : contains the UnpaywallClient with methos to request teh Unpaywall API
5. **dsapce_client_wrapper.py**: contains the DSpaceClientWrapper with methods to search and update objects in Dspace using the Dspace Rest Client

Others : some tests with Orcid API and Istex API for managing authors names.

### Mappings

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

### Configs

Some pipeline's configurations are in `config.py`

- default_queries (on datasources) : queries used by the harvesters
- source_order : order of preferred item (according datasource) for deduplicate operation
- scopus_epfl_afids: list of structures Scopus Ids in Scopus referential (used for discriminate EPFL authors for Scopus datasource)
- LICENSE_CONDITIONS : dict of conditions for parsing the best_oa_location from Unpaywall

## Tests and examples

**Documentation on using clients and scripts** : `documentation_and_examples.ipynb`

**To test the Python scripts** : `demo_pipeline.ipynb`

## Airflow

### Docker installation

```
docker build . -f Dockerfile-airflow --pull --tag airflow:custom
docker run -it --name=airflow -p 8081:8080 -v $PWD/clients:/opt/airflow/dags/clients  -v $PWD/data_pipeline:/opt/airflow/dags/data_pipeline  -v $PWD/imports_dag.py:/opt/airflow/dags/imports_dag.py -it airflow:custom airflow webserver
docker exec -it airflow airflow scheduler
```

**Important** : la création du user admin ne semble pas fonctionner depuis le Dockerfile, il faut exécuter en depuis le container

```
airflow users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.org
```





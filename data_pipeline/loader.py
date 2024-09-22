import pandas as pd
from clients.dspace_client_wrapper import DSpaceClientWrapper

from utils import manage_logger

logger = manage_logger("./logs/loader.log")

class Loader:
    def __init__(self, df_metadata, df_authors):
        self.df_metadata = df_metadata
        self.df_authors = df_authors
        self.dspace_wrapper = DSpaceClientWrapper()
        
    def _patch_units(self, workspace_id, units):
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
        return self.dspace_wrapper.update_workspace(workspace_id, patch_operations)
    
    def _add_upw(self, workspace_id):
        return self.dspace_wrapper.import_unpaywall_fulltext(workspace_id)   
        
    def create_complete_publication(self):
        for index, row in self.df_metadata.iterrows():
            workspace_id = self.dspace_wrapper.push_publication(row['source'], row['internal_id'], row['ifs3_collection_id'])
            if workspace_id:
                logger.info(f"Successfully pushed publication with ID: {workspace_id}")

                # Retrieve corresponding epfl_api_mainunit_name from df_authors
                matching_authors = self.df_authors[self.df_authors['row_id'] == row['id']]
                units = [{"acro": author['epfl_api_mainunit_name']} for _, author in matching_authors.iterrows()]
                
                if units:
                    update_response = self._patch_units(workspace_id, units)
                    if update_response:
                        upw_response = self._add_upw(workspace_id, units)
                        if upw_response:
                            logger.info("Import Unpaywall successful.")
                        else:
                            logger.error("Failed to import Unpaywall.")
                        return self.dspace_wrapper.create_workflowitem(workspace_id)
                    else:
                        logger.error(f"Failed to patch units for workspace ID: {workspace_id}.")

                       
                else:
                    logger.warning(f"No matching authors found for row ID: {row['id']}.")

            else:
                logger.error(f"Failed to push publication with source: {row['source']}, internal_id: {row['internal_id']}, and collection_id: {row['collection_id']}")
    
    ##### Do not use, only a unfinished test #########      
    def manage_person(self):
        for index, row in self.df.iterrows():
            if pd.notna(row['sciper_id']):
                if 'Dspace' in row['dspace_uuid']:
                    data = {
                        'epfl_api_mainunit_name': row['epfl_api_mainunit_name']
                    }
                    self.dspace_wrapper._update_object(row['dspace_uuid'], data)
                    logger.info(f"Updated person with DSpace UUID: {row['dspace_uuid']}")
                else:
                    data = {
                        'sciper_id': row['sciper_id'],
                        'epfl_api_mainunit_name': row['epfl_api_mainunit_name']
                    }
                    self.dspace_wrapper._create_object(data)
                    logger.info(f"Created person with SCIPER ID: {row['sciper_id']}")
            else:
                logger.warning(f"SCIPER ID is missing or NA for row: {index}")
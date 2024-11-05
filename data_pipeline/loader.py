import pandas as pd
import os
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import licenses_mapping, versions_mapping

from utils import manage_logger
from config import logs_dir

log_file_path = os.path.join(logs_dir, "loader.log")
logger = manage_logger(log_file_path)

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
                    "confidence": 500,
                    "place": 0,
                }
            )

        patch_operations = [
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

    def _patch_file_metadata(self, workspace_id, upw_license, upw_version):
        # On vérifie si le mapping pour la license et la version existe
        license_metadata = licenses_mapping.get(upw_license)
        version_metadata = versions_mapping.get(upw_version)

        if not license_metadata:
            logger.error(f"Le mapping pour la license '{upw_license}' n'existe pas.")

        if not version_metadata:
            logger.error(f"Le mapping pour la version '{upw_version}' n'existe pas.")

        patch_operations = [
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/metadata/dc.type",
                "value": [
                    {
                        "value": "main document",
                        "language": None,
                        "authority": None,
                        "display": "Main document",
                        "securityLevel": None,
                        "confidence": -1,
                        "place": 0,
                        "otherInformation": None,
                    }
                ],
            },
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/metadata/oaire.version",
                "value": [
                    {
                        "value": version_metadata["value"],
                        "language": None,
                        "authority": None,
                        "display": version_metadata["display"],
                        "securityLevel": None,
                        "confidence": -1,
                        "place": 0,
                        "otherInformation": None,
                    }
                ],
            },
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/metadata/oaire.licenseCondition",
                "value": [
                    {
                        "value": license_metadata["value"],
                        "language": None,
                        "authority": None,
                        "display": license_metadata["display"],
                        "securityLevel": None,
                        "confidence": -1,
                        "place": 0,
                        "otherInformation": None,
                    }
                ],
            },
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/accessConditions",
                "value": [{"name": "openaccess"}],
            },
        ]
        return self.dspace_wrapper.update_workspace(workspace_id, patch_operations)

    def _add_file(self, workspace_id, file_path):
        return self.dspace_wrapper.upload_file_to_workspace(workspace_id, file_path)

    def _filter_publications_by_valid_affilliations(self):
        # Find the 'row_id' of authors with a non-null 'epfl_api_mainunit_name'
        valid_author_ids = self.df_authors[
            self.df_authors["epfl_api_mainunit_name"].notnull()
        ]["row_id"].unique()

        if len(valid_author_ids) > 0:
            # Filter the publications using valid 'row_id's
            filtered_publications = self.df_metadata[
                self.df_metadata["row_id"].isin(valid_author_ids)
            ]
            logger.info(f"Filtered publications count: {len(filtered_publications)}")
            return filtered_publications
        else:
            logger.warning("No valid authors found with 'epfl_api_mainunit_name'.")
            return pd.DataFrame()  # Return an empty DataFrame if no valid authors found


    def create_complete_publication(self):
        # Filter publications by valid authors first
        df_items_to_import = self._filter_publications_by_valid_affilliations()
        df_items_imported = df_items_to_import.copy()

        # Proceed only if we have valid publications
        if df_items_to_import.empty:
            logger.error("No valid publications to process.")
            return df_items_imported  # Return an empty or unchanged DataFrame

        for index, row in df_items_to_import.iterrows():
            workspace_id = self.dspace_wrapper.push_publication(
                row.get("source", ""),
                row.get("internal_id", ""),
                row.get("ifs3_collection_id", ""),
            )
            valid_pdf = row.get("upw_valid_pdf", "")

            if pd.notna(valid_pdf) and valid_pdf != "":
                file_path = f"./pdfs/{valid_pdf}"
                logger.debug(f"file_path: {file_path}")
            else:
                file_path = None

            if workspace_id:
                logger.info(f"Successfully pushed publication with ID: {workspace_id}")
                # Store workspace_id in df_item_imported
                df_items_imported.at[index, "workspace_id"] = workspace_id

                # Retrieve corresponding epfl_api_mainunit_name from df_authors
                matching_authors = self.df_authors[
                    self.df_authors["row_id"] == row["row_id"]
                ]
                units = [
                    {"acro": author["epfl_api_mainunit_name"]}
                    for _, author in matching_authors.iterrows()
                    if pd.notna(author["epfl_api_mainunit_name"])
                    and author["epfl_api_mainunit_name"] != ""
                ]

                unique_units = {unit["acro"]: unit for unit in units}.values()
                logger.debug(f"retrieved units: {unique_units}")
                if unique_units:
                    self._patch_units(workspace_id, unique_units)
                    if file_path and os.path.exists(file_path):
                        file_response = self._add_file(workspace_id, file_path)
                        if file_response.status_code in [200, 201]:
                            logger.info(
                                f"File added successfully to workspace item {workspace_id}"
                            )
                            self._patch_file_metadata(
                                workspace_id,
                                row.get("upw_license"),
                                row.get("upw_version"),
                            )
                        else:
                            logger.warning(
                                f"Failed to add file to workspace item {workspace_id}. Status: {file_response.status_code}. Response: {file_response.text}"
                            )
                    else:
                        logger.warning(
                            f"File {file_path} does not exist. Skipping file upload."
                        )

                    # Create workflowitem and retrieve the workflow_id
                    workflow_response = self.dspace_wrapper.create_workflowitem(
                        workspace_id
                    )
                    if workflow_response and "id" in workflow_response:
                        workflow_id = workflow_response["id"]
                        logger.info(
                            f"Successfully created workflow item with ID: {workflow_id}"
                        )
                        # Store workflow_id in df_item_imported
                        df_items_imported.at[index, "workflow_id"] = workflow_id
                else:
                    logger.warning(
                        f"No matching units found for row ID: {row['row_id']}."
                    )
            else:
                logger.error(
                    f"Failed to push publication with source: {row['source']}, internal_id: {row['internal_id']}, and collection_id: {row['collection_id']}"
                )

        return df_items_imported

    ##### Do not use, only a unfinished test #########
    def manage_person(self):
        for index, row in self.df.iterrows():
            if pd.notna(row["sciper_id"]):
                if "Dspace" in row["dspace_uuid"]:
                    data = {"epfl_api_mainunit_name": row["epfl_api_mainunit_name"]}
                    self.dspace_wrapper._update_object(row["dspace_uuid"], data)
                    logger.info(
                        f"Updated person with DSpace UUID: {row['dspace_uuid']}"
                    )
                else:
                    data = {
                        "sciper_id": row["sciper_id"],
                        "epfl_api_mainunit_name": row["epfl_api_mainunit_name"],
                    }
                    self.dspace_wrapper._create_object(data)
                    logger.info(f"Created person with SCIPER ID: {row['sciper_id']}")
            else:
                logger.warning(f"SCIPER ID is missing or NA for row: {index}")

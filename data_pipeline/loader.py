import pandas as pd
import os
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import licenses_mapping, versions_mapping, collections_mapping

from utils import manage_logger
from config import logs_dir

log_file_path = os.path.join(logs_dir, "loader.log")
logger = manage_logger(log_file_path)

class Loader:
    def __init__(self, df_metadata, df_authors):
        self.df_metadata = df_metadata
        self.df_authors = df_authors
        self.dspace_wrapper = DSpaceClientWrapper()

    def _get_form_section(self, ifs3_collection_id):
        """
        Récupère la section associée à un ifs3_collection_id depuis le fichier de mapping.
        """
        for collection_name, attributes in collections_mapping.items():
            if attributes["id"] == ifs3_collection_id:
                return attributes["section"]

        logger.error(f"No section found for collection ID: {ifs3_collection_id}")
        return None

    def _patch_additional_metadata(
        self, workspace_id, row, units, ifs3_collection_id, workspace_response
    ):
        """
        Met à jour uniquement les champs nécessaires (selon les erreurs retournées dans workspace_response).
        """
        form_section = self._get_form_section(ifs3_collection_id)
        logger.info(
            f"Collection ID: '{ifs3_collection_id}' et section name: '{form_section}'."
        )
        if not form_section:
            logger.error(
                f"Invalid collection ID: {ifs3_collection_id}. Unable to determine form section."
            )
            return

        try:
            required_paths = []
            if "errors" in workspace_response:
                for error in workspace_response["errors"]:
                    if error.get("message") == "error.validation.required":
                        required_paths.extend(error.get("paths", []))
                    elif error.get("message") == "error.validation.license.required":
                        required_paths.extend(error.get("paths", []))

            logger.debug(f"Required paths to update: {required_paths}")

            patch_operations = self._construct_patch_operations(
                row, units, form_section, required_paths
            )
            logger.debug(f"Patch operations: {patch_operations}")

            response = self.dspace_wrapper._update_workspace(workspace_id, patch_operations)

            if "errors" in response:
                errors = response["errors"]
                for error in errors:
                    error_message = error.get("message", "No message provided")
                    error_paths = ", ".join(error.get("paths", [])) or "No paths provided"
                    logger.error(f"Error message: {error_message}")
                    logger.error(f"Paths concerned: {error_paths}")
                return

            logger.info(f"Metadata patched successfully for workspace {workspace_id}.")
        except Exception as e:
            logger.error(f"An error occurred while patching additional metadata: {e}")

    def _construct_patch_operations(self, row, units, form_section, required_paths):
        """Constructs PATCH operations for metadata updates with optimized error handling."""

        def build_value(value, authority=None, language="en", confidence=600, place=0):
            """Helper function to build a metadata value structure."""
            if not isinstance(value, str) or not value.strip():
                logger.warning(f"Invalid value provided: {value}")
                return None
            return {
                "value": value,
                "language": language,
                "authority": authority,
                "confidence": confidence,
                "place": place,
            }

        def create_metadata_operation(path, value, op="add"):
            """Helper to create a PATCH operation."""
            if value:  # Only include if value is not None or empty
                if isinstance(value, list) and all(v is None for v in value):
                    return None
                return {"op": op, "path": path, "value": value}
            return None

        def determine_section_path(base_path, form_section):
            """Determine correct path suffix based on form_section."""
            if form_section == "conference_":
                return f"{form_section}details"
            return f"{form_section}type"

        # Determine correct path for dc.type dynamically
        dc_type_path_suffix = determine_section_path("dc.type", form_section)

        def parse_conference_info(conference_info):
            """Parses the conference_info string into structured metadata."""
            operations = []
            if not conference_info:
                return operations

            conferences = conference_info.split("||")

            conference_types = []
            conference_names = []
            conference_places = []
            conference_dates = []

            for conf in conferences:
                parts = conf.split("::")
                name = parts[0].strip() if len(parts) > 0 and parts[0].strip() else None
                place = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
                start_date = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
                end_date = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None

                if name:
                    conference_names.append(
                        build_value(name, confidence=-1, language=None)
                    )
                if place:
                    conference_places.append(
                        build_value(place, confidence=-1, language=None)
                    )
                if start_date and end_date:
                    conference_dates.append(
                        build_value(f"{start_date} - {end_date}", confidence=-1, language=None)
                    )
                # Add a static "conference" type for each event
                conference_types.append(
                    build_value("conference", confidence=-1, language=None)
                )

            if conference_types:
                operations.append((
                    "/sections/conference_event/epfl.relation.conferenceType",
                    conference_types,
                    "add",
                ))
            if conference_names:
                operations.append((
                    "/sections/conference_event/dc.relation.conference",
                    conference_names,
                    "add",
                ))
            if conference_places:
                operations.append((
                    "/sections/conference_event/oaire.citation.conferencePlace",
                    conference_places,
                    "add",
                ))
            if conference_dates:
                operations.append((
                    "/sections/conference_event/oaire.citation.conferenceDate",
                    conference_dates,
                    "add",
                ))

            # Add a placeholder for acronym if needed
            operations.append((
                "/sections/conference_event/oairecerif.acronym",
                [build_value("#PLACEHOLDER_PARENT_METADATA_VALUE#", confidence=-1, language=None)],
                "add",
            ))

            return operations

        def parse_funding_info(funding_info):
            """Parses the funding_info string into structured metadata."""
            operations = []
            if not funding_info:
                return operations

            grants = funding_info.split("||")
            funders = []
            grant_nos = []

            for grant in grants:
                if "::" in grant:
                    funder, grantno = grant.split("::", 1)
                    if funder.strip():  # Add funder only if it is non-empty
                        funders.append(
                            build_value(funder.strip(), confidence=-1, language=None)
                        )
                    # Replace empty grantno with placeholder
                    grant_nos.append(
                        build_value(
                            grantno.strip() if grantno.strip() else "#PLACEHOLDER_PARENT_METADATA_VALUE#",
                            confidence=-1,
                            language=None,
                        )
                    )
                elif grant.strip():  # Handle cases where funder exists but no "::"
                    funders.append(
                        build_value(grant.strip(), confidence=-1, language=None)
                    )
                    # Add placeholder for missing grantno
                    grant_nos.append(
                        build_value("#PLACEHOLDER_PARENT_METADATA_VALUE#", confidence=-1, language=None)
                    )

            # Add operations only if there are valid values
            if funders:
                operations.append((
                    "/sections/grants/oairecerif.funder",
                    funders,
                    "add",
                ))
            if grant_nos:
                operations.append((
                    "/sections/grants/dc.relation.grantno",
                    grant_nos,
                    "add",
                ))
            return operations

        # Define metadata to be patched
        metadata_definitions = [
            (
                f"/sections/{form_section}details/dc.description.sponsorship",
                [
                    build_value(
                        unit.get("acro"),
                        f"will be referenced::ACRONYM::{unit.get('acro')}",
                    )
                    for unit in units
                    if unit.get("acro")
                ],
                "add",
            ),
            (
                f"/sections/{form_section}details/epfl.peerreviewed",
                [build_value("REVIEWED")],
                "add",
            ),
            (
                f"/sections/ctb-bitstream-metadata/ctb.oaireXXlicenseCondition",
                [build_value(row.get("license"))],
                "add",
            ),
            (
                f"/sections/{form_section}details/epfl.writtenAt",
                [build_value("EPFL")],
                "add",
            ),
            (
                "/sections/license/granted",
                "true",
                "add",
            ),
            (
                f"/sections/{dc_type_path_suffix}/dc.type/0",
                build_value(
                    row.get("dc.type"),
                    authority=row.get("dc.type_authority"),
                    language=None,
                ),
                "replace",
            ),
            (
                f"/sections/{form_section}details/dc.description.abstract",
                [build_value(row.get("abstract"), language="en", confidence=-1)],
                "add",
            ),
        ]

        # Add funding_info operations dynamically
        metadata_definitions.extend(parse_funding_info(row.get("fundings_info")))
        metadata_definitions.extend(parse_conference_info(row.get("conference_info")))

        # Generate patch operations
        filtered_operations = []
        for path, value, op in metadata_definitions:
            operation = create_metadata_operation(path, value, op)
            if operation:
                filtered_operations.append(operation)

        if not filtered_operations:
            logger.warning("No operations constructed; required paths might be missing.")

        return filtered_operations

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
        return self.dspace_wrapper._update_workspace(workspace_id, patch_operations)

    def _add_file(self, workspace_id, file_path):
        return self.dspace_wrapper._upload_file_to_workspace(workspace_id, file_path)

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
            # Extract source and other identifiers from the row
            source = row.get("source", "")
            source_id = row.get("internal_id", "")
            collection_id = row.get("ifs3_collection_id", "")

            # Adjust the internal_id and source depending on the source
            if source == "openalex" or source == "zenodo":
                source_id = row.get("doi", source_id)
            if source == "openalex":
                source = "crossref"
            elif source == "zenodo":
                source = "datacite"

            # Call the method to push the publication to the workspace
            workspace_response = self.dspace_wrapper.push_publication(
                source,
                source_id,
                collection_id,
            )

            valid_pdf = row.get("upw_valid_pdf", "")

            if pd.notna(valid_pdf) and valid_pdf != "":
                file_path = f"./pdfs/{valid_pdf}"
                logger.debug(f"file_path: {file_path}")
            else:
                file_path = None

            if workspace_response and "id" in workspace_response:
                logger.debug(
                    f"Workspace creation response: {workspace_response}"
                )
                workspace_id = workspace_response["id"]
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
                    self._patch_additional_metadata(
                        workspace_id,
                        row,
                        unique_units,
                        collection_id,
                        workspace_response,
                    )
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
                    workflow_response = self.dspace_wrapper._create_workflowitem(
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
                logger.error(row.keys())
                logger.error(
                    f"Failed to push publication with source: {row['source']}, internal_id: {row['internal_id']}, and collection_id: {row['ifs3_collection_id']}"
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

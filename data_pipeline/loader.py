import pandas as pd
import os
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import licenses_mapping, versions_mapping, collections_mapping
from utils import manage_logger
from config import logs_dir
import re

log_file_path = os.path.join(logs_dir, "loader.log")
logger = manage_logger(log_file_path)


class Loader:
    def __init__(self, df_metadata, df_authors):
        self.df_metadata = df_metadata
        self.df_authors = df_authors
        self.dspace_wrapper = DSpaceClientWrapper()

    def _is_valid_uuid(self, value):
        """Check if the value is a valid UUID using regex."""
        uuid_regex = re.compile(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        )
        return bool(uuid_regex.match(value))

    def _get_form_section(self, ifs3_collection_id):
        """Retrieve the section name for a given collection ID."""
        for collection_name, attributes in collections_mapping.items():
            if attributes["id"] == ifs3_collection_id:
                return attributes["section"]

        logger.error(f"No section found for collection ID: {ifs3_collection_id}")
        return None

    def _clean_guessing_authors(self, workspace_response, form_section):
        """Process the `dc.contributor.author` field to remove `authority` and `confidence`."""
        if (
            "sections" in workspace_response
            and f"{form_section}details" in workspace_response["sections"]
        ):
            authors = workspace_response["sections"][f"{form_section}details"].get(
                "dc.contributor.author", []
            )
            for author in authors:
                author.pop("authority", None)
                author.pop("confidence", None)
            logger.info(f"Processed authors: {authors}")
            return authors
        logger.warning("No authors found in workspace response.")
        return []

    def _process_authors(self, workspace_response, row_id, form_section):
        """Process the `dc.contributor.author` field to enrich authors with information from df_authors."""
        if (
            "sections" in workspace_response
            and f"{form_section}details" in workspace_response["sections"]
        ):
            authors = workspace_response["sections"][f"{form_section}details"].get(
                "dc.contributor.author", []
            )
            for author in authors:
                author_name = author.get("value")
                if not author_name:
                    continue

                # Find matching author in df_authors for the corresponding row_id
                matching_author = self.df_authors[
                    (self.df_authors["row_id"] == row_id)
                    & (self.df_authors["author"].str.lower() == author_name.lower())
                ]

                if not matching_author.empty:
                    # Use the first match (if duplicates exist)
                    match = matching_author.iloc[0]
                    if pd.notna(match["dspace_uuid"]) and self._is_valid_uuid(
                        match["dspace_uuid"]
                    ):
                        authority_prefix = "will be referenced::SCIPER-ID::"
                    else:
                        authority_prefix = "will be generated::SCIPER-ID::"

                    # Enrich author data
                    author["authority"] = (
                        f"{authority_prefix}{str(match['sciper_id'])}"
                        if pd.notna(match["sciper_id"])
                        else None
                    )
                    author["confidence"] = 600 if pd.notna(match["sciper_id"]) else -1

            logger.info(f"Enriched authors: {authors}")
            return authors
        logger.warning("No authors found in workspace response.")
        return []

    def _patch_additional_metadata(
        self, workspace_id, row, units, ifs3_collection_id, workspace_response
    ):
        """Update only necessary fields based on errors returned in workspace_response."""
        form_section = self._get_form_section(ifs3_collection_id)
        logger.info(
            f"Collection ID: '{ifs3_collection_id}' and section name: '{form_section}'."
        )
        if not form_section:
            logger.error(
                f"Invalid collection ID: {ifs3_collection_id}. Unable to determine form section."
            )
            return

        try:
            # Process authors in the workspace response
            cleaned_authors = self._clean_guessing_authors(
                workspace_response, form_section
            )
            updated_authors = self._process_authors(
                workspace_response, row["row_id"], form_section
            )

            required_paths = []
            if "errors" in workspace_response:
                for error in workspace_response["errors"]:
                    if error.get("message") == "error.validation.required":
                        required_paths.extend(error.get("paths", []))
                    elif error.get("message") == "error.validation.license.required":
                        required_paths.extend(error.get("paths", []))

            logger.debug(f"Required paths to update: {required_paths}")

            patch_operations = self._construct_patch_operations(
                row, units, form_section, workspace_response
            )

            # Add operation to update authors
            if cleaned_authors:
                patch_operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{form_section}details/dc.contributor.author",
                        "value": cleaned_authors,
                    }
                )
            if updated_authors:
                patch_operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{form_section}details/dc.contributor.author",
                        "value": updated_authors,
                    }
                )

            logger.info(f"Patch operations: {patch_operations}")

            response = self.dspace_wrapper._update_workspace(
                workspace_id, patch_operations
            )

            if "errors" in response:
                errors = response["errors"]
                for error in errors:
                    error_message = error.get("message", "No message provided")
                    error_paths = (
                        ", ".join(error.get("paths", [])) or "No paths provided"
                    )
                    logger.error(f"Error message: {error_message}")
                    logger.error(f"Paths concerned: {error_paths}")
                return

            logger.info(f"Metadata patched successfully for workspace {workspace_id}.")
        except Exception as e:
            logger.error(f"An error occurred while patching additional metadata: {e}")

    def _construct_patch_operations(self, row, units, form_section, workspace_response):
        """Construct PATCH operations for metadata updates with optimized error handling."""

        def build_value(value, authority=None, language="en", confidence=-1, place=0):
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

        def metadata_exists(path, workspace_response):
            """Check if the metadata exists in the workspace response."""
            sections = workspace_response.get("sections", {})
            keys = path.split("/")[2:]  # Remove "/sections/" from path
            current = sections

            for key in keys:
                if isinstance(current, list):
                    return len(current) > 0
                elif key in current:
                    current = current[key]
                else:
                    return False
            return bool(current)

        def determine_operation(path, is_repeatable):
            """Determine if the operation should be replace or add."""
            if not is_repeatable and metadata_exists(path, workspace_response):
                return "replace"
            return "add"

        def determine_section_path(base_path, form_section):
            """Determine correct path suffix based on form_section."""
            if form_section in ["conference_", "book_"]:
                return f"{form_section}details"
            return f"{form_section}type"

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
                place = (
                    parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
                )
                start_date = (
                    parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
                )
                end_date = (
                    parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                )

                if name:
                    conference_names.append(
                        build_value(name)
                    )
                if place:
                    conference_places.append(
                        build_value(place)
                    )
                if start_date and end_date:
                    conference_dates.append(
                        build_value(
                            f"{start_date} - {end_date}"
                        )
                    )
                conference_types.append(
                    build_value("conference")
                )

            if conference_types:
                operations.append(
                    {"op": "add", "path": "/sections/conference_event/epfl.relation.conferenceType", "value": conference_types}    
                )
            if conference_names:
                operations.append(
                    {"op": "add", "path": "/sections/conference_event/dc.relation.conference", "value": conference_names}    
                )
            if conference_places:
                operations.append(
                    {"op": "add", "path": "/sections/conference_event/oaire.citation.conferencePlace", "value": conference_places}    
                )
            if conference_dates:
                operations.append(
                    {"op": "add", "path": "/sections/conference_event/oaire.citation.conferenceDate", "value": conference_dates}    
                )

            operations.append(
                {
                    "op": "add",
                    "path": "/sections/conference_event/oairecerif.acronym",
                    "value": [
                        build_value(
                            "#PLACEHOLDER_PARENT_METADATA_VALUE#",
                            confidence=-1,
                            language=None,
                        )
                    ],
                }
            )

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
                    if funder.strip():
                        funders.append(
                            build_value(funder.strip())
                        )
                    grant_nos.append(
                        build_value(
                            (
                                grantno.strip()
                                if grantno.strip()
                                else "#PLACEHOLDER_PARENT_METADATA_VALUE#"
                            ),
                        )
                    )
                elif grant.strip():
                    funders.append(
                        build_value(grant.strip())
                    )
                    grant_nos.append(
                        build_value(
                            "#PLACEHOLDER_PARENT_METADATA_VALUE#",
                        )
                    )

            if funders:
                operations.append(
                    {"op": "add", "path": "/sections/grants/oairecerif.funder", "value": funders}
                )
            if grant_nos:
                operations.append(
                    {
                        "op": "add",
                        "path": "/sections/grants/dc.relation.grantno",
                        "value": grant_nos,
                    }
                )
            return operations
        
        def parse_editors(editors):
                """Parses the editors field into structured metadata."""
                if not editors:
                    return []

                editor_list = [
                    build_value(editor.strip())
                    for editor in editors.split("||")
                    if editor.strip()
                ]

                affiliation_placeholder = [
                    build_value("#PLACEHOLDER_PARENT_METADATA_VALUE#") for _ in editor_list
                ]
                orcid_placeholder = [
                    build_value("#PLACEHOLDER_PARENT_METADATA_VALUE#") for _ in editor_list
                ]

                operations = []
                if editor_list:
                    operations.append(
                        {
                            "op": "add",
                            "path": "/sections/bookcontainer_details/dc.contributor.scientificeditor",
                            "value": editor_list,
                        }
                    )
                    operations.append(
                        {
                            "op": "add",
                            "path": "/sections/bookcontainer_details/oairecerif.scientificeditor.affiliation",
                            "value": affiliation_placeholder,
                        }
                    )
                    operations.append(
                        {
                            "op": "add",
                            "path": "/sections/bookcontainer_details/oairecerif.affiliation.orgunit",
                            "value": affiliation_placeholder,
                        }
                    )
                    operations.append(
                        {
                            "op": "add",
                            "path": "/sections/bookcontainer_details/person.identifier.orcid",
                            "value": orcid_placeholder,
                        }
                    )

                return operations        

        metadata_definitions = []

        fields = [
            (
                f"/sections/{dc_type_path_suffix}/dc.type",
                [
                    build_value(
                        row.get("dc.type"),
                        authority=row.get("dc.type_authority"),
                        language="en",
                        confidence=600,
                    )
                ],
                False,
            ),
            (
                "/sections/journalcontainer_details/dc.relation.journal",
                [build_value(row.get("journalTitle"))],
                False,
            ),
            (
                "/sections/journalcontainer_details/dc.relation.issn",
                [
                    build_value(issn)
                    for issn in str(row.get("journalISSN", "")).split("||")
                    if issn.strip()
                ],
                True,
            ),
            (
                "/sections/journalcontainer_details/oaire.citation.volume",
                [build_value(row.get("journalVolume"))],
                False,
            ),
            (
                "/sections/journalcontainer_details/oaire.citation.issue",
                [build_value(row.get("issue"))],
                False,
            ),
            (
                "/sections/journalcontainer_details/oaire.citation.articlenumber",
                [build_value(row.get("artno"))],
                False,
            ),
            (
                "/sections/journalcontainer_details/oaire.citation.startPage",
                [build_value(row.get("startingPage"))],
                False,
            ),
            (
                "/sections/journalcontainer_details/oaire.citation.endPage",
                [build_value(row.get("endingPage"))],
                False,
            ),
            (
                f"/sections/bookcontainer_details/dc.publisher",
                [build_value(row.get("publisher"))],
                False,
            ),
            (
                f"/sections/bookcontainer_details/dc.relation.ispartofseries",
                [
                    build_value(
                        f"{row.get('seriesTitle', '')}; {row.get('seriesVolume', '')}".strip(
                            "; "
                        )
                    )
                ],
                True,
            ),
            (
                f"/sections/bookcontainer_details/dc.relation.serieissn",
                [
                    build_value(issn)
                    for issn in str(row.get("seriesISSN", "")).split("||")
                    if issn.strip()
                ],
                True,
            ),
            (
                f"/sections/bookcontainer_details/dc.relation.ispartof",
                [build_value(row.get("bookTitle"))],
                False,
            ),
            (
                f"/sections/bookcontainer_details/epfl.part.number",
                [build_value(row.get("bookPart"))],
                False,
            ),
            (
                f"/sections/bookcontainer_details/dc.relation.ispartof",
                [build_value(row.get("bookPart"))],
                False,
            ),
            (
                f"/sections/{form_section}details/epfl.writtenAt",
                [build_value("EPFL")],
                False,
            ),
            (
                f"/sections/{form_section}details/dc.description.abstract",
                [build_value(row.get("abstract"), language="en")],
                False,
            ),
            (
                f"/sections/{form_section}details/dc.description.sponsorship",
                [
                    build_value(
                        unit.get("acro"),
                        f"will be referenced::ACRONYM::{unit.get('acro')}",
                        confidence=600,
                    )
                    for unit in units
                    if unit.get("acro")
                ],
                True,
            ),
            (
                f"/sections/{form_section}details/epfl.peerreviewed",
                [build_value("REVIEWED")],
                False,
            ),
            (
                f"/sections/ctb-bitstream-metadata/ctb.oaireXXlicenseCondition",
                [build_value(row.get("license"))],
                False,
            ),
        ]

        for path, value, is_repeatable in fields:
            if value and not all(v is None for v in value if isinstance(value, list)):  # Skip invalid or empty values
                op = determine_operation(path, is_repeatable)
                if op == "replace" and not is_repeatable:
                    if isinstance(value, list) and len(value) == 1:
                        value = value[0]  # Unwrap value if replace and only one element
                    path = f"{path}/0"
                metadata_definitions.append({"op": op, "path": path, "value": value})

        metadata_definitions.extend(parse_funding_info(row.get("fundings_info")))
        metadata_definitions.extend(parse_conference_info(row.get("conference_info")))
        # Process editors
        metadata_definitions.extend(parse_editors(row.get("editors")))
        # Add specific patch for license/granted
        metadata_definitions.append({
            "op": "add",
            "path": "/sections/license/granted",
            "value": "true"
        })

        logger.info(f"metadata_definitions : {metadata_definitions}")

        if not metadata_definitions:
            logger.warning("No operations constructed; required paths might be missing.")

        return metadata_definitions

    def _patch_file_metadata(self, workspace_id, upw_license, upw_version):
        """Patch metadata for file."""
        license_metadata = licenses_mapping.get(upw_license)
        version_metadata = versions_mapping.get(upw_version)

        if not license_metadata:
            logger.error(f"License mapping for '{upw_license}' does not exist.")

        if not version_metadata:
            logger.error(f"Version mapping for '{upw_version}' does not exist.")

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

    def _filter_publications_by_valid_affiliations(self):
        """Filter publications with valid author affiliations."""
        valid_author_ids = self.df_authors[
            self.df_authors["epfl_api_mainunit_name"].notnull()
        ]["row_id"].unique()

        if len(valid_author_ids) > 0:
            filtered_publications = self.df_metadata[
                self.df_metadata["row_id"].isin(valid_author_ids)
            ]
            logger.info(f"Filtered publications count: {len(filtered_publications)}")
            return filtered_publications
        else:
            logger.warning("No valid authors found with 'epfl_api_mainunit_name'.")
            return pd.DataFrame()  # Return an empty DataFrame if no valid authors found

    def create_complete_publication(self):
        """Create complete publications including metadata and file uploads."""
        df_items_to_import = self._filter_publications_by_valid_affiliations()
        df_items_imported = df_items_to_import.copy()

        if df_items_to_import.empty:
            logger.error("No valid publications to process.")
            return df_items_imported

        for index, row in df_items_to_import.iterrows():
            source = row.get("source", "")
            source_id = row.get("internal_id", "")
            collection_id = row.get("ifs3_collection_id", "")

            if source == "openalex" or source == "zenodo":
                source_id = row.get("doi", source_id)
            if source == "openalex":
                source = "crossref"
            elif source == "zenodo":
                source = "datacite"

            workspace_response = self.dspace_wrapper.push_publication(
                source, source_id, collection_id
            )

            valid_pdf = row.get("upw_valid_pdf", "")
            file_path = (
                f"./pdfs/{valid_pdf}"
                if pd.notna(valid_pdf) and valid_pdf != ""
                else None
            )

            if workspace_response and "id" in workspace_response:
                workspace_id = workspace_response["id"]
                logger.info(f"Successfully pushed publication with ID: {workspace_id}")
                df_items_imported.at[index, "workspace_id"] = workspace_id

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
                                f"Failed to add file to workspace item {workspace_id}. Status: {file_response.status_code}."
                            )
                    else:
                        logger.warning(
                            f"File {file_path} does not exist. Skipping file upload."
                        )

                    # workflow_response = self.dspace_wrapper._create_workflowitem(
                    #     workspace_id
                    # )
                    # if workflow_response and "id" in workflow_response:
                    #     workflow_id = workflow_response["id"]
                    #     logger.info(
                    #         f"Successfully created workflow item with ID: {workflow_id}"
                    #     )
                    #     df_items_imported.at[index, "workflow_id"] = workflow_id
                else:
                    logger.warning(
                        f"No matching units found for row ID: {row['row_id']}."
                    )
            else:
                logger.error(
                    f"Failed to push publication with source: {row['source']}, internal_id: {row['internal_id']}, and collection_id: {row['ifs3_collection_id']}"
                )

        return df_items_imported

    def manage_person(self):
        """Update or create person records in DSpace."""
        for index, row in self.df_metadata.iterrows():
            if pd.notna(row.get("sciper_id")):
                if "Dspace" in row.get("dspace_uuid", ""):
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

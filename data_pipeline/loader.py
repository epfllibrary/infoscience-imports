"""Loader processor for ingesting items into DSpace."""

import os
import re
import pandas as pd
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import licenses_mapping, versions_mapping, collections_mapping
from utils import manage_logger
from config import logs_dir

log_file_path = os.path.join(logs_dir, "loader.log")
logger = manage_logger(log_file_path)

dspace_wrapper = DSpaceClientWrapper()
class Loader:
    """Load items into DSpace using workflow."""

    def __init__(self, df_metadata, df_epfl_authors, df_authors):
        self.df_metadata = df_metadata
        self.df_epfl_authors = df_epfl_authors
        self.df_authors = df_authors

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

    def _process_and_replace_authors(self, workspace_response, row_id, form_section):
        """Replace and enrich dc.contributor.author."""
        if (
            "sections" not in workspace_response
            or f"{form_section}details" not in workspace_response["sections"]
        ):
            logger.warning("No authors section found in workspace response.")
            return []

        matching_authors = self.df_authors[self.df_authors["row_id"] == row_id]

        if matching_authors.empty:
            logger.warning(f"No matching authors found in df_authors for row_id: {row_id}.")
            return []

        def create_metadata(value, display=None, confidence=-1, authority=None):
            return {
                "value": value,
                "authority": authority,
                "display": display or value,
                "confidence": confidence,
            }

        def get_first_split(
            value, delimiter="|", default="#PLACEHOLDER_PARENT_METADATA_VALUE#"
        ):
            """
            Extracts the first part after the delimiter in the given string.
            If a prefix consisting of digits followed by ':' is present, returns the part after ':'.
            If no valid prefix is present, returns the first part as is.
            Returns the default value if the string is NaN or empty.
            """
            if pd.notna(value) and value.strip():
                first_part = value.split(delimiter, 1)[0].strip()
                # Check if the prefix consists of digits followed by ':'
                if re.match(r"^\d+:", first_part):
                    return first_part.split(":", 1)[-1].strip()
                return first_part
            else:
                return default

        authors_metadata = []
        affiliations_metadata = []
        orgunit_metadata = []
        orcid_metadata = []
        roles_metadata = []

        for _, author_row in matching_authors.iterrows():
            authors_metadata.append(create_metadata(author_row["author"]))

            orcid_metadata.append(create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#"))

            affiliation_name = get_first_split(author_row.get("organizations", ""))
            affiliations_metadata.append(create_metadata(affiliation_name))

            # orgunit_name = get_first_split(author_row.get("suborganization", ""))
            orgunit_metadata.append(
                create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#")
            )

            roles_metadata.append(create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#"))

        for i, author in enumerate(authors_metadata):
            author_name = author["value"]
            matching_epfl_author = self.df_epfl_authors[
                (self.df_epfl_authors["row_id"] == row_id)
                & (self.df_epfl_authors["author"] == author_name)
            ]

            if matching_epfl_author.empty:
                continue

            for _, match in matching_epfl_author.iterrows():
                if pd.notna(match.get("sciper_id")):
                    prefix = (
                        "will be referenced::"
                        if pd.notna(match.get("dspace_uuid"))
                        else "will be generated::"
                    )
                    author.update(
                        authority=f"{prefix}SCIPER-ID::{match['sciper_id']}",
                        confidence=600,
                    )

                if pd.notna(match.get("organizations")):
                    affiliations_metadata[i] = create_metadata(
                        "\u00c9cole Polytechnique F\u00e9d\u00e9rale de Lausanne",
                        authority="will be referenced::ROR-ID::https://ror.org/02s376052",
                        confidence=600,
                    )

        patch_operations = [
            {
                "op": "add",
                "path": f"/sections/{form_section}details/dc.contributor.author",
                "value": authors_metadata,
            },
            {
                "op": "add",
                "path": f"/sections/{form_section}details/oairecerif.author.affiliation",
                "value": affiliations_metadata,
            },
            {
                "op": "add",
                "path": f"/sections/{form_section}details/oairecerif.affiliation.orgunit",
                "value": orgunit_metadata,
            },
            {
                "op": "add",
                "path": f"/sections/{form_section}details/person.identifier.orcid",
                "value": orcid_metadata,
            },
            {
                "op": "add",
                "path": f"/sections/{form_section}details/epfl.contributor.role",
                "value": roles_metadata,
            },
        ]

        return patch_operations

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
            # Construct remove operations
            remove_operations = self._construct_remove_operations(
                workspace_response, form_section
            )
            # logger.debug(f"remove_operations: {remove_operations}")

            if remove_operations:
                try:
                    updated_workspace = dspace_wrapper.update_workspace(
                        workspace_id, remove_operations
                    )
                except Exception as e:
                    logger.error(f"Failed to execute remove operations: {e}")
            else:
                # If remove_operations is empty, set updated_workspace to workspace_response
                updated_workspace = workspace_response

            required_paths = []
            if "errors" in updated_workspace:
                for error in updated_workspace["errors"]:
                    if error.get("message") == "error.validation.required":
                        required_paths.extend(error.get("paths", []))
                    elif error.get("message") == "error.validation.license.required":
                        required_paths.extend(error.get("paths", []))

            logger.debug(f"Required paths to update: {required_paths}")

            patch_operations = self._construct_patch_operations(
                row, units, form_section, updated_workspace
            )

            # Replace authors using the updated function
            author_patch = self._process_and_replace_authors(
                updated_workspace, row["row_id"], form_section
            )
            logger.debug("Patch author_patch: %s", author_patch)

            # Add operation to update authors
            if author_patch:
                patch_operations.extend(author_patch)

            logger.debug("Patch operations: %s", patch_operations)

            # Apply patch operations
            try:
                response = dspace_wrapper.update_workspace(
                    workspace_id, patch_operations
                )
            except Exception as e:
                logger.error("Failed to execute patch operations: %s", e)
                return

            # Handle errors in the response
            for error in response.get("errors", []):
                error_message = error.get("message", "No message provided")
                error_paths = ", ".join(error.get("paths", [])) or "No paths provided"
                logger.error(f"Error message: {error_message}")
                logger.error(f"Paths concerned: {error_paths}")

            logger.info(f"Metadata patched successfully for workspace {workspace_id}.")

        except Exception as e:
            logger.error(f"An error occurred while patching additional metadata: {e}")

    def _metadata_exists(self, path, workspace_response):
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

    def _create_op(self, path, values):
        return {
            "op": "add",
            "path": path,
            "value": [
                {
                    "value": value,
                    "language": None,
                    "authority": None,
                    "display": value,
                    "securityLevel": 0,
                    "confidence": -1,
                    "place": 0,
                    "source": None,
                    "otherInformation": None,
                }
                for value in values
            ],
        }

    def _construct_remove_operations(self, workspace_response, form_section):

        metadata_definitions = []

        # Check for existing metadata and add remove operations if needed
        removable_metadata_paths = [
            f"/sections/{form_section}details/dc.contributor.author",
            f"/sections/{form_section}details/oairecerif.author.affiliation",
            f"/sections/{form_section}details/oairecerif.affiliation.orgunit",
            f"/sections/{form_section}details/person.identifier.orcid",
            f"/sections/{form_section}details/epfl.contributor.role",
            "/sections/bookcontainer_details/dc.relation.ispartof",
            "/sections/journalcontainer_details/dc.relation.journal",
            "/sections/journalcontainer_details/dc.relation.issn",
            "/sections/journalcontainer_details/oaire.citation.volume",
        ]

        for path in removable_metadata_paths:
            if self._metadata_exists(path, workspace_response):
                metadata_definitions.append({"op": "remove", "path": path})

        return metadata_definitions

    def _construct_patch_operations(self, row, units, form_section, workspace_response):
        """Construct PATCH operations for metadata updates with optimized error handling."""

        def build_value(value, authority=None, language=None, confidence=-1, place=0):
            """Helper function to build a metadata value structure."""
            if value is None or (isinstance(value, str) and not value.strip()):
                logger.debug(f"Invalid value provided: {value}")
                return None

            return {
                "value": value,
                "language": language,
                "authority": authority,
                "confidence": confidence,
                "place": place,
            }

        def determine_operation(path, is_repeatable):
            """Determine if the operation should be replace or add."""
            if not is_repeatable and self._metadata_exists(path, workspace_response):
                return "replace"
            return "add"

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
                    conference_names.append(build_value(name))
                if place:
                    conference_places.append(build_value(place))
                if start_date and end_date:
                    conference_dates.append(build_value(f"{start_date} - {end_date}"))
                conference_types.append(build_value("conference"))

            if conference_types:
                operations.append(
                    {
                        "op": "add",
                        "path": "/sections/conference_event/epfl.relation.conferenceType",
                        "value": conference_types,
                    }
                )
            if conference_names:
                operations.append(
                    {
                        "op": "add",
                        "path": "/sections/conference_event/dc.relation.conference",
                        "value": conference_names,
                    }
                )
            if conference_places:
                operations.append(
                    {
                        "op": "add",
                        "path": "/sections/conference_event/oaire.citation.conferencePlace",
                        "value": conference_places,
                    }
                )
            if conference_dates:
                operations.append(
                    {
                        "op": "add",
                        "path": "/sections/conference_event/oaire.citation.conferenceDate",
                        "value": conference_dates,
                    }
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
            """Parses the funding_info string into structured metadata with additional fields, handling missing data properly.

            Args:
                funding_info (str): A string containing the funding information, separated by '||' for multiple entries.

            Returns:
                list: A list of operations to add the parsed funding information in a structured format, or None if no funder information is present.
            """

            # Initialize lists for funders, funding names, grant numbers, and award URIs
            funders, funding_names, grant_nos, award_uris = [], [], [], []

            # Split input string into individual grants
            grants = funding_info.split("||")

            for grant in grants:
                # Split the grant info by "::" into funder and grant number (if available)
                parts = grant.split("::", 1)
                funder = parts[0].strip() if parts[0].strip() else None
                grantno = (
                    parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
                )

                # Only process entries with a non-empty funder
                if funder:
                    funders.append(funder)
                    grant_nos.append(
                        grantno if grantno else "#PLACEHOLDER_PARENT_METADATA_VALUE#"
                    )
                    funding_names.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")
                    award_uris.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")

            # Create the operations for each section if funders list is not empty
            operations = []
            if funders:
                operations.append(
                    self._create_op("/sections/grants/oairecerif.funder", funders)
                )
                if funding_names:
                    operations.append(
                        self._create_op(
                            "/sections/grants/dc.relation.funding", funding_names
                        )
                    )
                if grant_nos:
                    operations.append(
                        self._create_op(
                            "/sections/grants/dc.relation.grantno", grant_nos
                        )
                    )
                if award_uris:
                    operations.append(
                        self._create_op(
                            "/sections/grants/crisfund.award.uri", award_uris
                        )
                    )

            return operations

        def parse_editors(editors):
            """Parses the editors field into structured metadata, handling missing data properly.

            Args:
                editors (str): A string containing the editor information, separated by '||' for multiple entries.

            Returns:
                list: A list of operations to add the parsed editor information in a structured format, or None if no valid editor names are present.
            """

            # Initialize lists for editors, affiliations, and ORCIDs
            editors_list, affiliations, orcids = [], [], []

            # Split input string into individual editors
            editors_split = editors.split("||")

            for editor in editors_split:
                # Process each editor (name)
                editor_name = editor.strip()

                # Only process entries with a non-empty editor_name
                if editor_name:
                    editors_list.append(editor_name)
                    # Add placeholders for affiliations and ORCIDs (same for all editors)
                    affiliations.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")
                    orcids.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")

            # Create the operations for each section (editors, affiliations, ORCIDs) if editors_list is not empty
            operations = []
            if editors_list:
                operations.append(
                    self._create_op(
                        "/sections/bookcontainer_details/dc.contributor.scientificeditor",
                        editors_list,
                    )
                )
                if affiliations:
                    operations.append(
                        self._create_op(
                            "/sections/bookcontainer_details/oairecerif.scientificeditor.affiliation",
                            affiliations,
                        )
                    )
                    operations.append(
                        self._create_op(
                            "/sections/bookcontainer_details/oairecerif.affiliation.orgunit",
                            affiliations,
                        )
                    )
                if orcids:
                    operations.append(
                        self._create_op(
                            "/sections/bookcontainer_details/person.identifier.orcid",
                            orcids,
                        )
                    )

            return operations

        # Determine correct form_section and related sections
        type_section = f"{form_section}{'details' if form_section in ['conference_', 'book_'] else 'type'}"

        dc_type = row.get("dc.type")
        if dc_type in [
            "text::book/monograph::book part or chapter",
            "text::book/monograph",
        ]:
            pagination_section = "book_details"
            isbn_section = (
                "book_details"
                if dc_type == "text::book/monograph"
                else "bookcontainer_details"
            )
            isbn_metadata = (
                "dc.identifier.isbn"
                if dc_type == "text::book/monograph"
                else "dc.relation.isbn"
            )
        else:
            pagination_section = "journalcontainer_details"
            isbn_section = "bookcontainer_details"
            isbn_metadata = "dc.relation.isbn"

        metadata_definitions = []

        journal_issn = str(row.get("journalISSN", ""))
        issn_list = [issn.strip() for issn in journal_issn.split("||") if issn.strip()]
        authority_journal = f"will be generated::ISSN::{issn_list[0]}" if issn_list else None

        fields = [
            (
                f"/sections/{type_section}/dc.type",
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
                f"/sections/{form_section}details/dc.date.issued",
                [build_value(row.get("issueDate"))],
                False,
            ),
            (
                "/sections/alternative_identifiers/dc.identifier.pmid",
                [build_value(row.get("pmid"))],
                False,
            ),
            (
                f"/sections/{form_section}details/dc.subject",
                [
                    build_value(keyword)
                    for keyword in str(row.get("keywords", "")).split("||")
                    if keyword.strip()
                ],
                True,
            ),
            (
                "/sections/journalcontainer_details/dc.relation.journal",
                [
                    build_value(
                        row.get("journalTitle"),
                        authority=authority_journal,
                        confidence=500,
                    )
                ],
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
                f"/sections/{pagination_section}/oaire.citation.startPage",
                [build_value(row.get("startingPage"))],
                False,
            ),
            (
                f"/sections/{pagination_section}/oaire.citation.endPage",
                [build_value(row.get("endingPage"))],
                False,
            ),
            (
                "/sections/bookcontainer_details/dc.publisher",
                [build_value(row.get("publisher"))],
                False,
            ),
            (
                "/sections/bookcontainer_details/dc.publisher.place",
                [build_value(row.get("publisherPlace"))],
                False,
            ),
            (
                "/sections/bookcontainer_details/dc.relation.ispartofseries",
                [
                    build_value(
                        f"{row.get('seriesTitle', '')}; {row.get('seriesVolume', '')}".strip(
                            "; "
                        ),
                    )
                ],
                True,
            ),
            (
                "/sections/bookcontainer_details/dc.relation.serieissn",
                [
                    build_value(issn)
                    for issn in str(row.get("seriesISSN", "")).split("||")
                    if issn.strip()
                ],
                True,
            ),
            (
                "/sections/bookcontainer_details/dc.relation.ispartof",
                [build_value(row.get("bookTitle"))],
                False,
            ),
            (
                "/sections/bookcontainer_details/epfl.part.number",
                [build_value(row.get("bookPart"))],
                False,
            ),
            (
                f"/sections/{isbn_section}/{isbn_metadata}",
                [
                    build_value(isbn)
                    for isbn in str(row.get("bookISBN", "")).split("||")
                    if isbn.strip()
                ],
                True,
            ),
            (
                f"/sections/{form_section}details/dc.contributor",
                [
                    build_value(corp)
                    for corp in str(row.get("corporateAuthor", "")).split("||")
                    if corp.strip()
                ],
                True,
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
                f"/sections/{form_section}details/epfl.writtenAt",
                [build_value("EPFL")],
                False,
            ),
            (
                f"/sections/{form_section}details/epfl.peerreviewed",
                [build_value("REVIEWED")],
                False,
            ),
            (
                "/sections/ctb-bitstream-metadata/ctb.oaireXXlicenseCondition",
                [build_value(row.get("license"))],
                False,
            ),
        ]

        for path, value, is_repeatable in fields:
            if value and not all(
                v is None for v in value if isinstance(value, list)
            ):  # Skip invalid or empty values
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
        metadata_definitions.append(
            {"op": "add", "path": "/sections/license/granted", "value": "true"}
        )

        # logger.debug(f"metadata_definitions : {metadata_definitions}")

        if not metadata_definitions:
            logger.warning(
                "No operations constructed; required paths might be missing."
            )

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
        return dspace_wrapper.update_workspace(workspace_id, patch_operations)

    def _add_file(self, workspace_id, file_path):
        return dspace_wrapper.upload_file_to_workspace(workspace_id, file_path)

    def _filter_publications_by_valid_affiliations(self):
        """Filter publications with valid author affiliations."""
        valid_author_ids = self.df_epfl_authors[
            self.df_epfl_authors["epfl_api_mainunit_name"].notnull()
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

            workspace_response = dspace_wrapper.push_publication(
                source, source_id, collection_id
            )

            valid_pdf = row.get("upw_valid_pdf", "")
            file_path = (
                f"../data/pdfs/{valid_pdf}"
                if pd.notna(valid_pdf) and valid_pdf != ""
                else None
            )

            if workspace_response and "id" in workspace_response:
                workspace_id = workspace_response["id"]
                logger.info(f"Successfully pushed publication with ID: {workspace_id}")
                df_items_imported.at[index, "workspace_id"] = workspace_id

                matching_authors = self.df_epfl_authors[
                    self.df_epfl_authors["row_id"] == row["row_id"]
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

                    workflow_response = dspace_wrapper.create_workflowitem(
                        workspace_id
                    )
                    if workflow_response and "id" in workflow_response:
                        workflow_id = workflow_response["id"]
                        logger.info(
                            f"Successfully created workflow item with ID: {workflow_id}"
                        )
                        df_items_imported.at[index, "workflow_id"] = workflow_id
                else:
                    logger.warning(
                        f"No matching units found for row ID: {row['row_id']}."
                    )
            else:
                logger.error(
                    f"Failed to push publication with source: {row['source']}, internal_id: {row['internal_id']}, and collection_id: {row['ifs3_collection_id']}"
                )

        return df_items_imported

"""Loader processor for ingesting items into DSpace."""

import os
import re
import math
from pathlib import Path
import pandas as pd
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import licenses_mapping, versions_mapping, collections_mapping
from utils import manage_logger
from config import logs_dir

log_file_path = os.path.join(logs_dir, "logging.log")
logger = manage_logger(log_file_path)

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
pdf_dir = project_root / "data" / "pdfs"

dspace_wrapper = DSpaceClientWrapper()

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _is_nan_like(x) -> bool:
    """Return True if x behaves like a NaN (float('nan'), numpy.nan, pandas NA)."""
    try:
        # NaN != NaN, while None == None
        return x != x
    except Exception:
        return False


def _sanitize_value(v):
    """
    Recursively sanitize any JSON-like payload:
    - Replace NaN/Inf with None
    - Strip strings
    - Keep dict/list structure intact
    """
    if v is None:
        return None
    if _is_nan_like(v):
        return None
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(v, list):
        return [_sanitize_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _sanitize_value(val) for k, val in v.items()}
    return v


def _sanitize_ops(ops):
    """Sanitize a list of JSON-PATCH operations so json.dumps never sees NaN/Inf."""
    if not isinstance(ops, list):
        return ops
    out = []
    for op in ops:
        if not isinstance(op, dict):
            continue
        sanitized = {
            "op": op.get("op"),
            "path": op.get("path"),
            "value": _sanitize_value(op.get("value")),
        }
        out.append(sanitized)
    return out


def _normalize_ws_response(resp, fallback=None):
    """
    Ensure we always deal with a dict for workspace responses.
    Some environments/wrappers may return True/False/None when there is no JSON body.
    """
    if isinstance(resp, dict):
        return resp
    return fallback if isinstance(fallback, dict) else (fallback or {})


class Loader:
    """Load items into DSpace using workflow."""

    def __init__(self, df_metadata, df_epfl_authors, df_authors):
        self.df_metadata = df_metadata
        self.df_epfl_authors = df_epfl_authors
        self.df_authors = df_authors

        script_dir = Path(__file__).resolve().parent
        project_root = script_dir.parent
        self.pdf_dir = (project_root / "data" / "pdfs").resolve()

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
        """Replace and enrich dc.contributor.author.

        Skip any rows where `role` equals 'contributor' (case-insensitive).
        All other roles — including creator, author, empty, or NaN — are processed.
        """
        if (
            "sections" not in workspace_response
            or f"{form_section}details" not in workspace_response["sections"]
        ):
            logger.warning("No authors section found in workspace response.")
            return []

        subset = self.df_authors[self.df_authors["row_id"] == row_id].copy()
        if subset.empty:
            logger.warning(
                "No matching authors found in df_authors for row_id: %s.", row_id
            )
            return []

        # Drop contributors
        if "role" in subset.columns:
            role_series = subset["role"].astype(str).str.strip().str.lower()
            subset = subset[~role_series.eq("contributor")]
            if subset.empty:
                logger.info(
                    "All rows for row_id %s are contributors; skipping author patch.",
                    row_id,
                )
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
            Return the first segment of a delimited string, removing optional numeric/ROR prefixes.
            Fallback to default if empty or NaN.
            """
            if pd.notna(value):
                s = str(value).strip()
                if s:
                    first_part = s.split(delimiter, 1)[0].strip()
                    if re.match(
                        r"^(?:\d+|[0-9]{2}[a-z0-9]{7}):", first_part, re.IGNORECASE
                    ):
                        return first_part.split(":", 1)[1].strip()
                    return first_part
            return default

        authors_metadata = []
        affiliations_metadata = []
        orcid_metadata = []
        roles_metadata = []

        # Build metadata blocks
        for _, author_row in subset.iterrows():
            authors_metadata.append(
                create_metadata(str(author_row.get("author", "")).strip())
            )

            orcid_value = author_row.get("orcid_id")
            epfl_orcid_value = author_row.get("epfl_orcid")

            if pd.notna(orcid_value) and str(orcid_value).strip():
                orcid_metadata.append(create_metadata(str(orcid_value).strip()))
            elif pd.notna(epfl_orcid_value) and str(epfl_orcid_value).strip():
                orcid_metadata.append(create_metadata(str(epfl_orcid_value).strip()))
            else:
                orcid_metadata.append(
                    create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#")
                )

            if str(author_row.get("source", "")).lower() != "crossref":
                affiliation_name = get_first_split(author_row.get("organizations", ""))
                affiliations_metadata.append(create_metadata(affiliation_name))
            else:
                affiliations_metadata.append(
                    create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#")
                )

            roles_metadata.append(create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#"))

        # Authority enrichment
        for i, author in enumerate(authors_metadata):
            author_name = author["value"]
            matching_epfl_author = self.df_epfl_authors[
                (self.df_epfl_authors["row_id"] == row_id)
                & (self.df_epfl_authors["author"] == author_name)
            ]

            if matching_epfl_author.empty:
                continue

            for _, match in matching_epfl_author.iterrows():
                sciper = match.get("sciper_id")
                if pd.notna(sciper):
                    prefix = (
                        "will be referenced::"
                        if pd.notna(match.get("dspace_uuid"))
                        else "will be generated::"
                    )
                    author.update(
                        authority=f"{prefix}SCIPER-ID::{sciper}",
                        confidence=600,
                    )

                if pd.notna(match.get("organizations")):
                    affiliations_metadata[i] = create_metadata(
                        "École Polytechnique Fédérale de Lausanne",
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
                "path": f"/sections/{form_section}details/epfl.author.orcid",
                "value": orcid_metadata,
            },
            {
                "op": "add",
                "path": f"/sections/{form_section}details/epfl.author.role",
                "value": roles_metadata,
            },
        ]

        return patch_operations

    def _process_and_add_contributors(self, workspace_response, row_id, form_section):
        """Add Zenodo/DataFrame contributors (role == 'contributor') as DSpace contributors.

        Builds four ADD operations:
        - /sections/{form_section}details/epfl.contributor.role
        - /sections/{form_section}details/dc.contributor
        - /sections/{form_section}details/oairecerif.contributor.affiliation
        - /sections/{form_section}details/epfl.contributor.orcid

        Keeps order from the DataFrame. Skips if there are no contributors.
        """
        if (
            "sections" not in workspace_response
            or f"{form_section}details" not in workspace_response["sections"]
        ):
            logger.warning("No contributors section found in workspace response.")
            return []

        subset = self.df_authors[self.df_authors["row_id"] == row_id].copy()
        if subset.empty:
            return []

        # Keep only contributors
        if "role" in subset.columns:
            subset = subset[
                subset["role"].astype(str).str.strip().str.lower().eq("contributor")
            ]
        if subset.empty:
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
            """Return first segment, removing optional numeric/ROR prefix if present."""
            if pd.notna(value):
                s = str(value).strip()
                if s:
                    first_part = s.split(delimiter, 1)[0].strip()
                    if re.match(
                        r"^(?:\d+|[0-9]{2}[a-z0-9]{7}):", first_part, re.IGNORECASE
                    ):
                        return first_part.split(":", 1)[1].strip()
                    return first_part
            return default

        roles_meta = []
        names_meta = []
        affils_meta = []
        orcids_meta = []

        # Build blocks in order
        for _, row in subset.iterrows():
            name = str(row.get("author", "")).strip()
            if not name:
                # Skip malformed entries
                continue

            # For now we keep a placeholder for contributor role value
            roles_meta.append(create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#"))

            # ORCID: prefer orcid_id then epfl_orcid
            orcid_val = row.get("orcid_id")
            epfl_orcid_val = row.get("epfl_orcid")
            if pd.notna(orcid_val) and str(orcid_val).strip():
                orcids_meta.append(create_metadata(str(orcid_val).strip()))
            elif pd.notna(epfl_orcid_val) and str(epfl_orcid_val).strip():
                orcids_meta.append(create_metadata(str(epfl_orcid_val).strip()))
            else:
                orcids_meta.append(create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#"))

            # Affiliation: from organizations unless source == crossref
            if str(row.get("source", "")).lower() != "crossref":
                aff_name = get_first_split(row.get("organizations", ""))
                affils_meta.append(create_metadata(aff_name))
            else:
                affils_meta.append(create_metadata("#PLACEHOLDER_PARENT_METADATA_VALUE#"))

            # Name authority enrichment (SCIPER → authority + confidence)
            authority = None
            confidence = -1
            matching_epfl = self.df_epfl_authors[
                (self.df_epfl_authors["row_id"] == row_id)
                & (self.df_epfl_authors["author"] == name)
            ]
            if not matching_epfl.empty:
                for _, m in matching_epfl.iterrows():
                    sciper = m.get("sciper_id")
                    if pd.notna(sciper):
                        prefix = (
                            "will be referenced::"
                            if pd.notna(m.get("dspace_uuid"))
                            else "will be generated::"
                        )
                        authority = f"{prefix}SCIPER-ID::{sciper}"
                        confidence = 600
                        break

                # If EPFL affiliation is confirmed in match, override affiliation with EPFL + ROR authority
                if any(
                    pd.notna(m.get("organizations")) for _, m in matching_epfl.iterrows()
                ):
                    affils_meta[-1] = {
                        "value": "École Polytechnique Fédérale de Lausanne",
                        "authority": "will be referenced::ROR-ID::https://ror.org/02s376052",
                        "display": "École Polytechnique Fédérale de Lausanne",
                        "confidence": 600,
                    }

            names_meta.append(
                {
                    "value": name,
                    "authority": authority,
                    "display": name,
                    "confidence": confidence,
                }
            )

        if not names_meta:
            return []

        base = f"/sections/{form_section}details"
        ops = [
            {"op": "add", "path": f"{base}/epfl.contributor.role", "value": roles_meta},
            {"op": "add", "path": f"{base}/dc.contributor", "value": names_meta},
            {
                "op": "add",
                "path": f"{base}/oairecerif.contributor.affiliation",
                "value": affils_meta,
            },
            {"op": "add", "path": f"{base}/epfl.contributor.orcid", "value": orcids_meta},
        ]
        return ops

    def _patch_additional_metadata(
        self, workspace_id, row, units, ifs3_collection_id, workspace_response
    ):
        """Update only necessary fields based on errors returned in workspace_response."""
        form_section = self._get_form_section(ifs3_collection_id)
        logger.debug(
            f"Collection ID: '{ifs3_collection_id}' and section name: '{form_section}'."
        )
        if not form_section:
            logger.error(
                f"Invalid collection ID: {ifs3_collection_id}. Unable to determine form section."
            )
            return

        try:
            # 1) REMOVE operations for pre-existing metadata
            remove_operations = self._construct_remove_operations(
                workspace_response, form_section
            )

            if remove_operations:
                try:
                    _resp = dspace_wrapper.update_workspace(
                        workspace_id, _sanitize_ops(remove_operations)
                    )
                    updated_workspace = _normalize_ws_response(_resp, workspace_response)
                except Exception as e:
                    logger.error(f"Failed to execute remove operations: {e}")
                    updated_workspace = workspace_response
            else:
                updated_workspace = workspace_response

            required_paths = []
            if isinstance(updated_workspace, dict) and "errors" in updated_workspace:
                for error in updated_workspace.get("errors", []):
                    try:
                        msg = error.get("message")
                        if msg in ("error.validation.required", "error.validation.license.required"):
                            required_paths.extend(error.get("paths", []))
                    except Exception:
                        # Defensive: ignore malformed error entries
                        pass

            logger.debug(f"Required paths to update: {required_paths}")

            # 2) BUILD patch operations (ADD/REPLACE)
            patch_operations = self._construct_patch_operations(
                row, units, form_section, updated_workspace
            )

            # Authors
            author_patch = self._process_and_replace_authors(
                updated_workspace, row["row_id"], form_section
            )
            if author_patch:
                patch_operations.extend(author_patch)

            # Contributors
            contrib_patch = self._process_and_add_contributors(
                updated_workspace, row["row_id"], form_section
            )
            if contrib_patch:
                patch_operations.extend(contrib_patch)

            logger.debug("Patch operations (pre-sanitize): %s", patch_operations)

            # Sanitize JSON payload to avoid NaN/Inf issues
            patch_operations = _sanitize_ops(patch_operations)

            # 3) APPLY patch operations
            try:
                _resp = dspace_wrapper.update_workspace(
                    workspace_id, patch_operations
                )
                response = _normalize_ws_response(_resp, {})
            except Exception as e:
                logger.error("Failed to execute patch operations: %s", e)
                return

            # Handle any reported errors
            if not isinstance(response, dict):
                logger.warning("Non-dict response after patch; cannot inspect errors.")
                return

            for error in response.get("errors", []) or []:
                error_message = error.get("message", "No message provided")
                error_paths = ", ".join(error.get("paths", [])) or "No paths provided"
                logger.error(f"Error message: {error_message}")
                logger.error(f"Paths concerned: {error_paths}")

            logger.info(f"Metadata patched successfully for workspace {workspace_id}.")

        except Exception as e:
            logger.error(f"An error occurred while patching additional metadata: {e}")

    def _metadata_exists(self, path, workspace_response):
        """Check if the metadata exists in the workspace response."""
        if not isinstance(workspace_response, dict):
            return False

        sections = workspace_response.get("sections", {}) or {}
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
            f"/sections/{form_section}details/dc.title",
            f"/sections/{form_section}details/dc.contributor.author",
            f"/sections/{form_section}details/oairecerif.author.affiliation",
            f"/sections/{form_section}details/person.identifier.orcid",
            f"/sections/{form_section}details/epfl.author.role",
            f"/sections/{form_section}details/epfl.author.orcid",    
            "/sections/bookcontainer_details/dc.relation.ispartof",
            "/sections/journalcontainer_details/dc.relation.journal",
            "/sections/journalcontainer_details/dc.relation.issn",
            "/sections/journalcontainer_details/oaire.citation.volume",
            "/sections/related_works/datacite.relationType",
            "/sections/related_works/dc.relation.title",
            "/sections/related_works/datacite.relatedIdentifier",
        ]

        for path in removable_metadata_paths:
            if self._metadata_exists(path, workspace_response):
                metadata_definitions.append({"op": "remove", "path": path})

        return metadata_definitions

    def _construct_patch_operations(self, row, units, form_section, workspace_response):
        """Construct PATCH operations for metadata updates with optimized error handling."""

        def build_value(value, authority=None, language=None, confidence=-1, place=0):
            """Helper to build a metadata value structure (skip blank strings/None)."""
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
            """Return 'replace' for non-repeatable when exists, else 'add'."""
            if not is_repeatable and self._metadata_exists(path, workspace_response):
                return "replace"
            return "add"

        def parse_conference_info(conference_info):
            """Parse 'confName::place::start::end::acronym[||...]' into structured metadata."""
            operations = []
            if not conference_info:
                return operations

            # Choose section based on form_section
            section_name = "related_event" if form_section == "dataset_" else "conference_event"

            conferences = conference_info.split("||")

            conference_types = []
            conference_names = []
            conference_places = []
            conference_dates = []
            conference_acronyms = []

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
                acronym = parts[4].strip() if len(parts) > 4 and parts[4].strip() else None

                if name:
                    conference_names.append(build_value(name))

                if place:
                    conference_places.append(build_value(place))

                if start_date and end_date:
                    conference_dates.append(build_value(f"{start_date} - {end_date}"))
                else:
                    conference_dates.append(
                        build_value(
                            "#PLACEHOLDER_PARENT_METADATA_VALUE#",
                            confidence=-1,
                            language=None,
                        )
                    )

                if acronym:
                    conference_acronyms.append(build_value(acronym))
                else:
                    conference_acronyms.append(
                        build_value(
                            "#PLACEHOLDER_PARENT_METADATA_VALUE#",
                            confidence=-1,
                            language=None,
                        )
                    )

                conference_types.append(build_value("conference"))

            if conference_types:
                operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{section_name}/epfl.relation.conferenceType",
                        "value": conference_types,
                    }
                )
            if conference_names:
                operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{section_name}/dc.relation.conference",
                        "value": conference_names,
                    }
                )
            if conference_places:
                operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{section_name}/oaire.citation.conferencePlace",
                        "value": conference_places,
                    }
                )
            if conference_dates:
                operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{section_name}/oaire.citation.conferenceDate",
                        "value": conference_dates,
                    }
                )
            if conference_acronyms:
                operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{section_name}/oairecerif.acronym",
                        "value": conference_acronyms,
                    }
                )
            else:
                operations.append(
                    {
                        "op": "add",
                        "path": f"/sections/{section_name}/oairecerif.acronym",
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

        def parse_additional_link(additional_url: str):
            """
            Parse 'label::url[||label::url...]' into two field lists:
            - /sections/additional_fields/epfl.url             → labels
            - /sections/additional_fields/epfl.url.description → urls
            """
            ops = []
            if not additional_url:
                return ops

            raw_entries = [e.strip() for e in str(additional_url).split("||") if e.strip()]
            if not raw_entries:
                return ops

            labels, descriptions = [], []
            for entry in raw_entries:
                # Expect "label::url"
                parts = [p.strip() for p in entry.split("::", 1)]
                if len(parts) != 2:
                    # Fallback: treat whole entry as URL with a generic label
                    label, url = "Additional Link", entry
                else:
                    label, url = parts[0], parts[1]

                if not url:
                    continue
                if not label:
                    label = "Additional Link"

                labels.append(label)
                descriptions.append(url)

            if not labels:
                return ops

            ops.append(self._create_op("/sections/additional_fields/epfl.url", labels))
            ops.append(self._create_op("/sections/additional_fields/epfl.url.description", descriptions))
            return ops

        def parse_related_works(related_works):
            """
            Convert 'relation::identifier||...' into 3 PATCH operations:
            - datacite.relationType
            - dc.relation.title (placeholder)
            - datacite.relatedIdentifier
            """
            operations = []
            logger.debug("parse_related_works IN=%r", related_works)
            if not related_works:
                return operations

            entries = [e.strip() for e in str(related_works).split("||") if e.strip()]
            if not entries:
                return operations

            rel_types, titles, identifiers = [], [], []

            for e in entries:
                parts = [p.strip() for p in e.split("::", 1)]
                if len(parts) != 2:
                    continue
                rel, ident = parts[0], parts[1]
                if not rel or not ident:
                    continue

                # Normalize bare DOI into https URL if needed
                if ident.lower().startswith("10."):
                    ident = f"https://doi.org/{ident}"

                rel_types.append(rel)
                identifiers.append(ident)
                titles.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")

            if not rel_types:
                return operations

            operations.append(self._create_op("/sections/related_works/datacite.relationType", rel_types))
            operations.append(self._create_op("/sections/related_works/dc.relation.title", titles))
            operations.append(self._create_op("/sections/related_works/datacite.relatedIdentifier", identifiers))

            return operations

        def parse_funding_info(funding_info):
            """Parse 'Funder::GrantNo[||...]' into grants-related fields."""
            funders, funding_names, grant_nos, award_uris = [], [], [], []

            if not funding_info:
                return []

            grants = funding_info.split("||")

            for grant in grants:
                parts = grant.split("::", 1)
                funder = parts[0].strip() if parts and parts[0].strip() else None
                grantno = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None

                if funder:
                    funders.append(funder)
                    grant_nos.append(grantno if grantno else "#PLACEHOLDER_PARENT_METADATA_VALUE#")
                    funding_names.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")
                    award_uris.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")

            operations = []
            if funders:
                operations.append(self._create_op("/sections/grants/oairecerif.funder", funders))
                if funding_names:
                    operations.append(self._create_op("/sections/grants/dc.relation.funding", funding_names))
                if grant_nos:
                    operations.append(self._create_op("/sections/grants/dc.relation.grantno", grant_nos))
                if award_uris:
                    operations.append(self._create_op("/sections/grants/crisfund.award.uri", award_uris))

            return operations

        def parse_editors(editors):
            """Parse editors string 'A||B||...' into editor, affiliation, orcid placeholders."""
            if not editors:
                return []

            editors_list, affiliations, orcids = [], [], []
            for editor in editors.split("||"):
                editor_name = editor.strip()
                if editor_name:
                    editors_list.append(editor_name)
                    affiliations.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")
                    orcids.append("#PLACEHOLDER_PARENT_METADATA_VALUE#")

            operations = []
            if editors_list:
                operations.append(
                    self._create_op(
                        "/sections/bookcontainer_details/dc.contributor.scientificeditor",
                        editors_list,
                    )
                )
                operations.append(
                    self._create_op(
                        "/sections/bookcontainer_details/oairecerif.scientificeditor.affiliation",
                        affiliations,
                    )
                )
                operations.append(
                    self._create_op(
                        "/sections/bookcontainer_details/epfl.scientificeditor.orcid",
                        orcids,
                    )
                )
            return operations

        def parse_access_conditions(access_conditions: str):
            """
            Build patch operation for /sections/itemAccessConditions/accessConditions.
            - Ignore if empty value
            """
            if not access_conditions or not str(access_conditions).strip():
                return []

            return [{
                "op": "add",
                "path": "/sections/itemAccessConditions/accessConditions",
                "value": [{"name": str(access_conditions).strip()}],
            }]

        def parse_language(language_code: str):
            """
            Add dataset language without any mapping/conversion.
            Expects a 2-letter ISO code already prepared upstream (e.g., 'en').
            """
            if not language_code or not str(language_code).strip():
                return []
            return [{
                "op": "add",
                "path": "/sections/dataset_details/dc.language.iso",
                "value": [{
                    "value": str(language_code).strip(),
                    "language": None,
                    "authority": None,
                    "confidence": -1,
                    "place": 0
                }]
            }]

        def parse_version(version_str: str):
            """
            Add dataset version as-is (no mapping).
            """
            if not version_str or not str(version_str).strip():
                return []
            return [{
                "op": "add",
                "path": "/sections/dataset_details/dc.description.version",
                "value": [{
                    "value": str(version_str).strip(),
                    "language": None,
                    "authority": None,
                    "confidence": -1,
                    "place": 0
                }]
            }]

        # Determine correct form_section and related sections
        type_section = f"{form_section}{'details' if form_section in ['conference_', 'book_', 'dataset_'] else 'type'}"
        dc_type = row.get("dc.type")

        refereed = None if form_section in ("preprint_", "dataset_") else "REVIEWED"

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
            alter_id_section = "book_details"
        else:
            pagination_section = "journalcontainer_details"
            isbn_section = "bookcontainer_details"
            isbn_metadata = "dc.relation.isbn"
            alter_id_section = "alternative_identifiers"

        if form_section == "dataset_":
            publisher_container = "dataset_details"
        elif dc_type in ["text::preprint"]:
            publisher_container = "preprint_details"
        else:
            publisher_container = "bookcontainer_details"

        metadata_definitions = []

        journal_issn = str(row.get("journalISSN", ""))
        issn_list = [issn.strip() for issn in journal_issn.split("||") if issn.strip()]
        authority_journal = f"will be generated::ISSN::{issn_list[0]}" if issn_list else None

        acronyms = [unit.get("acro") for unit in units if unit.get("acro")]
        if len(acronyms) > 1 and "EPFL" in acronyms:
            acronyms = [acro for acro in acronyms if acro != "EPFL"]

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
                f"/sections/{form_section}details/dc.title",
                [build_value(row.get("title"))],
                False,
            ),
            (
                f"/sections/{form_section}details/dc.date.issued",
                [build_value(row.get("issueDate"))],
                False,
            ),
            (
                f"/sections/{alter_id_section}/dc.identifier.pmid",
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
                f"/sections/{publisher_container}/dc.publisher",
                [build_value(row.get("publisher"))],
                False,
            ),
            (
                f"/sections/{publisher_container}/dc.publisher.place",
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
                        acro,
                        f"will be referenced::ACRONYM::{acro}",
                        confidence=600,
                    )
                    for acro in acronyms
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
                [build_value(refereed)],
                False,
            ),
            (
                "/sections/ctb-bitstream-metadata/ctb.oaireXXlicenseCondition",
                [
                    build_value(
                        licenses_mapping.get(row.get("license"), {}).get("value")
                        or row.get("license")
                    )
                ],
                False,
            ),
        ]
        logger.debug(f"Constructed initial metadata fields: {fields}")

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

        if form_section not in ["dataset_"]:
            metadata_definitions.extend(parse_funding_info(row.get("fundings_info")))
            metadata_definitions.extend(parse_editors(row.get("editors")))
        else:
            metadata_definitions.extend(parse_language(row.get("language")))
            metadata_definitions.extend(parse_version(row.get("version")))
            metadata_definitions.extend(parse_additional_link(row.get("additional_url")))
            metadata_definitions.extend(parse_access_conditions(row.get("access_conditions")))
            metadata_definitions.extend(parse_related_works(row.get("related_works")))

        metadata_definitions.extend(parse_conference_info(row.get("conference_info")))

        # Add specific patch for license/granted (as string "true" per your payload examples)
        metadata_definitions.append(
            {"op": "add", "path": "/sections/license/granted", "value": True}
        )

        if not metadata_definitions:
            logger.warning("No operations constructed; required paths might be missing.")

        return metadata_definitions

    def _patch_file_metadata(self, workspace_id, upw_license, upw_version):
        """Patch metadata for file."""
        license_metadata = licenses_mapping.get(upw_license)
        version_metadata = versions_mapping.get(upw_version or "None")

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
        # Sanitize before sending
        patch_operations = _sanitize_ops(patch_operations)
        _resp = dspace_wrapper.update_workspace(workspace_id, patch_operations)
        return _normalize_ws_response(_resp, {})

    def _add_file(self, workspace_id, file_path):
        return dspace_wrapper.upload_file_to_workspace(workspace_id, file_path)

    def _filter_publications_by_valid_affiliations(self):
        """Filter publications with valid author affiliations."""
        valid_author_ids = self.df_epfl_authors[
            self.df_epfl_authors["final_mainunit"].notnull()
        ]["row_id"].unique()

        if len(valid_author_ids) > 0:
            filtered_publications = self.df_metadata[
                self.df_metadata["row_id"].isin(valid_author_ids)
            ]
            logger.info(f"Filtered publications count: {len(filtered_publications)}")
            return filtered_publications
        else:
            logger.warning("No valid authors found with 'final_mainunit'.")
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
            if source == "openalex+crossref":
                source = "crossref"
            elif source == "zenodo":
                source = "datacite"

            workspace_response = dspace_wrapper.push_publication(
                source, source_id, collection_id
            )

            valid_pdf = row.get("upw_valid_pdf", "")
            # If valid_pdf is already an absolute path, keep it as-is
            file_path = (
                pdf_dir / valid_pdf
                if pd.notna(valid_pdf) and str(valid_pdf).strip()
                else None
            )
            logger.debug(
                f"Path to PDF file : {file_path} - Exists: {file_path.exists() if file_path else 'None'}"
            )

            if workspace_response and isinstance(workspace_response, dict) and "id" in workspace_response:
                workspace_id = workspace_response["id"]
                logger.info(f"Successfully pushed publication with ID: {workspace_id}")
                df_items_imported.at[index, "workspace_id"] = workspace_id

                matching_authors = self.df_epfl_authors[
                    self.df_epfl_authors["row_id"] == row["row_id"]
                ]
                units = [
                    {"acro": author["final_mainunit"]}
                    for _, author in matching_authors.iterrows()
                    if pd.notna(author["final_mainunit"])
                    and author["final_mainunit"] != ""
                ]
                unique_units = {unit["acro"]: unit for unit in units}.values()
                logger.debug(f"Found units: {unique_units}")

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
                        if hasattr(file_response, "status_code") and file_response.status_code in [200, 201]:
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
                                f"Failed to add file to workspace item {workspace_id}. "
                                f"Status: {getattr(file_response, 'status_code', 'unknown')}."
                            )
                    else:
                        logger.warning(
                            f"File {file_path} does not exist. Skipping file upload."
                        )

                    workflow_response = dspace_wrapper.create_workflowitem(
                        workspace_id
                    )
                    if workflow_response and isinstance(workflow_response, dict) and "id" in workflow_response:
                        workflow_id = workflow_response["id"]
                        logger.info(
                            f"Successfully created workflow item with ID: {workflow_id}"
                        )
                        df_items_imported.at[index, "workflow_id"] = workflow_id
                    else:
                        logger.error(f"Unable to create workflow item for workspace item {workspace_id}")
                        df_items_imported.at[index, "workflow_id"] = None
                else:
                    logger.warning(
                        f"No matching units found for row ID: {row['row_id']}."
                    )
            else:
                logger.error(
                    f"Failed to push publication with source: {row.get('source')}, "
                    f"internal_id: {row.get('internal_id')}, and collection_id: {row.get('ifs3_collection_id')}"
                )

        return df_items_imported

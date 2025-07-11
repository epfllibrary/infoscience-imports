import os
from pathlib import Path
import pandas as pd
from clients.dspace_client_wrapper import DSpaceClientWrapper
from mappings import licenses_mapping, versions_mapping
from utils import manage_logger
from config import logs_dir

log_file_path = os.path.join(logs_dir, "pdf_updater.log")
logger = manage_logger(log_file_path)

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
pdf_dir = (project_root / "data" / "pdfs").resolve()

dspace_wrapper = DSpaceClientWrapper()


class PDFUpdater:
    """Class responsible for updating the PDF and its metadata in DSpace."""

    def __init__(self, df_metadata):
        self.df_metadata = df_metadata

    def _patch_file_metadata(self, uuid, upw_license, upw_version):
        """Patch license and version metadata for the uploaded file."""
        license_metadata = licenses_mapping.get(upw_license)
        version_metadata = versions_mapping.get(upw_version or "None")

        if not license_metadata:
            logger.error("Invalid license: %s", upw_license)
        if not version_metadata:
            logger.error("Invalid version: %s", upw_version)

        patch_operations = [
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/metadata/dc.type",
                "value": [
                    {
                        "value": "main document",
                        "display": "Main document",
                        "confidence": -1,
                    }
                ],
            },
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/metadata/oaire.version",
                "value": [
                    {
                        "value": version_metadata["value"],
                        "display": version_metadata["display"],
                        "confidence": -1,
                    }
                ],
            },
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/metadata/oaire.licenseCondition",
                "value": [
                    {
                        "value": license_metadata["value"],
                        "display": license_metadata["display"],
                        "confidence": -1,
                    }
                ],
            },
            {
                "op": "add",
                "path": "/sections/upload-publication/files/0/accessConditions",
                "value": [{"name": "openaccess"}],
            },
        ]

        return dspace_wrapper.update_adminitem(uuid, patch_operations)

    def _add_pdf(self, uuid, file_path):
        return dspace_wrapper.add_file_adminitem(uuid, file_path)

    def update_pdfs(self):
        """Iterates over the DataFrame and updates the PDF file in DSpace workspaces."""
        updated_rows = []

        for index, row in self.df_metadata.iterrows():
            uuid = row.get("uuid")
            pdf_filename = row.get("upw_valid_pdf")

            if not uuid:
                logger.warning("Missing uuid for row %s. Skipping.", index)
                continue

            file_path = (
                pdf_dir / pdf_filename
                if pd.notna(pdf_filename) and pdf_filename
                else None
            )

            if not file_path or not file_path.exists():
                logger.warning(
                    "Missing or invalid PDF file for row %s: %s", index, file_path
                )
                continue

            # Upload the file
            response = self._add_pdf(uuid, file_path)
            logger.info("Uploading PDF for item %s: %s", uuid, response)
            if response and response.status_code in (200, 201):
                logger.info("Successfully uploaded PDF for item %s", uuid)
                # Patch file metadata
                self._patch_file_metadata(
                    uuid, row.get("upw_license"), row.get("upw_version")
                )
                updated_rows.append(index)
            else:
                logger.error(
                    "Failed to upload PDF for workspace %s. Status: %s",
                    uuid,
                    response.status_code if response else "No response"
                )

        return self.df_metadata.loc[updated_rows]

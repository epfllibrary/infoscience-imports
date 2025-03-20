"""
This module contains the GenerateReports class that generates 
a consolidated report with the required indicators and corresponding data rows.
"""
import os
import smtplib
import datetime
from email.message import EmailMessage
import pandas as pd
from config import logs_dir
from utils import manage_logger

class GenerateReports:
    def __init__(self, dataframe, df_unloaded, df_epfl_authors, df_loaded):
        """Initialize the GenerateReports with given DataFrames."""
        log_file_path = os.path.join(logs_dir, "logging.log")
        self.logger = manage_logger(log_file_path)

        self.df = dataframe.copy()
        self.df_unloaded = df_unloaded.copy()
        self.df_epfl_authors = df_epfl_authors.copy()
        self.df_loaded = df_loaded.copy()
        # Ensure 'upw_is_oa' column is boolean, replacing NaN with False
        if "upw_is_oa" in self.df.columns:
            self.df["upw_is_oa"] = (
                self.df["upw_is_oa"].fillna(False).infer_objects(copy=False).astype(bool)
            )

    def total_publications_found(self):
        """Return the total number of unique publications found."""
        return self.df["row_id"].nunique(), self.df

    def publications_by_source(self):
        """Return the number of publications grouped by source."""
        return self.df.groupby("source").size().reset_index(name="count"), self.df

    def publications_by_collection(self):
        """Return the number of publications grouped by collection."""
        return (
            self.df.groupby("dc.type").size().reset_index(name="count"),
            self.df.groupby("dc.type").size().reset_index(name="count"),
        )

    def open_access_publications(self):
        """Return the number of open access publications grouped by license and OA status."""
        df_oa = self.df[self.df["upw_is_oa"]]
        return (
            df_oa.groupby(["upw_license", "upw_oa_status"])
            .size()
            .reset_index(name="count"),
            df_oa,
        )

    def open_access_with_pdf(self):
        """Return the number of open access publications with a valid PDF available."""
        df_oa_pdf = self.df[self.df["upw_is_oa"] & self.df["upw_valid_pdf"].notna()]
        return (
            df_oa_pdf.groupby(["upw_license", "upw_oa_status"])
            .size()
            .reset_index(name="count"),
            df_oa_pdf,
        )

    def duplicated_publications_count(self):
        """Return the number of rejected publications due to duplication."""
        return len(self.df_unloaded), self.df_unloaded

    def epfl_affiliated_publications(self):
        """Return the number of unique publications with EPFL affiliation found in the external source."""
        return self.df_epfl_authors["row_id"].nunique(), self.df_epfl_authors

    def epfl_reconciled_authors(self):
        """Return the number of unique publications where an EPFL author has been reconciled."""
        df_reconciled = self.df_epfl_authors[self.df_epfl_authors["sciper_id"].notna()]
        return df_reconciled["row_id"].nunique(), df_reconciled

    def epfl_reconciled_authors_with_unit(self):
        """Return the number of unique publications where an EPFL author has been reconciled with their unit."""
        df_reconciled_unit = self.df_epfl_authors[
            (self.df_epfl_authors["sciper_id"].notna())
            & (self.df_epfl_authors["epfl_api_mainunit_name"].notna())
        ]
        return df_reconciled_unit["row_id"].nunique(), df_reconciled_unit

    def imported_publications_workspace(self):
        """Return the number of imported publications in workspace (drafts)."""
        df_workspace = self.df_loaded[self.df_loaded["workspace_id"].notna()]
        return df_workspace.shape[0], df_workspace

    def imported_publications_workflow(self):
        """Return the number of imported publications in workflow."""
        df_workflow = self.df_loaded[self.df_loaded["workflow_id"].notna()]
        return df_workflow.shape[0], df_workflow

    def imported_publications_by_journal(self):
        """Return the number of imported publications grouped by journal title."""
        df_workflow = self.df_loaded[self.df_loaded["workflow_id"].notna()]
        return df_workflow.groupby("journalTitle").size().reset_index(
            name="count"
        ), df_workflow.groupby("journalTitle").size().reset_index(name="count")

    def failed_imports(self):
        """Return the number of publications where import failed and list affected items."""
        df_failed = self.df_loaded[
            self.df_loaded["workspace_id"].isna() & self.df_loaded["workflow_id"].isna()
        ]
        return df_failed.shape[0], df_failed

    def excluded_publications_count(self):
        """Return the number of publications that were excluded (present in df_metadata but not in df_loaded)."""
        df_excluded = self.df[~self.df["row_id"].isin(self.df_loaded["row_id"])]
        return len(df_excluded), df_excluded

    def generate_excel_report(self, file_path=None, output_dir="."):
        """Generate an Excel report with all calculated indicators and corresponding data rows."""
        report = self.generate_report()
        if file_path is None:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_path = f"{timestamp}_Import_Report.xlsx"
        output_path = os.path.join(output_dir, file_path)

        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            for sheet_name, (indicator_value, df_data) in report.items():
                summary_df = pd.DataFrame({sheet_name: [indicator_value]})
                summary_df.to_excel(
                    writer, sheet_name=sheet_name, index=False, startrow=0
                )
                if isinstance(df_data, pd.DataFrame) and not df_data.empty:
                    df_data.to_excel(
                        writer, sheet_name=sheet_name, index=False, startrow=3
                    )

            workbook = writer.book
            for sheet in writer.sheets:
                worksheet = writer.sheets[sheet]
                worksheet.set_column("A:Z", 20)

        return output_path

    def send_report_by_email(
        self,
        recipient_email,
        sender_email,
        smtp_server,
        import_start_date,
        import_end_date,
        file_path=None,
    ):
        """Send the generated Excel report via email without authentication."""
        if file_path is None:
            file_path = self.generate_excel_report()

        msg = EmailMessage()
        msg["Subject"] = "Infoscience Import Report"
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg.set_content(
            f"Please find attached the latest Infoscience Import Report for the date interval from  {import_start_date} to {import_end_date}."
        )

        with open(file_path, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)

        msg.add_attachment(
            file_data,
            maintype="application",
            subtype="octet-stream",
            filename=file_name,
        )

        with smtplib.SMTP(smtp_server) as server:
            server.send_message(msg)
            server.quit()

        self.logger.info(
            f"Email sent successfully to {recipient_email} with attachment {file_name}."
        )

    def generate_report(self):
        """Generate a consolidated report with the required indicators and corresponding data rows."""
        return {
            "Filtered Publications": self.total_publications_found(),
            "Rejected Duplicated": self.duplicated_publications_count(),
            "Rejected Not Reconciliated": self.excluded_publications_count(),
            "Publications by Source": self.publications_by_source(),
            "Publications by Type": self.publications_by_collection(),
            "OA Publications": self.open_access_publications(),
            "OA with PDF": self.open_access_with_pdf(),
            "Imported in Workspace": self.imported_publications_workspace(),
            #"Imported in Workflow": self.imported_publications_workflow(),
            "Imported by Journal": self.imported_publications_by_journal(),
            "Detected EPFL Authors": self.epfl_affiliated_publications(),
            "Matched EPFL Authors": self.epfl_reconciled_authors(),
            "EPFL Authors with Unit": self.epfl_reconciled_authors_with_unit(),
            "Failed Imports": self.failed_imports(),
        }

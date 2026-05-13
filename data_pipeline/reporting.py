"""
This module contains the GenerateReports class that generates 
a consolidated report with the required indicators and corresponding data rows.
"""
import os
import smtplib
import datetime
from email.message import EmailMessage
import pandas as pd
from typing import List, Optional
from utils import get_pipeline_logger

class GenerateReports:
    def __init__(self, dataframe, df_unloaded, df_epfl_authors, df_loaded):
        """Initialize the GenerateReports with given DataFrames."""
        self.logger = get_pipeline_logger(self.__class__.__name__.lower())

        self.df = dataframe.copy()
        self.df_unloaded = df_unloaded.copy()
        self.df_epfl_authors = df_epfl_authors.copy()
        self.df_loaded = df_loaded.copy()
        # Ensure 'upw_is_oa' column is boolean, replacing NaN with False
        if "upw_is_oa" in self.df.columns:
            self.df["upw_is_oa"] = (
                self.df["upw_is_oa"].fillna(False).infer_objects(copy=False).astype(bool)
            )

    # -------------------------
    # Helpers
    # -------------------------
    def _has_cols(self, df: pd.DataFrame, cols: List[str]) -> bool:
        return all(c in df.columns for c in cols)

    def _empty_result(self, cols: Optional[List[str]] = None) -> pd.DataFrame:
        return pd.DataFrame(columns=cols or [])

    def _safe_groupby_count(
        self, df: pd.DataFrame, by: List[str], count_name: str = "count"
    ):
        """
        Return (grouped_counts_df, df_used). If required columns are missing, returns (empty, empty).
        """
        if df is None or df.empty:
            return self._empty_result(by + [count_name]), self._empty_result()
        if not self._has_cols(df, by):
            return self._empty_result(by + [count_name]), self._empty_result()
        g = df.groupby(by).size().reset_index(name=count_name)
        return g, df

    def _safe_filter(self, df: pd.DataFrame, required_cols: List[str], predicate):
        """
        Apply predicate(df) only if required_cols exist; otherwise return empty df.
        """
        if df is None or df.empty:
            return self._empty_result(), self._empty_result()
        if not self._has_cols(df, required_cols):
            return self._empty_result(), self._empty_result()
        out = predicate(df)
        return out, out

    def total_publications_found(self):
        """Return the total number of unique publications found."""
        return self.df["row_id"].nunique(), self.df

    def publications_by_source(self):
        """Return the number of publications grouped by source."""
        return self.df.groupby("source").size().reset_index(name="count"), self.df

    def publications_by_collection(self):
        """Return number of publications grouped by collection/type."""
        g, _ = self._safe_groupby_count(self.df, by=["dc.type"])
        return g, g

    def open_access_publications(self):
        """Return OA publications grouped by license and OA status (when available)."""

        def _pred(df):
            return df[df["upw_is_oa"]]

        df_oa, df_oa_used = self._safe_filter(self.df, ["upw_is_oa"], _pred)
        # group only if license + status exist
        return self._safe_groupby_count(df_oa, by=["upw_license", "upw_oa_status"])

    def open_access_with_pdf(self):
        """Return OA publications with valid PDF available (when available)."""

        def _pred(df):
            return df[df["upw_is_oa"] & df["upw_valid_pdf"].notna()]

        df_oa_pdf, _ = self._safe_filter(self.df, ["upw_is_oa", "upw_valid_pdf"], _pred)
        return self._safe_groupby_count(df_oa_pdf, by=["upw_license", "upw_oa_status"])

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
            & (self.df_epfl_authors["final_mainunit"].notna())
        ]
        return df_reconciled_unit["row_id"].nunique(), df_reconciled_unit

    def imported_publications_workspace(self):
        """Imported publications in workspace (drafts)."""
        if (
            self.df_loaded is None
            or self.df_loaded.empty
            or "workspace_id" not in self.df_loaded.columns
        ):
            return 0, self._empty_result()
        df_workspace = self.df_loaded[self.df_loaded["workspace_id"].notna()]
        return df_workspace.shape[0], df_workspace

    def imported_publications_workflow(self):
        """Imported publications in workflow."""
        if (
            self.df_loaded is None
            or self.df_loaded.empty
            or "workflow_id" not in self.df_loaded.columns
        ):
            return 0, self._empty_result()
        df_workflow = self.df_loaded[self.df_loaded["workflow_id"].notna()]
        return df_workflow.shape[0], df_workflow

    def imported_publications_by_journal(self):
        """Return number of imported publications grouped by journal title (when available)."""
        df_workflow = self.df_loaded
        if df_workflow is None or df_workflow.empty:
            return self._empty_result(["journalTitle", "count"]), self._empty_result()

        # workflow filter only if column exists
        if "workflow_id" in df_workflow.columns:
            df_workflow = df_workflow[df_workflow["workflow_id"].notna()]

        # if journalTitle doesn't exist (e.g., patents), return empty indicator gracefully
        return self._safe_groupby_count(df_workflow, by=["journalTitle"])

    def failed_imports(self):
        """Publications where import failed."""
        if self.df_loaded is None or self.df_loaded.empty:
            return 0, self._empty_result()

        # if both ids missing as columns, can't compute
        if (
            "workspace_id" not in self.df_loaded.columns
            or "workflow_id" not in self.df_loaded.columns
        ):
            return 0, self._empty_result()

        df_failed = self.df_loaded[
            self.df_loaded["workspace_id"].isna() & self.df_loaded["workflow_id"].isna()
        ]
        return df_failed.shape[0], df_failed

    def excluded_publications_count(self):
        """Excluded publications (present in df but not in df_loaded)."""
        if not self._has_cols(self.df, ["row_id"]) or not self._has_cols(
            self.df_loaded, ["row_id"]
        ):
            return 0, self._empty_result()
        df_excluded = self.df[~self.df["row_id"].isin(self.df_loaded["row_id"])]
        return len(df_excluded), df_excluded

    def generate_excel_report(self, file_path=None, output_dir=".", run_id=None):
        """Generate an Excel report with all calculated indicators and corresponding data rows."""
        report = self.generate_report()
        if file_path is None:
            prefix = run_id if run_id else datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_path = f"{prefix}_Import_Report.xlsx"
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
        run_id=None,
        env=None,
    ):
        """Send the generated Excel report via email without authentication."""
        if file_path is None:
            file_path = self.generate_excel_report(run_id=run_id)

        subject = "Infoscience Import Report"
        if env:
            subject += f" [{env.upper()}]"
        if run_id:
            subject += f" — {run_id}"

        body_meta = ""
        if env:
            body_meta += f" (env: {env.upper()})"
        if run_id:
            body_meta += f" [run: {run_id}]"

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg.set_content(
            f"Please find attached the latest Infoscience Import Report{body_meta}"
            f" for the date interval from {import_start_date} to {import_end_date}."
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
            "Imported in Workflow": self.imported_publications_workflow(),
            "Imported by Journal": self.imported_publications_by_journal(),
            "Detected EPFL Authors": self.epfl_affiliated_publications(),
            "Matched EPFL Authors": self.epfl_reconciled_authors(),
            "EPFL Authors with Unit": self.epfl_reconciled_authors_with_unit(),
            "Failed Imports": self.failed_imports(),
        }

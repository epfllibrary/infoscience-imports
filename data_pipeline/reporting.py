import pandas as pd
import os
import datetime


class GenerateReports:
    def __init__(self, dataframe, df_unloaded, df_epfl_authors, df_loaded):
        """Initialize the GenerateReports with given DataFrames."""
        self.df = dataframe.copy()
        self.df_unloaded = df_unloaded.copy()
        self.df_epfl_authors = df_epfl_authors.copy()
        self.df_loaded = df_loaded.copy()
        # Ensure 'upw_is_oa' column is boolean, replacing NaN with False
        self.df["upw_is_oa"] = self.df["upw_is_oa"].fillna(False).astype(bool)

    def total_publications_found(self):
        """Return the total number of unique publications found."""
        return self.df["row_id"].nunique(), self.df

    def publications_by_source(self):
        """Return the number of publications grouped by source."""
        return self.df.groupby("source").size().reset_index(name="count"), self.df

    def publications_by_collection(self):
        """Return the number of publications grouped by collection."""
        return (
            self.df.groupby("ifs3_collection").size().reset_index(name="count"),
            self.df,
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

    def generate_report(self):
        """Generate a consolidated report with the required indicators and corresponding data rows."""
        return {
            "Total Publications Found": self.total_publications_found(),
            "Publications by Source": self.publications_by_source(),
            "Publications by Collection": self.publications_by_collection(),
            "Open Access Publications": self.open_access_publications(),
            "Open Access with PDF": self.open_access_with_pdf(),
            "Duplicated Publications": self.duplicated_publications_count(),
            "EPFL Affiliated Publications": self.epfl_affiliated_publications(),
            "EPFL Reconciled Authors": self.epfl_reconciled_authors(),
            "EPFL Authors with Unit": self.epfl_reconciled_authors_with_unit(),
            "Imported in Workspace": self.imported_publications_workspace(),
            "Imported in Workflow": self.imported_publications_workflow(),
            "Failed Imports": self.failed_imports(),
            "Rejected Publications": self.excluded_publications_count(),
        }

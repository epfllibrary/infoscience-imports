"""Shared UI constants — design tokens, lookup tables, section definitions."""

from __future__ import annotations

# ── Design tokens ─────────────────────────────────────────────────────────────

PRIMARY     = "#632CA6"   # primary accent (Datadog purple)
SECONDARY   = "#7F56D9"   # secondary accent
C_GREEN     = "#16A34A"
C_YELLOW    = "#D97706"
C_RED       = "#DC2626"
C_RED_DARK  = "#991B1B"
C_DARK      = "#101828"
C_BLACK     = "#1D2939"
C_GRAY_600  = "#667085"
C_GRAY_100  = "#E4E7EC"
C_BLUE      = "#3B82F6"

# ── Pipeline sources ──────────────────────────────────────────────────────────

SOURCES: list[str] = ["scopus", "crossref", "openalex+crossref", "openalex", "wos", "datacite", "epo", "zenodo"]

# ── Run statuses ──────────────────────────────────────────────────────────────

RUN_STATUSES: list[str] = ["running", "completed", "failed", "killed"]

# ── Infoscience item statuses (post-import tracking) ──────────────────────────

INFOSCIENCE_STATUSES: list[str] = [
    "published", "withdrawn", "deleted", "rejected", "still_pending",
]

INFOSCIENCE_STATUS_LABELS: dict[str, str] = {
    "published":     "Publié",
    "withdrawn":     "Retiré",
    "deleted":       "Supprimé",
    "rejected":      "Rejeté",
    "still_pending": "En attente",
}

INFOSCIENCE_STATUS_CSS: dict[str, str] = {
    "published":     "ifs-st-published",
    "withdrawn":     "ifs-st-withdrawn",
    "deleted":       "ifs-st-deleted",
    "rejected":      "ifs-st-rejected",
    "still_pending": "ifs-st-pending",
}

# ── Deduplication note labels ─────────────────────────────────────────────────

DEDUP_LABELS: dict[str, str] = {
    "supersedes_preprint":         "Preprint existant",
    "cross_type_doi":              "DOI cross-type",
    "dataset_in_other_collection": "Dataset avec publication liée",
    "published_version_exists":    "Version publiée existante",
}

# ── Metadata sections — raw JSON (new runs) ───────────────────────────────────

RAW_META_SECTIONS: list[tuple[str, list[str]]] = [
    ("Identifiants",      ["doi", "pmid", "bookDOI", "internal_id"]),
    ("Titre & type",      ["title", "doctype", "dc.type", "pubyear", "issueDate"]),
    ("Revue / Article",   ["journalTitle", "journalISSN", "journalVolume", "issue",
                            "startingPage", "endingPage", "artno", "publisher", "publisherPlace"]),
    ("Livre / Série",     ["bookTitle", "seriesTitle", "bookISBN", "seriesISSN",
                            "bookDOI", "bookPart", "editors", "corporateAuthor", "seriesVolume"]),
    ("Mots-clés",         ["keywords"]),
    ("Résumé",            ["abstract"]),
    ("Open Access",       ["upw_is_oa", "upw_oa_status", "upw_license", "upw_version",
                            "upw_host", "upw_pdf_urls", "journal_is_oa", "journal_is_in_doaj"]),
    ("Conférence",        ["conference_info"]),
    ("Financement",       ["fundings_info"]),
    ("Collection DSpace", ["ifs3_collection", "dc.type_authority"]),
]

# ── Metadata sections — DB columns only (old runs without raw_metadata) ───────

DB_META_SECTIONS: list[tuple[str, list[str]]] = [
    ("Identifiants",    ["doi", "internal_id", "dspace_item_uuid"]),
    ("Bibliographique", ["title", "dc_type", "pub_year", "source", "status"]),
    ("Open Access",     ["upw_is_oa", "upw_oa_status", "upw_license", "upw_valid_pdf"]),
    ("Pipeline",        ["run_id", "workspace_id", "workflow_id", "loaded_at"]),
    ("Compteurs",       ["seen_count", "infoscience_dedup_count"]),
]

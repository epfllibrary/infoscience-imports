"""DuckDB persistence layer — Infoscience import pipeline v2.

Stratégie de connexion :
  Toutes les connexions sont courtes (open → execute → close).
  Aucune connexion n'est jamais maintenue ouverte entre deux opérations.
  Cela évite le conflit de verrou DuckDB (une seule connexion write à la fois
  sur macOS / DuckDB ≥ 0.9), que la connexion soit read-only ou write.
  Un retry avec backoff absorbe les collisions dans la fenêtre de quelques ms
  où deux opérations s'exécuteraient simultanément.
"""

from __future__ import annotations

import logging
import math
import time
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

logger = logging.getLogger("pipeline.db")

_SCHEMA_VERSION = 2   # bump when schema changes


def _default_db_path() -> Path:
    """Return the DuckDB path for the currently active environment."""
    try:
        import sys
        root = Path(__file__).resolve().parent.parent
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        from env_loader import db_path
        return db_path()
    except Exception:
        # Fallback if env_loader is unavailable (e.g. standalone import)
        return Path(__file__).resolve().parent.parent / "data" / "pipeline_dev.duckdb"


class PipelineDB:

    def __init__(self, db_path=None, read_only: bool = False):
        self.db_path  = Path(db_path) if db_path is not None else _default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Always run schema with a write connection: all DDL statements use
        # CREATE/ALTER … IF NOT EXISTS so they are fully idempotent.
        # This ensures migrations are applied even for read-only UI instances
        # on an existing DB (e.g. new columns added after initial creation).
        self._read_only = False
        self._run_schema()
        self._read_only = read_only

    # ── connection management ────────────────────────────────────────────
    # Always short-lived: open → execute → close.
    # Never kept open between calls → no persistent file lock.

    def _connect(self, retries: int = 5, base_delay: float = 0.2):
        """Open a short-lived DuckDB connection with retry + exponential backoff."""
        last_err = None
        for attempt in range(retries):
            try:
                return duckdb.connect(str(self.db_path), read_only=self._read_only)
            except Exception as e:
                last_err = e
                if attempt < retries - 1:
                    wait = base_delay * (2 ** attempt)
                    logger.debug("DuckDB connect retry %d/%d in %.1fs: %s",
                                 attempt + 1, retries, wait, e)
                    time.sleep(wait)
        raise last_err

    def _query(self, sql: str, params: list = None) -> pd.DataFrame:
        con = self._connect()
        try:
            return con.execute(sql, params).df() if params else con.execute(sql).df()
        finally:
            con.close()

    def _query_one(self, sql: str, params: list = None):
        con = self._connect()
        try:
            r = con.execute(sql, params) if params else con.execute(sql)
            return r.fetchone()
        finally:
            con.close()

    def _exec(self, sql: str, params: list = None) -> None:
        con = self._connect()
        try:
            con.execute(sql, params) if params else con.execute(sql)
        finally:
            con.close()

    def _executemany(self, sql: str, rows: list) -> None:
        if not rows:
            return
        con = self._connect()
        try:
            con.executemany(sql, rows)
        finally:
            con.close()

    def _safe_bool(self, val):
        """Convert val to Python bool or None, handling pd.NA / float NaN safely."""
        if val is None:
            return None
        try:
            # pd.isna handles pd.NA, float NaN, None, and pd.NaT in one call
            import pandas as _pd
            if _pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        try:
            return bool(val)
        except (TypeError, ValueError):
            return None

    def _safe(self, val) -> Optional[str]:
        if val is None:
            return None
        try:
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                return None
        except Exception:
            pass
        s = str(val).strip()
        return s if s and s.lower() not in ("nan", "none", "nat", "") else None

    # ── schema ──────────────────────────────────────────────────────────

    def _run_schema(self) -> None:
        """Create tables and indexes in a single connection (no lock contention)."""
        con = self._connect()
        try:
            for stmt in [
                """CREATE TABLE IF NOT EXISTS runs (
                    run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP NOT NULL,
                    ended_at TIMESTAMP, window_start DATE, window_end DATE,
                    sources VARCHAR, dry_run BOOLEAN DEFAULT FALSE,
                    status VARCHAR DEFAULT 'running')""",
                """CREATE TABLE IF NOT EXISTS source_stats (
                    run_id VARCHAR NOT NULL, source VARCHAR NOT NULL,
                    harvested INTEGER DEFAULT 0, deduplicated INTEGER DEFAULT 0,
                    loaded INTEGER DEFAULT 0, rejected INTEGER DEFAULT 0,
                    PRIMARY KEY (run_id, source))""",
                """CREATE TABLE IF NOT EXISTS publications (
                    run_id VARCHAR NOT NULL, row_id VARCHAR, doi VARCHAR,
                    title VARCHAR, source VARCHAR, dc_type VARCHAR,
                    collection VARCHAR, status VARCHAR,
                    workspace_id VARCHAR, workflow_id VARCHAR,
                    error_msg VARCHAR, loaded_at TIMESTAMP DEFAULT NOW())""",
                """CREATE TABLE IF NOT EXISTS epfl_authors (
                    sciper VARCHAR PRIMARY KEY, full_name VARCHAR,
                    first_name VARCHAR, last_name VARCHAR,
                    orcid VARCHAR, epfl_orcid VARCHAR,
                    scopus_id VARCHAR, wos_id VARCHAR, openalex_id VARCHAR,
                    epfl_status VARCHAR, epfl_position VARCHAR,
                    main_unit VARCHAR, dspace_uuid VARCHAR,
                    last_seen TIMESTAMP DEFAULT NOW())""",
                """CREATE TABLE IF NOT EXISTS units (
                    acronym VARCHAR PRIMARY KEY, name_fr VARCHAR, name_en VARCHAR,
                    unit_type VARCHAR, epfl_unit_id VARCHAR, dspace_uuid VARCHAR,
                    last_seen TIMESTAMP DEFAULT NOW())""",
                """CREATE TABLE IF NOT EXISTS pub_authors (
                    run_id VARCHAR NOT NULL, row_id VARCHAR NOT NULL,
                    sciper VARCHAR NOT NULL, role VARCHAR,
                    PRIMARY KEY (run_id, row_id, sciper))""",
                """CREATE TABLE IF NOT EXISTS pub_units (
                    run_id VARCHAR NOT NULL, row_id VARCHAR NOT NULL,
                    acronym VARCHAR NOT NULL,
                    PRIMARY KEY (run_id, row_id, acronym))""",
                """CREATE TABLE IF NOT EXISTS run_logs (
                    log_id INTEGER PRIMARY KEY, run_id VARCHAR NOT NULL,
                    ts TIMESTAMP DEFAULT NOW(), level VARCHAR, message VARCHAR)""",
            ]:
                con.execute(stmt)

            for idx in [
                "CREATE INDEX IF NOT EXISTS idx_pubs_run    ON publications(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_pubs_status ON publications(status)",
                "CREATE INDEX IF NOT EXISTS idx_pubs_source ON publications(source)",
                "CREATE INDEX IF NOT EXISTS idx_pubs_type   ON publications(dc_type)",
                "CREATE INDEX IF NOT EXISTS idx_pa_sciper   ON pub_authors(sciper)",
                "CREATE INDEX IF NOT EXISTS idx_pa_run      ON pub_authors(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_pu_acronym  ON pub_units(acronym)",
            ]:
                con.execute(idx)

            # Schema migrations — additive only, safe on existing DBs
            for migration in [
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS pub_year VARCHAR",
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS upw_is_oa BOOLEAN",
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS upw_valid_pdf BOOLEAN",
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS upw_oa_status VARCHAR",
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS upw_license VARCHAR",
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS journal_title VARCHAR",
                "ALTER TABLE publications ADD COLUMN IF NOT EXISTS internal_id VARCHAR",
                """CREATE TABLE IF NOT EXISTS pub_detected_authors (
                    run_id VARCHAR NOT NULL, row_id VARCHAR NOT NULL,
                    author_name VARCHAR NOT NULL,
                    PRIMARY KEY (run_id, row_id, author_name))""",
            ]:
                con.execute(migration)
        finally:
            con.close()

    # ── run lifecycle ────────────────────────────────────────────────────

    def start_run(self, run_id, window_start, window_end, sources, dry_run=False):
        self._exec(
            "INSERT INTO runs (run_id,started_at,window_start,window_end,sources,dry_run,status)"
            " VALUES (?,NOW(),?,?,?,?,'running')",
            [run_id, window_start, window_end, ",".join(sources), dry_run])

    def finish_run(self, run_id, status="completed"):
        self._exec("UPDATE runs SET ended_at=NOW(), status=? WHERE run_id=?",
                   [status, run_id])

    # ── source stats ─────────────────────────────────────────────────────

    def record_source_stats(self, run_id, source, harvested=0,
                            deduplicated=0, loaded=0, rejected=0):
        self._exec(
            "INSERT INTO source_stats (run_id,source,harvested,deduplicated,loaded,rejected)"
            " VALUES (?,?,?,?,?,?) ON CONFLICT (run_id,source) DO UPDATE SET"
            " harvested=excluded.harvested, deduplicated=excluded.deduplicated,"
            " loaded=excluded.loaded, rejected=excluded.rejected",
            [run_id, source, harvested, deduplicated, loaded, rejected])

    # ── publications ─────────────────────────────────────────────────────

    def record_publications(self, run_id, df_imported, df_rejected,
                            df_deduplicated=None):
        s = self._safe
        sb = self._safe_bool
        rows = []
        for _, row in df_imported.iterrows():
            wf = s(row.get("workflow_id"))
            ws = s(row.get("workspace_id"))
            rows.append((run_id, s(row.get("row_id")), s(row.get("doi")),
                         s(row.get("title")), s(row.get("source")),
                         s(row.get("dc.type")), s(row.get("ifs3_collection_id")),
                         "workflow" if wf else ("workspace" if ws else "workflow"),
                         ws, wf, None,
                         s(row.get("pubyear")), sb(row.get("upw_is_oa")),
                         sb(row.get("upw_valid_pdf")), s(row.get("upw_oa_status")),
                         s(row.get("upw_license")), s(row.get("journalTitle")),
                         s(row.get("internal_id"))))
        for _, row in df_rejected.iterrows():
            rows.append((run_id, s(row.get("row_id")), s(row.get("doi")),
                         s(row.get("title")), s(row.get("source")),
                         s(row.get("dc.type")), s(row.get("ifs3_collection_id")),
                         "rejected", None, None,
                         s(row.get("reject_reason", row.get("is_duplicate"))),
                         s(row.get("pubyear")), sb(row.get("upw_is_oa")),
                         sb(row.get("upw_valid_pdf")), s(row.get("upw_oa_status")),
                         s(row.get("upw_license")), None,
                         s(row.get("internal_id"))))
        if df_deduplicated is not None and not df_deduplicated.empty:
            for _, row in df_deduplicated.iterrows():
                rows.append((run_id, s(row.get("row_id")), s(row.get("doi")),
                             s(row.get("title")), s(row.get("source")),
                             s(row.get("dc.type")), s(row.get("ifs3_collection_id")),
                             "deduplicated", None, None, "Already exists in Infoscience",
                             s(row.get("pubyear")), sb(row.get("upw_is_oa")),
                             sb(row.get("upw_valid_pdf")), s(row.get("upw_oa_status")),
                             s(row.get("upw_license")), None,
                             s(row.get("internal_id"))))
        self._executemany(
            "INSERT INTO publications (run_id,row_id,doi,title,source,dc_type,collection,"
            "status,workspace_id,workflow_id,error_msg,"
            "pub_year,upw_is_oa,upw_valid_pdf,upw_oa_status,upw_license,journal_title,internal_id)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)

    # ── epfl authors ─────────────────────────────────────────────────────

    def record_epfl_authors(self, df: pd.DataFrame):
        if df is None or df.empty:
            return
        s = self._safe
        rows, seen = [], set()
        for _, row in df.iterrows():
            sciper = s(row.get("sciper_id"))
            if not sciper or sciper in seen:
                continue
            seen.add(sciper)
            src = s(row.get("source")) or ""
            iid = s(row.get("internal_author_id")) or ""
            rows.append((sciper,
                s(row.get("author")), s(row.get("nameparse_firstname")),
                s(row.get("nameparse_lastname")),
                s(row.get("orcid_id")), s(row.get("epfl_orcid")),
                iid if src == "scopus" else None,
                iid if src == "wos" else None,
                iid if src in ("openalex","openalex+crossref") else None,
                s(row.get("epfl_status")), s(row.get("epfl_position")),
                s(row.get("final_mainunit")), s(row.get("dspace_uuid"))))
        self._executemany(
            "INSERT INTO epfl_authors"
            " (sciper,full_name,first_name,last_name,orcid,epfl_orcid,"
            " scopus_id,wos_id,openalex_id,epfl_status,epfl_position,"
            " main_unit,dspace_uuid,last_seen)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW())"
            " ON CONFLICT (sciper) DO UPDATE SET"
            " full_name=COALESCE(excluded.full_name,epfl_authors.full_name),"
            " first_name=COALESCE(excluded.first_name,epfl_authors.first_name),"
            " last_name=COALESCE(excluded.last_name,epfl_authors.last_name),"
            " orcid=COALESCE(excluded.orcid,epfl_authors.orcid),"
            " epfl_orcid=COALESCE(excluded.epfl_orcid,epfl_authors.epfl_orcid),"
            " scopus_id=COALESCE(excluded.scopus_id,epfl_authors.scopus_id),"
            " wos_id=COALESCE(excluded.wos_id,epfl_authors.wos_id),"
            " openalex_id=COALESCE(excluded.openalex_id,epfl_authors.openalex_id),"
            " epfl_status=COALESCE(excluded.epfl_status,epfl_authors.epfl_status),"
            " epfl_position=COALESCE(excluded.epfl_position,epfl_authors.epfl_position),"
            " main_unit=COALESCE(excluded.main_unit,epfl_authors.main_unit),"
            " dspace_uuid=COALESCE(excluded.dspace_uuid,epfl_authors.dspace_uuid),"
            " last_seen=NOW()", rows)
        logger.info("DB: %d EPFL authors upserted", len(rows))

    # ── units ────────────────────────────────────────────────────────────

    def record_units(self, df: pd.DataFrame):
        if df is None or df.empty:
            return
        s = self._safe
        seen, rows = set(), []
        for _, row in df.iterrows():
            acro = s(row.get("final_mainunit"))
            if not acro or acro in seen:
                continue
            seen.add(acro)
            rows.append((acro, s(row.get("epfl_api_mainunit_name")), None,
                         s(row.get("epfl_api_mainunit_type")),
                         s(row.get("epfl_api_mainunit_id")), None))
        self._executemany(
            "INSERT INTO units (acronym,name_fr,name_en,unit_type,epfl_unit_id,dspace_uuid,last_seen)"
            " VALUES (?,?,?,?,?,?,NOW())"
            " ON CONFLICT (acronym) DO UPDATE SET"
            " name_fr=COALESCE(excluded.name_fr,units.name_fr),"
            " unit_type=COALESCE(excluded.unit_type,units.unit_type),"
            " epfl_unit_id=COALESCE(excluded.epfl_unit_id,units.epfl_unit_id),"
            " last_seen=NOW()", rows)
        logger.info("DB: %d units upserted", len(seen))

    # ── pub links ────────────────────────────────────────────────────────

    def record_pub_author_links(self, run_id, df):
        if df is None or df.empty:
            return
        s = self._safe
        rows = [(run_id, s(r.get("row_id")), s(r.get("sciper_id")), s(r.get("role")))
                for _, r in df.iterrows()
                if s(r.get("sciper_id")) and s(r.get("row_id"))]
        self._executemany(
            "INSERT OR IGNORE INTO pub_authors (run_id,row_id,sciper,role) VALUES (?,?,?,?)",
            rows)

    def record_pub_unit_links(self, run_id, df):
        if df is None or df.empty:
            return
        s = self._safe
        seen, rows = set(), []
        for _, row in df.iterrows():
            key = (run_id, s(row.get("row_id")), s(row.get("final_mainunit")))
            if None in key or key in seen:
                continue
            seen.add(key); rows.append(key)
        self._executemany(
            "INSERT OR IGNORE INTO pub_units (run_id,row_id,acronym) VALUES (?,?,?)", rows)

    def record_detected_authors(self, run_id: str, df: pd.DataFrame) -> None:
        """Store EPFL-detected but unreconciled author names per publication.

        Only stores rows where sciper_id is absent — i.e. authors whose EPFL
        affiliation was detected but who could not be matched to a SCIPER.
        This corresponds to the "Detected EPFL Authors" − "Matched EPFL Authors"
        difference shown in the Excel report, and is what the UI displays for
        rejected publications.
        """
        if df is None or df.empty:
            return
        s = self._safe
        rows = [
            (run_id, s(r.get("row_id")), s(r.get("author")))
            for _, r in df.iterrows()
            if s(r.get("row_id")) and s(r.get("author")) and not s(r.get("sciper_id"))
        ]
        self._executemany(
            "INSERT INTO pub_detected_authors (run_id, row_id, author_name)"
            " VALUES (?,?,?) ON CONFLICT DO NOTHING",
            rows,
        )

    def add_log(self, run_id, level, message):
        self._exec("INSERT INTO run_logs (run_id,level,message) VALUES (?,?,?)",
                   [run_id, level, message])

    # ── read — dashboard ─────────────────────────────────────────────────

    def get_runs(self, limit=50) -> pd.DataFrame:
        return self._query(
            "SELECT run_id, started_at, ended_at,"
            " CASE WHEN ended_at IS NOT NULL"
            "      THEN CAST(epoch(ended_at) - epoch(started_at) AS INTEGER)"
            "      ELSE NULL END AS duration_s,"
            " window_start, window_end, sources, dry_run, status"
            " FROM runs ORDER BY started_at DESC LIMIT ?", [limit])

    def get_summary_stats(self) -> dict:
        r = self._query_one(
            "SELECT COUNT(DISTINCT run_id),"
            " SUM(CASE WHEN status IN ('workflow','workspace') THEN 1 ELSE 0 END),"
            " SUM(CASE WHEN status='deduplicated' THEN 1 ELSE 0 END),"
            " SUM(CASE WHEN status='rejected'     THEN 1 ELSE 0 END),"
            " (SELECT COUNT(*) FROM epfl_authors),"
            " (SELECT COUNT(*) FROM units)"
            " FROM publications")
        return {"total_runs": r[0] or 0, "total_imported": r[1] or 0,
                "total_deduped": r[2] or 0, "total_rejected": r[3] or 0,
                "total_authors": r[4] or 0, "total_units": r[5] or 0}

    def get_run_stats(self, run_id) -> pd.DataFrame:
        return self._query(
            "SELECT source,harvested,deduplicated,loaded,rejected"
            " FROM source_stats WHERE run_id=? ORDER BY source", [run_id])

    def get_trend(self, days=30) -> pd.DataFrame:
        return self._query(
            "SELECT CAST(loaded_at AS DATE) AS day, status, COUNT(*) AS count"
            " FROM publications"
            " WHERE loaded_at >= NOW() - INTERVAL (?) DAY"
            " AND status IN ('workflow','workspace','rejected','deduplicated')"
            " GROUP BY day, status ORDER BY day", [days])

    def get_sources_breakdown(self, run_id=None) -> pd.DataFrame:
        # source_stats only stores harvested per source; loaded/rejected are derived
        # from the publications table where status is accurate per source.
        if run_id:
            ss_sub = (
                "SELECT source, SUM(harvested) AS total_harvested"
                " FROM source_stats WHERE source != '__total__' AND run_id=?"
                " GROUP BY source"
            )
            p_cond = "AND p.run_id = ?"
            params = [run_id, run_id]
        else:
            ss_sub = (
                "SELECT source, SUM(harvested) AS total_harvested"
                " FROM source_stats WHERE source != '__total__'"
                " GROUP BY source"
            )
            p_cond = ""
            params = []
        return self._query(
            f"SELECT p.source,"
            f" COALESCE(MAX(ss.total_harvested), 0) AS harvested,"
            f" COUNT(CASE WHEN p.status IN ('workflow','workspace') THEN 1 END) AS loaded,"
            f" COUNT(CASE WHEN p.status = 'rejected'               THEN 1 END) AS rejected,"
            f" COUNT(CASE WHEN p.status = 'deduplicated'           THEN 1 END) AS deduplicated"
            f" FROM publications p"
            f" LEFT JOIN ({ss_sub}) ss ON ss.source = p.source"
            f" WHERE p.source IS NOT NULL {p_cond}"
            f" GROUP BY p.source"
            f" ORDER BY loaded DESC",
            params or None)

    def get_imported_by_month(self, months: int = 12) -> pd.DataFrame:
        """Monthly imported publication counts for the last N months."""
        return self._query(
            "SELECT DATE_TRUNC('month', r.started_at) AS month, COUNT(*) AS count"
            " FROM publications p"
            " INNER JOIN runs r ON r.run_id = p.run_id"
            " WHERE p.status IN ('workflow','workspace')"
            " AND r.started_at >= NOW() - INTERVAL (?) MONTH"
            " GROUP BY month ORDER BY month",
            [months])

    def get_pubs_by_source_and_type(self, run_id=None) -> pd.DataFrame:
        """Imported publications grouped by source and document type (for stacked bar)."""
        w, p = self._dash_where(run_id)   # filters to workflow/workspace
        return self._query(
            f"SELECT p.source, COALESCE(p.dc_type, 'Non défini') AS dc_type,"
            f" COUNT(*) AS count"
            f" FROM publications p {w}"
            f" AND p.source IS NOT NULL"
            f" GROUP BY p.source, p.dc_type"
            f" ORDER BY p.source, count DESC", p)

    # ── read — dashboard charts ──────────────────────────────────────────

    _IMPORTED = "p.status IN ('workflow','workspace')"

    def _dash_where(self, run_id):
        if run_id:
            return f"WHERE {self._IMPORTED} AND p.run_id = ?", [run_id]
        return f"WHERE {self._IMPORTED}", []

    def get_pubs_by_type(self, run_id=None) -> pd.DataFrame:
        w, p = self._dash_where(run_id)
        return self._query(
            f"SELECT p.dc_type AS type, COUNT(*) AS count"
            f" FROM publications p {w}"
            f" GROUP BY p.dc_type ORDER BY count DESC", p)

    def get_pubs_by_oa_status(self, run_id=None) -> pd.DataFrame:
        _non_open = "('elsevier-specific','publisher-specific-oa','implied-oa')"
        w, p = self._dash_where(run_id)
        return self._query(
            f"SELECT CASE"
            f"  WHEN p.upw_is_oa IS NULL                          THEN 'Non défini'"
            f"  WHEN p.upw_is_oa = FALSE                          THEN 'Non-OA'"
            f"  WHEN p.upw_license IN {_non_open}                 THEN 'OA non-libre'"
            f"  WHEN p.upw_valid_pdf = TRUE                       THEN 'OA + PDF'"
            f"  ELSE 'OA sans PDF'"
            f" END AS oa_category, COUNT(*) AS count"
            f" FROM publications p {w}"
            f" GROUP BY oa_category ORDER BY count DESC", p)

    def get_pubs_by_year(self, run_id=None) -> pd.DataFrame:
        w, p = self._dash_where(run_id)
        return self._query(
            f"SELECT p.pub_year AS year, COUNT(*) AS count"
            f" FROM publications p {w}"
            f" AND p.pub_year IS NOT NULL"
            f" GROUP BY p.pub_year ORDER BY p.pub_year", p)

    def get_pdf_stats(self, run_id=None) -> dict:
        w, p = self._dash_where(run_id)
        r = self._query_one(
            f"SELECT COUNT(*) AS total,"
            f" SUM(CASE WHEN p.upw_valid_pdf = TRUE THEN 1 ELSE 0 END) AS with_pdf,"
            f" SUM(CASE WHEN p.upw_is_oa = TRUE THEN 1 ELSE 0 END) AS oa,"
            f" SUM(CASE WHEN p.upw_is_oa = FALSE THEN 1 ELSE 0 END) AS closed"
            f" FROM publications p {w}", p or None)
        if not r:
            return {"total": 0, "with_pdf": 0, "oa": 0, "closed": 0}
        return {"total": r[0] or 0, "with_pdf": r[1] or 0,
                "oa": r[2] or 0, "closed": r[3] or 0}

    def get_pubs_by_unit(self, run_id=None, limit=15) -> pd.DataFrame:
        if run_id:
            w = "WHERE p.status IN ('workflow','workspace') AND p.run_id = ?"
            p = [run_id, limit]
        else:
            w = "WHERE p.status IN ('workflow','workspace')"
            p = [limit]
        return self._query(
            f"SELECT pu.acronym, COUNT(DISTINCT p.row_id) AS count"
            f" FROM publications p"
            f" INNER JOIN pub_units pu ON pu.run_id=p.run_id AND pu.row_id=p.row_id"
            f" {w} GROUP BY pu.acronym ORDER BY count DESC LIMIT ?", p)

    def get_pubs_by_journal(self, run_id=None, limit=15) -> pd.DataFrame:
        w, p = self._dash_where(run_id)
        p = p + [limit]
        return self._query(
            f"SELECT p.journal_title AS journal, COUNT(*) AS count"
            f" FROM publications p {w}"
            f" AND p.journal_title IS NOT NULL AND p.journal_title != ''"
            f" GROUP BY p.journal_title ORDER BY count DESC LIMIT ?", p)

    def get_top_epfl_authors(self, run_id=None, limit=20) -> pd.DataFrame:
        where = "WHERE pa.run_id = ?" if run_id else ""
        params = ([run_id] if run_id else []) + [limit]
        return self._query(
            f"SELECT ea.full_name, ea.sciper, ea.main_unit,"
            f" COUNT(DISTINCT pa.run_id || '::' || pa.row_id) AS pub_count"
            f" FROM pub_authors pa"
            f" JOIN epfl_authors ea ON ea.sciper = pa.sciper"
            f" {where}"
            f" GROUP BY ea.full_name, ea.sciper, ea.main_unit"
            f" ORDER BY pub_count DESC LIMIT ?",
            params or None,
        )

    # ── read — publications ──────────────────────────────────────────────

    _NON_OPEN_LICENSES = frozenset({
        "elsevier-specific", "publisher-specific-oa", "implied-oa"
    })

    # Weak-status SQL fragments — hardcoded, never user-supplied.
    _WEAK_ST_SQL  = "','".join(["hôte", "hors epfl", "étudiant"])
    _WEAK_POS_SQL = "','".join([
        "academic guest", "consultant", "engineer", "external employee",
        "external student", "guest", "guest phd student", "lecturer",
        "postdoctoral researcher", "visiting professor",
    ])

    @staticmethod
    def _as_filter_list(v):
        """Normalize a filter value to a non-empty list, or return [] (= no filter)."""
        if not v:
            return []
        return list(v) if isinstance(v, (list, tuple)) else [v]

    @staticmethod
    def _in_clause(col: str, values: list) -> tuple:
        """Return (sql_fragment, params) for a single- or multi-value equality filter."""
        if len(values) == 1:
            return f"{col} = ?", [values[0]]
        ph = ",".join(["?"] * len(values))
        return f"{col} IN ({ph})", values

    def _pub_filters(self, run_id, status, source, dc_type, sciper, unit_acronym,
                     search, has_pdf=None, oa_filter=None, licence=None,
                     epfl_strength=None):
        """Shared filter-building logic for get_publications and count_publications.

        run_id, status, source, dc_type, unit_acronym, licence each accept either a
        single value (str) or a list of values — an empty list means no filter.
        """
        filters, params = [], []
        join_a = join_u = ""
        if sciper:
            join_a = "INNER JOIN pub_authors pa ON pa.run_id=p.run_id AND pa.row_id=p.row_id"
            filters.append("pa.sciper = ?"); params.append(sciper)

        unit_list = self._as_filter_list(unit_acronym)
        if unit_list:
            join_u = "INNER JOIN pub_units pu ON pu.run_id=p.run_id AND pu.row_id=p.row_id"
            cond, vals = self._in_clause("pu.acronym", unit_list)
            filters.append(cond); params.extend(vals)

        for col, val in [
            ("p.run_id", run_id), ("p.status", status),
            ("p.source", source),  ("p.dc_type", dc_type),
        ]:
            val_list = self._as_filter_list(val)
            if val_list:
                cond, vals = self._in_clause(col, val_list)
                filters.append(cond); params.extend(vals)

        if search:
            filters.append("(LOWER(p.title) LIKE ? OR LOWER(p.doi) LIKE ?)")
            params += [f"%{search.lower()}%", f"%{search.lower()}%"]
        if has_pdf is True:
            filters.append("p.upw_valid_pdf = TRUE")
        elif has_pdf is False:
            filters.append("(p.upw_valid_pdf IS NULL OR p.upw_valid_pdf = FALSE)")
        _non_open_sql = "','".join(self._NON_OPEN_LICENSES)
        if oa_filter == "OA":
            filters.append(
                f"p.upw_is_oa = TRUE"
                f" AND LOWER(COALESCE(p.upw_license,'')) NOT IN ('{_non_open_sql}')"
            )
        elif oa_filter == "Non-OA":
            filters.append("p.upw_is_oa = FALSE")
        elif oa_filter == "Non-libre":
            filters.append(
                f"p.upw_is_oa = TRUE"
                f" AND LOWER(COALESCE(p.upw_license,'')) IN ('{_non_open_sql}')"
            )
        elif oa_filter == "Non défini":
            filters.append("p.upw_is_oa IS NULL")

        licence_list = self._as_filter_list(licence)
        if licence_list:
            lowers = [l.lower() for l in licence_list]
            cond, vals = self._in_clause("LOWER(COALESCE(p.upw_license,''))", lowers)
            filters.append(cond); params.extend(vals)

        if epfl_strength in ("weak", "strong"):
            # SQL fragment that identifies a "strong" EPFL author (not weak).
            _strong = (
                f" LOWER(COALESCE(ea.epfl_status,'')) != ''"
                f" AND LOWER(COALESCE(ea.epfl_status,'')) NOT IN ('{self._WEAK_ST_SQL}')"
                f" AND NOT ("
                f"  LOWER(COALESCE(ea.epfl_status,'')) = 'personnel'"
                f"  AND (LOWER(COALESCE(ea.epfl_position,'')) = ''"
                f"       OR LOWER(COALESCE(ea.epfl_position,'')) IN ('{self._WEAK_POS_SQL}'))"
                f" )"
            )
            _has_author = (
                "EXISTS (SELECT 1 FROM pub_authors pa_w"
                " WHERE pa_w.run_id=p.run_id AND pa_w.row_id=p.row_id)"
            )
            _has_strong = (
                "EXISTS (SELECT 1 FROM pub_authors pa_w"
                " INNER JOIN epfl_authors ea ON ea.sciper=pa_w.sciper"
                f" WHERE pa_w.run_id=p.run_id AND pa_w.row_id=p.row_id AND {_strong})"
            )
            if epfl_strength == "weak":
                filters.append(f"{_has_author} AND NOT {_has_strong}")
            else:  # strong
                filters.append(_has_strong)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        return join_a, join_u, where, params

    def count_publications(self, run_id=None, status=None, source=None,
                           dc_type=None, sciper=None, unit_acronym=None,
                           search=None, has_pdf=None, oa_filter=None,
                           licence=None, epfl_strength=None) -> int:
        join_a, join_u, where, params = self._pub_filters(
            run_id, status, source, dc_type, sciper, unit_acronym, search,
            has_pdf=has_pdf, oa_filter=oa_filter, licence=licence,
            epfl_strength=epfl_strength)
        # Use a subquery with the same DISTINCT columns as get_publications so
        # the count matches the actual number of rows the paginated query returns.
        # COUNT(DISTINCT row_id) under-counts when the same row_id appears with
        # different status/field values (e.g. workflow + deduplicated in same run).
        r = self._query_one(
            f"SELECT COUNT(*) FROM ("
            f"SELECT DISTINCT p.run_id,p.row_id,p.doi,p.title,p.source,p.dc_type,"
            f"p.status,p.workspace_id,p.workflow_id,p.error_msg,p.loaded_at,"
            f"p.pub_year,p.upw_is_oa,p.upw_valid_pdf,p.upw_oa_status,p.upw_license,"
            f"p.internal_id"
            f" FROM publications p {join_a} {join_u} {where}"
            f") _c",
            params or None)
        return int(r[0]) if r and r[0] else 0

    def get_publications(self, run_id=None, status=None, source=None,
                         dc_type=None, sciper=None, unit_acronym=None,
                         search=None, has_pdf=None, oa_filter=None,
                         licence=None, epfl_strength=None,
                         limit=100, offset=0) -> pd.DataFrame:
        join_a, join_u, where, params = self._pub_filters(
            run_id, status, source, dc_type, sciper, unit_acronym, search,
            has_pdf=has_pdf, oa_filter=oa_filter, licence=licence,
            epfl_strength=epfl_strength)
        params += [limit, offset]
        return self._query(
            f"SELECT DISTINCT p.run_id,p.row_id,p.doi,p.title,p.source,p.dc_type,"
            f" p.status,p.workspace_id,p.workflow_id,p.error_msg,p.loaded_at,"
            f" p.pub_year,p.upw_is_oa,p.upw_valid_pdf,p.upw_oa_status,p.upw_license,"
            f" p.internal_id"
            f" FROM publications p {join_a} {join_u} {where}"
            f" ORDER BY p.loaded_at DESC LIMIT ? OFFSET ?", params)

    # ── read — authors & units ───────────────────────────────────────────

    def get_epfl_authors(self, sciper=None, name_search=None,
                         unit=None, limit=200) -> pd.DataFrame:
        filters, params = [], []
        if sciper:      filters.append("sciper = ?");              params.append(sciper)
        if name_search: filters.append("LOWER(full_name) LIKE ?"); params.append(f"%{name_search.lower()}%")
        if unit:        filters.append("main_unit = ?");            params.append(unit)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        params.append(limit)
        return self._query(
            f"SELECT sciper,full_name,first_name,last_name,orcid,epfl_orcid,"
            f" scopus_id,wos_id,openalex_id,epfl_status,epfl_position,"
            f" main_unit,dspace_uuid,last_seen"
            f" FROM epfl_authors {where} ORDER BY last_name,first_name LIMIT ?", params)

    def get_units(self, unit_type=None) -> pd.DataFrame:
        w = "WHERE u.unit_type=?" if unit_type else ""
        p = [unit_type] if unit_type else []
        return self._query(
            f"SELECT u.acronym,u.name_fr,u.unit_type,u.epfl_unit_id,"
            f" COUNT(DISTINCT pa.sciper) AS author_count,"
            f" COUNT(DISTINCT pu.row_id) AS pub_count"
            f" FROM units u"
            f" LEFT JOIN pub_units   pu ON pu.acronym=u.acronym"
            f" LEFT JOIN pub_authors pa ON pa.run_id=pu.run_id AND pa.row_id=pu.row_id"
            f" {w} GROUP BY u.acronym,u.name_fr,u.unit_type,u.epfl_unit_id"
            f" ORDER BY pub_count DESC", p)

    def get_pub_authors_for_run(self, run_id) -> pd.DataFrame:
        return self._query(
            "SELECT p.row_id,p.doi,p.title,p.source,p.dc_type,p.status,"
            " p.workspace_id,p.workflow_id,"
            " a.sciper,a.full_name,a.first_name,a.last_name,"
            " a.orcid,a.epfl_status,a.epfl_position,a.main_unit,"
            " a.dspace_uuid AS author_dspace_uuid, pa.role"
            " FROM publications p"
            " INNER JOIN pub_authors pa ON pa.run_id=p.run_id AND pa.row_id=p.row_id"
            " INNER JOIN epfl_authors a ON a.sciper=pa.sciper"
            " WHERE p.run_id=? ORDER BY p.row_id,a.last_name", [run_id])

    def get_detected_authors_for_run(self, run_id: str) -> pd.DataFrame:
        return self._query(
            "SELECT row_id, author_name FROM pub_detected_authors WHERE run_id=? ORDER BY row_id, author_name",
            [run_id],
        )

    def get_pub_units_for_run(self, run_id: str) -> pd.DataFrame:
        return self._query(
            "SELECT pu.row_id, pu.acronym, u.unit_type"
            " FROM pub_units pu"
            " LEFT JOIN units u ON u.acronym = pu.acronym"
            " WHERE pu.run_id = ? ORDER BY pu.row_id, pu.acronym",
            [run_id],
        )

    def _pairs_cte(self, run_row_pairs: list) -> tuple:
        """Return (cte_sql, params) for a VALUES-based CTE over (run_id, row_id) pairs."""
        rows_sql = " UNION ALL ".join(["SELECT ? AS run_id, ? AS row_id"] * len(run_row_pairs))
        params = [v for pair in run_row_pairs for v in pair]
        return f"WITH _pairs AS ({rows_sql})", params

    def get_pub_authors_for_rows(self, run_row_pairs: list) -> pd.DataFrame:
        """Fetch author enrichment for an explicit list of (run_id, row_id) pairs."""
        if not run_row_pairs:
            return pd.DataFrame()
        cte, params = self._pairs_cte(run_row_pairs)
        return self._query(
            f"{cte}"
            " SELECT p.run_id, p.row_id, p.doi, p.title, p.source, p.dc_type, p.status,"
            " p.workspace_id, p.workflow_id,"
            " a.sciper, a.full_name, a.first_name, a.last_name,"
            " a.orcid, a.epfl_status, a.epfl_position, a.main_unit,"
            " a.dspace_uuid AS author_dspace_uuid, pa.role"
            " FROM publications p"
            " INNER JOIN _pairs       ON _pairs.run_id = p.run_id AND _pairs.row_id = p.row_id"
            " INNER JOIN pub_authors pa ON pa.run_id = p.run_id AND pa.row_id = p.row_id"
            " INNER JOIN epfl_authors a ON a.sciper = pa.sciper"
            " ORDER BY p.run_id, p.row_id, a.last_name",
            params,
        )

    def get_pub_units_for_rows(self, run_row_pairs: list) -> pd.DataFrame:
        """Fetch unit enrichment for an explicit list of (run_id, row_id) pairs."""
        if not run_row_pairs:
            return pd.DataFrame()
        cte, params = self._pairs_cte(run_row_pairs)
        return self._query(
            f"{cte}"
            " SELECT pu.run_id, pu.row_id, pu.acronym, u.unit_type"
            " FROM pub_units pu"
            " INNER JOIN _pairs ON _pairs.run_id = pu.run_id AND _pairs.row_id = pu.row_id"
            " LEFT JOIN units u ON u.acronym = pu.acronym"
            " ORDER BY pu.run_id, pu.row_id, pu.acronym",
            params,
        )

    def get_detected_authors_for_rows(self, run_row_pairs: list) -> pd.DataFrame:
        """Fetch detected-author names for an explicit list of (run_id, row_id) pairs."""
        if not run_row_pairs:
            return pd.DataFrame()
        cte, params = self._pairs_cte(run_row_pairs)
        return self._query(
            f"{cte}"
            " SELECT pda.run_id, pda.row_id, pda.author_name"
            " FROM pub_detected_authors pda"
            " INNER JOIN _pairs ON _pairs.run_id = pda.run_id AND _pairs.row_id = pda.row_id"
            " ORDER BY pda.run_id, pda.row_id, pda.author_name",
            params,
        )

    def get_distinct_dc_types(self) -> list:
        r = self._query("SELECT DISTINCT dc_type FROM publications WHERE dc_type IS NOT NULL ORDER BY dc_type")
        return r["dc_type"].tolist() if not r.empty else []

    def get_distinct_units(self) -> list:
        r = self._query("SELECT DISTINCT acronym FROM pub_units ORDER BY acronym")
        return r["acronym"].tolist() if not r.empty else []

    def get_distinct_sources(self) -> list:
        r = self._query("SELECT DISTINCT source FROM publications WHERE source IS NOT NULL ORDER BY source")
        return r["source"].tolist() if not r.empty else []

    def get_distinct_licences(self) -> list:
        r = self._query(
            "SELECT DISTINCT LOWER(upw_license) AS lic FROM publications"
            " WHERE upw_license IS NOT NULL AND upw_license != '' ORDER BY lic"
        )
        return r["lic"].tolist() if not r.empty else []

    def get_run_logs(self, run_id, limit=200) -> pd.DataFrame:
        return self._query(
            "SELECT ts,level,message FROM run_logs"
            " WHERE run_id=? ORDER BY ts DESC LIMIT ?", [run_id, limit])

    # ── read — dashboard aggregates ──────────────────────────────────────

    def get_dashboard_kpis(self, months: int = 12) -> dict:
        """KPI aggregates for the last N months: run counts, success rate, avg duration."""
        r = self._query_one(
            "SELECT COUNT(*) AS total_runs,"
            " SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,"
            " AVG(CASE WHEN ended_at IS NOT NULL THEN EPOCH(ended_at-started_at) END) AS avg_s"
            " FROM runs WHERE started_at >= NOW() - INTERVAL (?) MONTH",
            [months])
        total     = int(r[0] or 0)
        completed = int(r[1] or 0)
        avg_s     = float(r[2]) if r[2] is not None else None
        imp = self._query_one(
            "SELECT COUNT(*) FROM publications p"
            " INNER JOIN runs r ON r.run_id=p.run_id"
            " WHERE p.status IN ('workflow','workspace')"
            " AND r.started_at >= NOW() - INTERVAL (?) MONTH",
            [months])
        rej = self._query_one(
            "SELECT COUNT(*) FROM publications p"
            " INNER JOIN runs r ON r.run_id=p.run_id"
            " WHERE p.status = 'rejected'"
            " AND r.started_at >= NOW() - INTERVAL (?) MONTH",
            [months])
        return {
            "total_runs":     total,
            "completed":      completed,
            "success_rate":   round(100 * completed / total) if total else 0,
            "avg_duration_s": avg_s,
            "total_imported": int(imp[0] or 0) if imp else 0,
            "total_rejected": int(rej[0] or 0) if rej else 0,
        }

    def get_pubs_status_per_run(self, limit: int = 20) -> pd.DataFrame:
        """Per-run publication status counts for the most recent N runs."""
        return self._query(
            "SELECT p.run_id, p.status, COUNT(*) AS count"
            " FROM publications p"
            " WHERE p.run_id IN ("
            "   SELECT run_id FROM runs ORDER BY started_at DESC LIMIT ?"
            " ) GROUP BY p.run_id, p.status",
            [limit])

    def get_pubs_by_status(self, run_id=None) -> pd.DataFrame:
        """Publication counts grouped by status (works for both a specific run and all runs)."""
        w = "WHERE run_id=?" if run_id else ""
        p = [run_id] if run_id else []
        return self._query(
            f"SELECT status, COUNT(*) AS count FROM publications {w}"
            f" GROUP BY status ORDER BY count DESC", p)

    # ── close (no-op: no persistent connection) ──────────────────────────

    def close(self) -> None:
        """No-op: connections are closed immediately after each operation."""
        pass

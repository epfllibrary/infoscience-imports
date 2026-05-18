"""DuckDB persistence layer — Infoscience import pipeline v2.

Connection strategy:
  All connections are short-lived (open → execute → close).
  No connection is ever kept open between two operations.
  This avoids the DuckDB write-lock conflict (one write connection at a time
  on macOS / DuckDB >= 0.9), whether the connection is read-only or write.
  A retry with backoff absorbs collisions within the few-millisecond window
  where two operations might execute simultaneously.
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

_SCHEMA_VERSION = 3   # bump when schema changes


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
                # Canonical publications table — one row per unique article.
                # pub_id = normalised DOI, or {source}:{internal_id} fallback.
                """CREATE TABLE IF NOT EXISTS publications (
                    pub_id VARCHAR PRIMARY KEY,
                    doi VARCHAR,
                    title VARCHAR,
                    source VARCHAR,
                    dc_type VARCHAR,
                    collection VARCHAR,
                    pub_year VARCHAR,
                    upw_is_oa BOOLEAN,
                    upw_valid_pdf BOOLEAN,
                    upw_oa_status VARCHAR,
                    upw_license VARCHAR,
                    journal_title VARCHAR,
                    internal_id VARCHAR,
                    first_seen_at TIMESTAMP DEFAULT NOW(),
                    last_seen_at TIMESTAMP DEFAULT NOW(),
                    seen_count INTEGER DEFAULT 1,
                    infoscience_dedup_count INTEGER DEFAULT 0)""",
                # Per-run publication records — one row per (run_id, pub_id).
                """CREATE TABLE IF NOT EXISTS run_publications (
                    run_id VARCHAR NOT NULL,
                    pub_id VARCHAR NOT NULL,
                    row_id VARCHAR,
                    status VARCHAR,
                    workspace_id VARCHAR,
                    workflow_id VARCHAR,
                    error_msg VARCHAR,
                    loaded_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (run_id, pub_id))""",
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
                """CREATE TABLE IF NOT EXISTS pub_detected_authors (
                    run_id VARCHAR NOT NULL, row_id VARCHAR NOT NULL,
                    author_name VARCHAR NOT NULL,
                    PRIMARY KEY (run_id, row_id, author_name))""",
            ]:
                con.execute(stmt)

            for idx in [
                "CREATE INDEX IF NOT EXISTS idx_pubs_run    ON run_publications(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_pubs_status ON run_publications(status)",
                "CREATE INDEX IF NOT EXISTS idx_pubs_source ON publications(source)",
                "CREATE INDEX IF NOT EXISTS idx_pubs_type   ON publications(dc_type)",
                "CREATE INDEX IF NOT EXISTS idx_rp_pubid    ON run_publications(pub_id)",
                "CREATE INDEX IF NOT EXISTS idx_pa_sciper   ON pub_authors(sciper)",
                "CREATE INDEX IF NOT EXISTS idx_pa_run      ON pub_authors(run_id)",
                "CREATE INDEX IF NOT EXISTS idx_pu_acronym  ON pub_units(acronym)",
            ]:
                con.execute(idx)

            # Run idempotent migration from v1 (per-run publications) to v2
            # (canonical publications + run_publications).
            self._migrate_publications_v2(con)
        finally:
            con.close()

    @staticmethod
    def _compute_pub_id(doi, source, internal_id) -> str | None:
        """Compute a stable publication identifier.

        Prefers normalised DOI; falls back to {source}:{internal_id}.
        Returns None if neither is available.
        """
        if doi:
            return doi.lower().strip()
        if internal_id and source:
            return f"{source}:{internal_id}"
        return None

    def _migrate_publications_v2(self, con) -> None:
        """Migrate from per-run publications (v1) to normalised schema (v2).

        Migration is triggered when the publications table has a run_id column
        but no pub_id column — the definitive v1 indicator.

        Safe to call multiple times — it is a no-op if already migrated.
        """
        pub_has_run_id = con.execute(
            "SELECT COUNT(*) FROM information_schema.columns"
            " WHERE table_name = 'publications' AND column_name = 'run_id'"
        ).fetchone()[0]
        pub_has_pub_id = con.execute(
            "SELECT COUNT(*) FROM information_schema.columns"
            " WHERE table_name = 'publications' AND column_name = 'pub_id'"
        ).fetchone()[0]

        if not pub_has_run_id or pub_has_pub_id:
            # Either no v1 schema, or already migrated to v2
            return

        logger.info("DB: migrating publications schema from v1 to v2 …")

        # Step 1 — drop old indexes so the rename can proceed, then rename
        for old_idx in [
            "idx_pubs_run", "idx_pubs_status", "idx_pubs_source", "idx_pubs_type",
        ]:
            try:
                con.execute(f"DROP INDEX IF EXISTS {old_idx}")
            except Exception:
                pass
        con.execute("ALTER TABLE publications RENAME TO _pub_v1_legacy")

        # Step 2 — create run_publications (new schema)
        con.execute("""
            CREATE TABLE IF NOT EXISTS run_publications (
                run_id VARCHAR NOT NULL,
                pub_id VARCHAR NOT NULL,
                row_id VARCHAR,
                status VARCHAR,
                workspace_id VARCHAR,
                workflow_id VARCHAR,
                error_msg VARCHAR,
                loaded_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (run_id, pub_id))
        """)

        # Step 3 — populate run_publications from legacy data
        con.execute("""
            INSERT INTO run_publications
                (run_id, pub_id, row_id, status, workspace_id, workflow_id,
                 error_msg, loaded_at)
            SELECT run_id,
                COALESCE(
                    LOWER(TRIM(doi)),
                    source || ':' || COALESCE(
                        TRIM(CAST(internal_id AS VARCHAR)),
                        CAST(row_id AS VARCHAR)
                    )
                ) AS pub_id,
                CAST(row_id AS VARCHAR),
                status, workspace_id, workflow_id, error_msg, loaded_at
            FROM _pub_v1_legacy
            WHERE COALESCE(
                LOWER(TRIM(doi)),
                source || ':' || COALESCE(
                    TRIM(CAST(internal_id AS VARCHAR)),
                    CAST(row_id AS VARCHAR)
                )
            ) IS NOT NULL
        """)

        # Step 4 — create canonical publications table (new schema)
        con.execute("""
            CREATE TABLE IF NOT EXISTS publications (
                pub_id VARCHAR PRIMARY KEY,
                doi VARCHAR,
                title VARCHAR,
                source VARCHAR,
                dc_type VARCHAR,
                collection VARCHAR,
                pub_year VARCHAR,
                upw_is_oa BOOLEAN,
                upw_valid_pdf BOOLEAN,
                upw_oa_status VARCHAR,
                upw_license VARCHAR,
                journal_title VARCHAR,
                internal_id VARCHAR,
                first_seen_at TIMESTAMP DEFAULT NOW(),
                last_seen_at TIMESTAMP DEFAULT NOW(),
                seen_count INTEGER DEFAULT 1,
                infoscience_dedup_count INTEGER DEFAULT 0)
        """)

        # Step 5 — populate canonical publications using window functions to
        # pick the most recent metadata per pub_id and aggregate counts.
        con.execute("""
            WITH base AS (
                SELECT *,
                    COALESCE(
                        LOWER(TRIM(doi)),
                        source || ':' || COALESCE(
                            TRIM(CAST(internal_id AS VARCHAR)),
                            CAST(row_id AS VARCHAR)
                        )
                    ) AS pub_id
                FROM _pub_v1_legacy
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY pub_id
                        ORDER BY COALESCE(loaded_at, '1970-01-01') DESC
                    ) AS rn,
                    COUNT(*) OVER (PARTITION BY pub_id) AS seen_count_v,
                    COUNT(CASE WHEN status = 'deduplicated' THEN 1 END)
                        OVER (PARTITION BY pub_id) AS dedup_count_v,
                    MIN(COALESCE(loaded_at, NOW()))
                        OVER (PARTITION BY pub_id) AS first_seen_v,
                    MAX(COALESCE(loaded_at, NOW()))
                        OVER (PARTITION BY pub_id) AS last_seen_v
                FROM base
                WHERE pub_id IS NOT NULL
            )
            INSERT INTO publications (
                pub_id, doi, title, source, dc_type, collection, pub_year,
                upw_is_oa, upw_valid_pdf, upw_oa_status, upw_license,
                journal_title, internal_id,
                first_seen_at, last_seen_at, seen_count, infoscience_dedup_count)
            SELECT pub_id, doi, title, source, dc_type, collection, pub_year,
                upw_is_oa, upw_valid_pdf, upw_oa_status, upw_license,
                journal_title, internal_id,
                first_seen_v, last_seen_v, seen_count_v, dedup_count_v
            FROM ranked
            WHERE rn = 1
        """)

        # Step 6 — recreate indexes on new tables
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_pubs_run ON run_publications(run_id)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_pubs_status ON run_publications(status)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_rp_pubid ON run_publications(pub_id)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_pubs_source ON publications(source)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_pubs_type ON publications(dc_type)"
        )

        # Step 7 — drop legacy table
        con.execute("DROP TABLE _pub_v1_legacy")

        logger.info("DB: publications v2 migration completed")

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

        # Build canonical publication rows and run_publication link rows in one pass.
        pub_rows = []    # for canonical publications upsert
        rp_rows = []     # for run_publications insert

        def _process(df, status_override, error_override=None):
            for _, row in df.iterrows():
                doi = s(row.get("doi"))
                source = s(row.get("source"))
                internal_id = s(row.get("internal_id"))
                pub_id = self._compute_pub_id(doi, source, internal_id)
                if not pub_id:
                    continue

                wf = s(row.get("workflow_id"))
                ws = s(row.get("workspace_id"))
                if status_override:
                    status = status_override
                else:
                    status = "workflow" if wf else ("workspace" if ws else "workflow")

                is_dedup = 1 if status == "deduplicated" else 0
                error = error_override or s(row.get("reject_reason", row.get("is_duplicate")))

                pub_rows.append((
                    pub_id, doi, s(row.get("title")), source,
                    s(row.get("dc.type")), s(row.get("ifs3_collection_id")),
                    s(row.get("pubyear")),
                    sb(row.get("upw_is_oa")), sb(row.get("upw_valid_pdf")),
                    s(row.get("upw_oa_status")), s(row.get("upw_license")),
                    s(row.get("journalTitle")), internal_id,
                    is_dedup,
                ))
                rp_rows.append((
                    run_id, pub_id, s(row.get("row_id")),
                    status, ws, wf, error,
                ))

        _process(df_imported, status_override=None)
        _process(df_rejected, status_override="rejected")
        if df_deduplicated is not None and not df_deduplicated.empty:
            _process(df_deduplicated, status_override="deduplicated",
                     error_override="Already exists in Infoscience")

        # Upsert canonical publications — update counters and refresh OA metadata.
        self._executemany(
            "INSERT INTO publications"
            " (pub_id, doi, title, source, dc_type, collection, pub_year,"
            "  upw_is_oa, upw_valid_pdf, upw_oa_status, upw_license,"
            "  journal_title, internal_id,"
            "  first_seen_at, last_seen_at, seen_count, infoscience_dedup_count)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),NOW(),1,?)"
            " ON CONFLICT (pub_id) DO UPDATE SET"
            "  seen_count = seen_count + 1,"
            "  last_seen_at = NOW(),"
            "  infoscience_dedup_count ="
            "    infoscience_dedup_count + excluded.infoscience_dedup_count,"
            "  upw_is_oa     = COALESCE(excluded.upw_is_oa,     upw_is_oa),"
            "  upw_valid_pdf = COALESCE(excluded.upw_valid_pdf, upw_valid_pdf),"
            "  upw_oa_status = COALESCE(excluded.upw_oa_status, upw_oa_status),"
            "  upw_license   = COALESCE(excluded.upw_license,   upw_license)",
            pub_rows,
        )

        # Insert per-run records — ignore duplicates (same pub seen twice in one run).
        self._executemany(
            "INSERT INTO run_publications"
            " (run_id, pub_id, row_id, status, workspace_id, workflow_id, error_msg)"
            " VALUES (?,?,?,?,?,?,?)"
            " ON CONFLICT (run_id, pub_id) DO NOTHING",
            rp_rows,
        )

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
            "SELECT COUNT(DISTINCT rp.run_id),"
            " COUNT(DISTINCT CASE WHEN rp.status IN ('workflow','workspace')"
            "   THEN rp.pub_id END),"
            " COUNT(DISTINCT CASE WHEN rp.status = 'deduplicated'"
            "   THEN rp.pub_id END),"
            " COUNT(DISTINCT CASE WHEN rp.status = 'rejected'"
            "   THEN rp.pub_id END),"
            " (SELECT COUNT(*) FROM epfl_authors),"
            " (SELECT COUNT(*) FROM units)"
            " FROM run_publications rp")
        return {"total_runs": r[0] or 0, "total_imported": r[1] or 0,
                "total_deduped": r[2] or 0, "total_rejected": r[3] or 0,
                "total_authors": r[4] or 0, "total_units": r[5] or 0}

    def get_run_stats(self, run_id) -> pd.DataFrame:
        return self._query(
            "SELECT source,harvested,deduplicated,loaded,rejected"
            " FROM source_stats WHERE run_id=? ORDER BY source", [run_id])

    def get_trend(self, days=30) -> pd.DataFrame:
        return self._query(
            "SELECT CAST(rp.loaded_at AS DATE) AS day, rp.status,"
            " COUNT(DISTINCT rp.pub_id) AS count"
            " FROM run_publications rp"
            " WHERE rp.loaded_at >= NOW() - INTERVAL (?) DAY"
            " AND rp.status IN ('workflow','workspace','rejected','deduplicated')"
            " GROUP BY day, rp.status ORDER BY day", [days])

    def get_sources_breakdown(self, run_id=None) -> pd.DataFrame:
        # source_stats only stores harvested per source; loaded/rejected are derived
        # from run_publications where status is accurate per source.
        if run_id:
            ss_sub = (
                "SELECT source, SUM(harvested) AS total_harvested"
                " FROM source_stats WHERE source != '__total__' AND run_id=?"
                " GROUP BY source"
            )
            p_cond = "AND rp.run_id = ?"
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
            f" COUNT(DISTINCT CASE WHEN rp.status IN ('workflow','workspace')"
            f"   THEN rp.pub_id END) AS loaded,"
            f" COUNT(DISTINCT CASE WHEN rp.status = 'rejected'"
            f"   THEN rp.pub_id END) AS rejected,"
            f" COUNT(DISTINCT CASE WHEN rp.status = 'deduplicated'"
            f"   THEN rp.pub_id END) AS deduplicated"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id"
            f" LEFT JOIN ({ss_sub}) ss ON ss.source = p.source"
            f" WHERE p.source IS NOT NULL {p_cond}"
            f" GROUP BY p.source"
            f" ORDER BY loaded DESC",
            params or None)

    def get_imported_by_month(self, months: int = 12) -> pd.DataFrame:
        """Monthly imported publication counts for the last N months."""
        return self._query(
            "SELECT DATE_TRUNC('month', r.started_at) AS month,"
            " COUNT(DISTINCT rp.pub_id) AS count"
            " FROM run_publications rp"
            " INNER JOIN runs r ON r.run_id = rp.run_id"
            " WHERE rp.status IN ('workflow','workspace')"
            " AND r.started_at >= NOW() - INTERVAL (?) MONTH"
            " GROUP BY month ORDER BY month",
            [months])

    def get_pubs_by_source_and_type(self, run_id=None) -> pd.DataFrame:
        """Imported publications grouped by source and document type (for stacked bar)."""
        w, p = self._dash_where(run_id)
        return self._query(
            f"SELECT p.source, COALESCE(p.dc_type, 'Non défini') AS dc_type,"
            f" COUNT(*) AS count"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id {w}"
            f" AND p.source IS NOT NULL"
            f" GROUP BY p.source, p.dc_type"
            f" ORDER BY p.source, count DESC", p)

    # ── read — dashboard charts ──────────────────────────────────────────

    _IMPORTED = "rp.status IN ('workflow','workspace')"

    def _dash_where(self, run_id):
        if run_id:
            return f"WHERE {self._IMPORTED} AND rp.run_id = ?", [run_id]
        return f"WHERE {self._IMPORTED}", []

    def get_pubs_by_type(self, run_id=None) -> pd.DataFrame:
        w, p = self._dash_where(run_id)
        return self._query(
            f"SELECT p.dc_type AS type, COUNT(*) AS count"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id {w}"
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
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id {w}"
            f" GROUP BY oa_category ORDER BY count DESC", p)

    def get_pubs_by_year(self, run_id=None) -> pd.DataFrame:
        w, p = self._dash_where(run_id)
        return self._query(
            f"SELECT p.pub_year AS year, COUNT(*) AS count"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id {w}"
            f" AND p.pub_year IS NOT NULL"
            f" GROUP BY p.pub_year ORDER BY p.pub_year", p)

    def get_pdf_stats(self, run_id=None) -> dict:
        w, p = self._dash_where(run_id)
        r = self._query_one(
            f"SELECT COUNT(*) AS total,"
            f" SUM(CASE WHEN p.upw_valid_pdf = TRUE THEN 1 ELSE 0 END) AS with_pdf,"
            f" SUM(CASE WHEN p.upw_is_oa = TRUE THEN 1 ELSE 0 END) AS oa,"
            f" SUM(CASE WHEN p.upw_is_oa = FALSE THEN 1 ELSE 0 END) AS closed"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id {w}", p or None)
        if not r:
            return {"total": 0, "with_pdf": 0, "oa": 0, "closed": 0}
        return {"total": r[0] or 0, "with_pdf": r[1] or 0,
                "oa": r[2] or 0, "closed": r[3] or 0}

    def get_pubs_by_unit(self, run_id=None, limit=15) -> pd.DataFrame:
        if run_id:
            w = ("WHERE rp.status IN ('workflow','workspace') AND rp.run_id = ?")
            p = [run_id, limit]
        else:
            w = "WHERE rp.status IN ('workflow','workspace')"
            p = [limit]
        return self._query(
            f"SELECT pu.acronym, COUNT(DISTINCT rp.pub_id) AS count"
            f" FROM run_publications rp"
            f" INNER JOIN pub_units pu ON pu.run_id=rp.run_id AND pu.row_id=rp.row_id"
            f" {w} GROUP BY pu.acronym ORDER BY count DESC LIMIT ?", p)

    def get_pubs_by_journal(self, run_id=None, limit=15) -> pd.DataFrame:
        w, p = self._dash_where(run_id)
        p = p + [limit]
        return self._query(
            f"SELECT p.journal_title AS journal, COUNT(*) AS count"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id {w}"
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
            join_a = (
                "INNER JOIN pub_authors pa"
                " ON pa.run_id=rp.run_id AND pa.row_id=rp.row_id"
            )
            filters.append("pa.sciper = ?"); params.append(sciper)

        unit_list = self._as_filter_list(unit_acronym)
        if unit_list:
            join_u = (
                "INNER JOIN pub_units pu"
                " ON pu.run_id=rp.run_id AND pu.row_id=rp.row_id"
            )
            cond, vals = self._in_clause("pu.acronym", unit_list)
            filters.append(cond); params.extend(vals)

        # run_id and status live on run_publications; source and dc_type on publications.
        for col, val in [
            ("rp.run_id", run_id), ("rp.status", status),
            ("p.source",  source),  ("p.dc_type", dc_type),
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
                " WHERE pa_w.run_id=rp.run_id AND pa_w.row_id=rp.row_id)"
            )
            _has_strong = (
                "EXISTS (SELECT 1 FROM pub_authors pa_w"
                " INNER JOIN epfl_authors ea ON ea.sciper=pa_w.sciper"
                f" WHERE pa_w.run_id=rp.run_id AND pa_w.row_id=rp.row_id AND {_strong})"
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
        r = self._query_one(
            f"SELECT COUNT(*) FROM ("
            f"  SELECT DISTINCT rp.run_id, rp.pub_id, p.doi, p.title,"
            f"    p.source, p.dc_type, rp.status, rp.workspace_id,"
            f"    rp.workflow_id, rp.error_msg, rp.loaded_at,"
            f"    p.pub_year, p.upw_is_oa, p.upw_valid_pdf,"
            f"    p.upw_oa_status, p.upw_license, p.internal_id"
            f"  FROM run_publications rp"
            f"  JOIN publications p ON p.pub_id = rp.pub_id"
            f"  {join_a} {join_u} {where}"
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
            f"SELECT DISTINCT rp.run_id, rp.row_id, p.doi, p.title,"
            f" p.source, p.dc_type, rp.status, rp.workspace_id,"
            f" rp.workflow_id, rp.error_msg, rp.loaded_at,"
            f" p.pub_year, p.upw_is_oa, p.upw_valid_pdf,"
            f" p.upw_oa_status, p.upw_license, p.internal_id,"
            f" p.seen_count, p.infoscience_dedup_count"
            f" FROM run_publications rp"
            f" JOIN publications p ON p.pub_id = rp.pub_id"
            f" {join_a} {join_u} {where}"
            f" ORDER BY rp.loaded_at DESC LIMIT ? OFFSET ?", params)

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
            "SELECT rp.row_id, p.doi, p.title, p.source, p.dc_type, rp.status,"
            " rp.workspace_id, rp.workflow_id,"
            " a.sciper, a.full_name, a.first_name, a.last_name,"
            " a.orcid, a.epfl_status, a.epfl_position, a.main_unit,"
            " a.dspace_uuid AS author_dspace_uuid, pa.role"
            " FROM run_publications rp"
            " JOIN publications p ON p.pub_id = rp.pub_id"
            " INNER JOIN pub_authors pa ON pa.run_id=rp.run_id AND pa.row_id=rp.row_id"
            " INNER JOIN epfl_authors a ON a.sciper=pa.sciper"
            " WHERE rp.run_id=? ORDER BY rp.row_id, a.last_name", [run_id])

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
            " SELECT rp.run_id, rp.row_id, p.doi, p.title, p.source, p.dc_type,"
            " rp.status, rp.workspace_id, rp.workflow_id,"
            " a.sciper, a.full_name, a.first_name, a.last_name,"
            " a.orcid, a.epfl_status, a.epfl_position, a.main_unit,"
            " a.dspace_uuid AS author_dspace_uuid, pa.role"
            " FROM run_publications rp"
            " JOIN publications p ON p.pub_id = rp.pub_id"
            " INNER JOIN _pairs       ON _pairs.run_id = rp.run_id"
            "                       AND _pairs.row_id  = rp.row_id"
            " INNER JOIN pub_authors pa ON pa.run_id = rp.run_id"
            "                          AND pa.row_id = rp.row_id"
            " INNER JOIN epfl_authors a ON a.sciper = pa.sciper"
            " ORDER BY rp.run_id, rp.row_id, a.last_name",
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
            "SELECT COUNT(*) FROM run_publications rp"
            " INNER JOIN runs r ON r.run_id = rp.run_id"
            " WHERE rp.status IN ('workflow','workspace')"
            " AND r.started_at >= NOW() - INTERVAL (?) MONTH",
            [months])
        rej = self._query_one(
            "SELECT COUNT(*) FROM run_publications rp"
            " INNER JOIN runs r ON r.run_id = rp.run_id"
            " WHERE rp.status = 'rejected'"
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
            "SELECT rp.run_id, rp.status, COUNT(*) AS count"
            " FROM run_publications rp"
            " WHERE rp.run_id IN ("
            "   SELECT run_id FROM runs ORDER BY started_at DESC LIMIT ?"
            " ) GROUP BY rp.run_id, rp.status",
            [limit])

    def get_pubs_by_status(self, run_id=None) -> pd.DataFrame:
        """Publication counts grouped by status (works for both a specific run and all runs)."""
        w = "WHERE rp.run_id=?" if run_id else ""
        p = [run_id] if run_id else []
        return self._query(
            f"SELECT rp.status AS status, COUNT(*) AS count"
            f" FROM run_publications rp {w}"
            f" GROUP BY rp.status ORDER BY count DESC", p)

    # ── close (no-op: no persistent connection) ──────────────────────────

    def close(self) -> None:
        """No-op: connections are closed immediately after each operation."""
        pass

"""CLI entry point for the OA Enricher pipeline (Phase 1 — read-only).

Steps and data flow
-------------------
Each step persists its output as a DuckDB table so that subsequent steps can
run independently without re-doing earlier work.  All year ranges coexist in
the same DB, keyed by (_year_from, _year_to).

    harvest     → oa_monitor_{env}.duckdb :: harvest table
    enrich      reads harvest → oa_monitor_{env}.duckdb :: enriched table
    export-noam reads enriched → CSVs + Excel in --output-dir
    inspect     prints a summary of available data in the DB
    all         runs all steps in sequence

The DB is in --work-dir (default: data/oa_work/).  It is separate from
pipeline_{env}.duckdb to avoid lock contention with the main pipeline UI.
The UI opens it read-only for monitoring and data browsing.

Usage:
    python oa_monitor/run_oa_enricher.py --year-from 2022 --year-to 2024

    # Step by step
    python oa_monitor/run_oa_enricher.py --year-from 2022 --year-to 2024 --step harvest
    python oa_monitor/run_oa_enricher.py --year-from 2022 --year-to 2024 --step enrich
    python oa_monitor/run_oa_enricher.py --year-from 2022 --year-to 2024 --step export-noam

    # Check what's in the DB
    python oa_monitor/run_oa_enricher.py --year-from 2022 --year-to 2024 --step inspect
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Project root must be on sys.path when run as a subprocess from the UI.
sys.path.append(str(Path(__file__).resolve().parent.parent))  # noqa: E402
# pylint: disable=wrong-import-position
from env_loader import load_env
from utils import get_pipeline_logger

logger = get_pipeline_logger("run_oa_enricher")

_SEP = "─" * 52
_DEFAULT_WORK_DIR = "data/oa_work"
_DEFAULT_OUTPUT_DIR = "data/noam"


# ── Log helpers ───────────────────────────────────────────────────────────────

def _step(label: str) -> float:
    logger.info(_SEP)
    logger.info("  %s", label)
    logger.info(_SEP)
    return time.perf_counter()


def _done(msg: str, t0: float) -> None:
    logger.info("  ✓  %s  [%.1f s]", msg, time.perf_counter() - t0)


# ── DuckDB checkpoint helpers ─────────────────────────────────────────────────

def _db_path(work_dir: Path, env: str) -> Path:
    work_dir.mkdir(parents=True, exist_ok=True)
    return work_dir / f"oa_monitor_{env}.duckdb"


_RAP_TABLE = "rap_tracking"
_RAP_GAP_TABLE = "rap_gap_analysis"


def _connect(path: Path, read_only: bool = False):
    """Open a DuckDB connection, turning a lock conflict into a clear error.

    DuckDB allows only one process to hold a connection to a given file.
    A conflict here almost always means another OA Monitor run for the same
    --env is still active — typically the UI's background subprocess
    (see data/oa_monitor_{env}.json for its PID) or a concurrent CLI
    invocation. Surfacing a raw duckdb.IOException/ConnectionException
    traceback obscures that; this gives an actionable message instead.
    """
    import duckdb  # pylint: disable=import-outside-toplevel
    try:
        return duckdb.connect(str(path), read_only=read_only)
    except duckdb.OperationalError:
        logger.error("═" * 52)
        logger.error("  Could not open %s (%s)", path, "read-only" if read_only else "read-write")
        logger.error("  Another process already holds a connection to this file —")
        logger.error("  most likely a still-running OA Monitor job for the same --env")
        logger.error("  (check the UI's ▶ Lancer tab, or data/oa_monitor_{env}.json for its PID).")
        logger.error("  Wait for it to finish, then retry.")
        logger.error("═" * 52)
        sys.exit(1)


def _duckdb_col_type(series) -> str:
    """Map a pandas Series dtype to a DuckDB column type for ALTER TABLE."""
    import pandas as _pd
    if _pd.api.types.is_bool_dtype(series.dtype):
        return "BOOLEAN"
    if _pd.api.types.is_integer_dtype(series.dtype):
        return "BIGINT"
    if _pd.api.types.is_float_dtype(series.dtype):
        return "DOUBLE"
    return "VARCHAR"


def _rebuild_table(con, table: str, df_new, year_from: int, year_to: int) -> None:
    """Drop and recreate a table, preserving rows from other year ranges."""
    import pandas as _pd
    df_keep = con.execute(
        f"SELECT * FROM {table} "
        f"WHERE NOT (_year_from = {year_from} AND _year_to = {year_to})"
    ).df()
    con.execute(f"DROP TABLE {table}")
    if not df_keep.empty:
        merged = _pd.concat([df_keep, df_new], ignore_index=True)
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM merged")
    else:
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_new")


def _save(df, table: str, work_dir: Path, env: str, year_from: int, year_to: int) -> None:
    """Write df into a DuckDB table, replacing any existing rows for this year range.

    Handles schema evolution automatically:
    - New columns in df are added via ALTER TABLE (e.g., resolved_oa_source added later).
    - Type mismatches (e.g., isbn column changed from INT to VARCHAR) trigger a full table
      rebuild that preserves all other year-range rows.
    """
    import duckdb  # pylint: disable=import-outside-toplevel
    path = _db_path(work_dir, env)
    df_out = df.copy()
    df_out["_year_from"] = year_from
    df_out["_year_to"] = year_to
    con = _connect(path)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if table in tables:
            con.execute(
                f"DELETE FROM {table} WHERE _year_from = {year_from} AND _year_to = {year_to}"
            )
            # Add any new columns that appeared in df_out since the table was created
            existing_cols = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}
            for col in df_out.columns:
                if col not in existing_cols:
                    col_type = _duckdb_col_type(df_out[col])
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                    logger.info("  added column '%s' (%s) to %s", col, col_type, table)
            # Re-read column order after any ALTER and align df_out to it
            tbl_cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
            df_aligned = df_out.reindex(columns=tbl_cols)
            try:
                con.execute(f"INSERT INTO {table} SELECT * FROM df_aligned")
            except duckdb.ConversionException as exc:
                # Column type mismatch (e.g., isbn was INT, now VARCHAR) — rebuild the table
                # preserving all other year-range rows with the new schema.
                logger.warning("  type mismatch in '%s' — rebuilding table: %s", table, exc)
                _rebuild_table(con, table, df_out, year_from, year_to)
        else:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_out")
        logger.info("  saved → %s :: %s  (%d rows)", path.name, table, len(df))
    finally:
        con.close()


def _load(table: str, work_dir: Path, env: str, year_from: int, year_to: int):
    """Read rows for the given year range from a DuckDB table."""
    path = _db_path(work_dir, env)
    if not path.exists():
        logger.error("  DB not found: %s", path)
        logger.error("  Run --step harvest first.")
        sys.exit(1)
    con = _connect(path, read_only=True)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if table not in tables:
            logger.error("  Table '%s' not found in %s", table, path.name)
            prev = "harvest" if table == "enriched" else "harvest"
            logger.error("  Run --step %s first.", prev)
            sys.exit(1)
        df = con.execute(
            f"SELECT * EXCLUDE (_year_from, _year_to) FROM {table} "
            f"WHERE _year_from = {year_from} AND _year_to = {year_to}"
        ).df()
        if df.empty:
            logger.error("  No rows in '%s' for %d–%d", table, year_from, year_to)
            sys.exit(1)
        logger.info("  loaded ← %s :: %s  (%d rows)", path.name, table, len(df))
        return df
    finally:
        con.close()


def _load_all(table: str, work_dir: Path, env: str):
    """Read every row of a year-partitioned table, across all year ranges.

    Used by steps that need the full history (e.g. rap-gaps matching R&P
    tracking, which spans 2020-2026, against every harvested year range).
    """
    path = _db_path(work_dir, env)
    if not path.exists():
        logger.error("  DB not found: %s", path)
        sys.exit(1)
    con = _connect(path, read_only=True)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if table not in tables:
            logger.error("  Table '%s' not found in %s", table, path.name)
            sys.exit(1)
        df = con.execute(f"SELECT * EXCLUDE (_year_from, _year_to) FROM {table}").df()
        logger.info("  loaded ← %s :: %s  (%d rows, all year ranges)", path.name, table, len(df))
        return df
    finally:
        con.close()


# rap_tracking and rap_gap_analysis are full consolidations — unlike
# harvest/enriched they have no natural year-range scope, so each is stored as
# a single table refreshed in full on every run (no _year_from/_year_to columns).

def _save_global_table(df, table: str, work_dir: Path, env: str) -> None:
    """Persist a full-refresh table (no year partitioning)."""
    path = _db_path(work_dir, env)
    con = _connect(path)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df")
        logger.info("  saved → %s :: %s  (%d rows)", path.name, table, len(df))
    finally:
        con.close()


def _load_global_table(table: str, work_dir: Path, env: str, required_step: str):
    """Read a full-refresh (non year-partitioned) table."""
    path = _db_path(work_dir, env)
    if not path.exists():
        logger.error("  DB not found: %s", path)
        logger.error("  Run --step %s first.", required_step)
        sys.exit(1)
    con = _connect(path, read_only=True)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if table not in tables:
            logger.error("  Table '%s' not found in %s", table, path.name)
            logger.error("  Run --step %s first.", required_step)
            sys.exit(1)
        df = con.execute(f"SELECT * FROM {table}").df()
        logger.info("  loaded ← %s :: %s  (%d rows)", path.name, table, len(df))
        return df
    finally:
        con.close()


# ── Step: inspect ─────────────────────────────────────────────────────────────

def _run_inspect(work_dir: Path, env: str, year_from: int, year_to: int) -> int:
    """Print a summary of data available in the OA Monitor DB for this year range."""
    path = _db_path(work_dir, env)
    logger.info("═" * 52)
    logger.info("  OA Monitor DB — %d–%d  [env: %s]", year_from, year_to, env)
    logger.info("  %s", path)
    logger.info("═" * 52)
    if not path.exists():
        logger.info("  (DB does not exist yet — run --step harvest first)")
        return 0
    con = _connect(path, read_only=True)
    try:
        size_kb = path.stat().st_size // 1024
        logger.info("  DB size: %d KB", size_kb)
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in ("harvest", "enriched"):
            if table not in tables:
                logger.info("  [ ] %-10s  (not yet generated)", table)
                continue
            count = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE _year_from={year_from} AND _year_to={year_to}"
            ).fetchone()[0]
            if count == 0:
                logger.info("  [ ] %-10s  (no data for %d–%d)", table, year_from, year_to)
                continue
            logger.info("  [✓] %-10s  %d rows", table, count)
            if table == "enriched":
                rows = con.execute(
                    f"SELECT oa_category_advanced, COUNT(*) AS n "
                    f"FROM {table} WHERE _year_from={year_from} AND _year_to={year_to} "
                    f"GROUP BY 1 ORDER BY n DESC"
                ).fetchall()
                breakdown = "  |  ".join(f"{r[0]}: {r[1]}" for r in rows)
                logger.info("      OA: %s", breakdown)
                open_n = con.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE _year_from={year_from} AND _year_to={year_to} "
                    f"AND oa_category_basic = 'Open'"
                ).fetchone()[0]
                logger.info("      Open Access: %d/%d (%.0f%%)",
                            open_n, count, 100 * open_n / count if count else 0)

        # rap_tracking is global (no year-range scope) — reported unconditionally.
        if _RAP_TABLE not in tables:
            logger.info("  [ ] %-10s  (not yet generated)", _RAP_TABLE)
        else:
            rap_count = con.execute(f"SELECT COUNT(*) FROM {_RAP_TABLE}").fetchone()[0]
            review_count = con.execute(
                f"SELECT COUNT(*) FROM {_RAP_TABLE} WHERE "
                f"license_needs_review OR oa_type_needs_review OR article_type_needs_review"
            ).fetchone()[0]
            logger.info("  [✓] %-10s  %d rows  (%d flagged for review)",
                        _RAP_TABLE, rap_count, review_count)

        # rap_gap_analysis is also global — reported unconditionally.
        if _RAP_GAP_TABLE not in tables:
            logger.info("  [ ] %-10s  (not yet generated)", _RAP_GAP_TABLE)
        else:
            gap_rows = con.execute(
                f"SELECT gap_status, COUNT(*) AS n FROM {_RAP_GAP_TABLE} "
                f"GROUP BY 1 ORDER BY n DESC"
            ).fetchall()
            gap_total = sum(r[1] for r in gap_rows)
            breakdown = "  |  ".join(f"{r[0]}: {r[1]}" for r in gap_rows)
            logger.info("  [✓] %-10s  %d rows  (%s)", _RAP_GAP_TABLE, gap_total, breakdown)

        # List all available year ranges
        all_ranges = []
        for table in ("harvest", "enriched"):
            if table in tables:
                ranges = con.execute(
                    f"SELECT DISTINCT _year_from, _year_to, COUNT(*) AS n "
                    f"FROM {table} GROUP BY 1, 2 ORDER BY 1, 2"
                ).fetchall()
                all_ranges.extend((table, r[0], r[1], r[2]) for r in ranges)
        if all_ranges:
            logger.info("  " + _SEP)
            logger.info("  All year ranges in DB:")
            for table, yf, yt, n in all_ranges:
                logger.info("    %-10s  %d–%d  (%d rows)", table, yf, yt, n)
    finally:
        con.close()
    logger.info("═" * 52)
    return 0


# ── Step: purge ───────────────────────────────────────────────────────────────

def _run_purge(work_dir: Path, env: str, year_from: int, year_to: int, all_data: bool) -> int:
    """Delete harvest + enriched rows for the given year range, or all data."""
    path = _db_path(work_dir, env)
    if not path.exists():
        logger.info("  DB does not exist (%s) — nothing to purge.", path)
        return 0

    scope_label = "ALL DATA" if all_data else f"{year_from}–{year_to}"
    logger.info("═" * 52)
    logger.info("  Purge  [%s]  env=%s", scope_label, env)
    logger.info("  %s", path)
    logger.info("═" * 52)

    con = _connect(path)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in ("harvest", "enriched"):
            if table not in tables:
                logger.info("  [ ] %-10s  (table absent, skip)", table)
                continue
            if all_data:
                deleted = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                con.execute(f"DELETE FROM {table}")
            else:
                deleted = con.execute(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE _year_from = {year_from} AND _year_to = {year_to}"
                ).fetchone()[0]
                con.execute(
                    f"DELETE FROM {table} WHERE _year_from = {year_from} AND _year_to = {year_to}"
                )
            logger.info("  [✓] %-10s  %d rows deleted  [%s]", table, deleted, scope_label)
    finally:
        con.close()

    logger.info("═" * 52)
    logger.info("  Purge complete.")
    logger.info("═" * 52)
    return 0


def _run_purge_rap(work_dir: Path, env: str) -> int:
    """Drop rap_tracking + rap_gap_analysis. Never touches data/apc source files.

    Both tables are dropped outright (not just emptied) so that a subsequent
    --step rap-gaps / --step inspect gives a clear "run consolidate-rap first"
    message rather than silently operating on an empty table.
    """
    path = _db_path(work_dir, env)
    if not path.exists():
        logger.info("  DB does not exist (%s) — nothing to purge.", path)
        return 0

    logger.info("═" * 52)
    logger.info("  Purge R&P tables  [%s, %s]  env=%s", _RAP_TABLE, _RAP_GAP_TABLE, env)
    logger.info("  %s", path)
    logger.info("  (data/apc source workbooks are never touched)")
    logger.info("═" * 52)

    con = _connect(path)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        for table in (_RAP_TABLE, _RAP_GAP_TABLE):
            if table not in tables:
                logger.info("  [ ] %-18s  (table absent, skip)", table)
                continue
            deleted = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            con.execute(f"DROP TABLE {table}")
            logger.info("  [✓] %-18s  %d rows deleted", table, deleted)
    finally:
        con.close()

    logger.info("═" * 52)
    logger.info("  Purge complete.")
    logger.info("═" * 52)
    return 0


# ── Argument parser ───────────────────────────────────────────────────────────

def _parse_args(argv=None) -> argparse.Namespace:
    """Parse CLI arguments."""
    p = argparse.ArgumentParser(
        description="OA Enricher — classify Infoscience publications and export NOAM data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--year-from", type=int, default=None,
        help="Start year (inclusive) — required for harvest/enrich/export-noam/inspect/purge",
    )
    p.add_argument(
        "--year-to", type=int, default=None,
        help="End year (inclusive) — required for harvest/enrich/export-noam/inspect/purge",
    )
    p.add_argument(
        "--step",
        choices=[
            "all", "harvest", "enrich", "export-noam", "inspect", "purge",
            "consolidate-rap", "rap-gaps", "purge-rap",
        ],
        default="all",
        help=(
            "Pipeline step (default: all). "
            "harvest → DuckDB::harvest. "
            "enrich → reads harvest, writes DuckDB::enriched. "
            "export-noam → reads enriched, writes CSVs+Excel. "
            "inspect → prints DB summary. "
            "purge → delete rows for the given year range (add --purge-all to delete everything). "
            "consolidate-rap → consolidates data/apc R&P tracking workbooks into DuckDB::rap_tracking "
            "(no year range needed — processes all years in one pass). "
            "rap-gaps → joins rap_tracking against enriched (all year ranges) by DOI, writes "
            "DuckDB::rap_gap_analysis (which R&P articles are missing from Infoscience or not "
            "yet open with the published/accepted version). "
            "purge-rap → drops rap_tracking + rap_gap_analysis (no year range needed). "
            "data/apc source workbooks are never touched — re-run consolidate-rap to rebuild."
        ),
    )
    p.add_argument(
        "--apc-root",
        default="data/apc",
        help="Root directory for R&P tracking workbooks (default: data/apc). Used by --step consolidate-rap.",
    )
    p.add_argument(
        "--purge-all",
        action="store_true",
        help="Used with --step purge: delete all data regardless of year range.",
    )
    p.add_argument(
        "--work-dir",
        default=_DEFAULT_WORK_DIR,
        help=f"Directory for the OA Monitor DuckDB (default: {_DEFAULT_WORK_DIR})",
    )
    p.add_argument(
        "--output-dir",
        default=_DEFAULT_OUTPUT_DIR,
        help=f"Output directory for NOAM CSVs and Excel (default: {_DEFAULT_OUTPUT_DIR})",
    )
    p.add_argument(
        "--env",
        default="dev",
        choices=["dev", "test", "prod"],
        help="Environment (default: dev)",
    )
    p.add_argument(
        "--extra-filter",
        default=None,
        help="Additional Solr clause ANDed into the DSpace harvest query.",
    )
    p.add_argument("--no-unpaywall", action="store_true", help="Skip Unpaywall enrichment")
    p.add_argument(
        "--no-bitstream-metadata",
        action="store_true",
        help="Skip bitstream-level metadata fetch (faster harvest, uses record-level only)",
    )
    p.add_argument("--batch-size", type=int, default=100, help="OpenAlex DOI batch size")
    p.add_argument("--upw-workers", type=int, default=5, help="Unpaywall parallel workers")
    p.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Cap harvest at N items (for testing; omit for production)",
    )
    p.add_argument("-v", "--verbose", action="count", default=0)
    return p.parse_args(argv)


def _configure_logging(verbosity: int) -> None:
    """Set root log level from -v / -vv count."""
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )


# ── Main run ──────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> int:
    """Execute the requested pipeline step(s) and return an exit code."""
    load_env(args.env)

    from oa_monitor.oa_harvester import InfoscienceOAHarvester  # pylint: disable=import-outside-toplevel
    from oa_monitor.oa_enricher import (  # pylint: disable=import-outside-toplevel
        apply_classification,
        enrich_openalex,
        enrich_unpaywall,
        export_noam,
    )

    step = args.step
    year_from, year_to = args.year_from, args.year_to
    work_dir = Path(args.work_dir)
    t_total = time.perf_counter()

    if step == "consolidate-rap":
        from oa_monitor.rap_consolidator import consolidate_rap_tracking  # pylint: disable=import-outside-toplevel
        t0 = _step(f"R&P consolidation  ({args.apc_root})")
        df = consolidate_rap_tracking(args.apc_root)
        review_cols = ["license_needs_review", "oa_type_needs_review", "article_type_needs_review"]
        n_review = int(df[review_cols].any(axis=1).sum()) if len(df) and all(c in df.columns for c in review_cols) else 0
        _save_global_table(df, _RAP_TABLE, work_dir, args.env)
        _done(f"{len(df):,} rows consolidated ({n_review} flagged for review)", t0)
        return 0

    if step == "rap-gaps":
        from oa_monitor.rap_gap_analysis import analyze_rap_gaps  # pylint: disable=import-outside-toplevel
        t0 = _step("R&P gap analysis — rap_tracking × enriched")
        rap_df = _load_global_table(_RAP_TABLE, work_dir, args.env, required_step="consolidate-rap")
        enriched_df = _load_all("enriched", work_dir, args.env)
        result = analyze_rap_gaps(rap_df, enriched_df)
        _save_global_table(result, _RAP_GAP_TABLE, work_dir, args.env)
        counts = result["gap_status"].value_counts()
        _done(
            f"{len(result):,} rows — " + "  |  ".join(f"{k}: {v}" for k, v in counts.items()),
            t0,
        )
        return 0

    if step == "purge-rap":
        return _run_purge_rap(work_dir, args.env)

    # harvest/enrich/export-noam/inspect/purge all operate on a year range.
    if year_from is None or year_to is None:
        logger.error("  --year-from and --year-to are required for --step %s", step)
        return 2

    if step == "inspect":
        return _run_inspect(work_dir, args.env, year_from, year_to)

    if step == "purge":
        return _run_purge(
            work_dir, args.env, year_from, year_to,
            all_data=getattr(args, "purge_all", False),
        )

    logger.info("═" * 52)
    logger.info("  OA Monitor — %d–%d  [step: %s, env: %s]", year_from, year_to, step, args.env)
    if args.extra_filter:
        logger.info("  filter: %s", args.extra_filter)
    logger.info("  DB: %s", _db_path(work_dir, args.env))
    logger.info("═" * 52)

    # ── M1 : Harvest ──────────────────────────────────────────────────────────
    if step in ("all", "harvest"):
        t0 = _step(f"M1 — Harvest  ({year_from}–{year_to})")
        harvester = InfoscienceOAHarvester()
        df = harvester.harvest(
            year_from, year_to,
            extra_filter=args.extra_filter,
            max_items=getattr(args, "max_items", None),
            with_bitstream_metadata=not getattr(args, "no_bitstream_metadata", False),
        )
        _save(df, "harvest", work_dir, args.env, year_from, year_to)
        _done(f"{len(df):,} items saved to DB::harvest", t0)

        if step == "harvest":
            logger.info("═" * 52)
            logger.info("  DONE — next: --step enrich --year-from %d --year-to %d", year_from, year_to)
            logger.info("═" * 52)
            return 0

    # ── M2 : Enrich ───────────────────────────────────────────────────────────
    if step in ("all", "enrich"):
        if step == "enrich":
            t0 = _step("Load DB::harvest checkpoint")
            df = _load("harvest", work_dir, args.env, year_from, year_to)
            _done(f"{len(df):,} rows", t0)

        t0 = _step(f"M2a — OpenAlex enrichment  (batch={args.batch_size})")
        df = enrich_openalex(df, batch_size=args.batch_size)
        matched = int(df["oa_is_oa"].notna().sum()) if "oa_is_oa" in df.columns else 0
        _done(f"{matched:,}/{len(df):,} DOIs matched in OpenAlex", t0)

        if not args.no_unpaywall:
            t0 = _step(f"M2b — Unpaywall enrichment  (workers={args.upw_workers})")
            df = enrich_unpaywall(df, workers=args.upw_workers)
            upw_matched = int(df["upw_is_oa"].notna().sum()) if "upw_is_oa" in df.columns else 0
            _done(f"{upw_matched:,}/{len(df):,} DOIs matched in Unpaywall", t0)
        else:
            logger.info("  (Unpaywall skipped — --no-unpaywall)")

        t0 = _step("M2c — OA classification")
        df = apply_classification(df)
        _done(f"{len(df):,} rows classified", t0)

        _save(df, "enriched", work_dir, args.env, year_from, year_to)

        if step == "enrich":
            logger.info("═" * 52)
            logger.info("  DONE — %d items  [%.1f s]  next: --step export-noam --year-from %d --year-to %d",
                        len(df), time.perf_counter() - t_total, year_from, year_to)
            logger.info("═" * 52)
            return 0

    # ── M6 : Export NOAM ──────────────────────────────────────────────────────
    if step in ("all", "export-noam"):
        if step == "export-noam":
            t0 = _step("Load DB::enriched checkpoint")
            df = _load("enriched", work_dir, args.env, year_from, year_to)
            _done(f"{len(df):,} rows", t0)

        output_dir = Path(args.output_dir)
        t0 = _step(f"M6 — NOAM export  → {output_dir}")
        result = export_noam(df, output_dir, year_from=year_from, year_to=year_to)
        logger.info("  Excel : %s", result["excel_file"].name)
        for _year, path in sorted(result["csv_files"].items()):
            logger.info("  CSV   : %s", path.name)
        _done(f"{len(result['csv_files'])} CSV files + Excel written", t0)

    logger.info("═" * 52)
    logger.info("  DONE — %d items  [%.1f s total]", len(df), time.perf_counter() - t_total)
    logger.info("═" * 52)
    return 0


def main(argv=None) -> None:
    """CLI entry point."""
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    sys.exit(run(args))


if __name__ == "__main__":
    main()

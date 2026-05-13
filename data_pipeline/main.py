#!/usr/bin/env python3
"""Main script to run the data pipeline (cron-friendly)."""

import os
import sys
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent))


# --- Project imports
import env_loader
from config import default_queries
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader
from data_pipeline.reporting import GenerateReports
from db.pipeline_db import PipelineDB
from data_pipeline.harvester import (
    WosHarvester,
    ScopusHarvester,
    CrossrefHarvester,
    OpenAlexCrossrefHarvester,
    ZenodoHarvester,
    EPOHarvester,
)

# -----------------------------------------------------------------------------
# Environment & constants
# -----------------------------------------------------------------------------
# load_dotenv() is intentionally NOT called here at module level.
# env_loader.load_env() is called inside main() after --env is parsed,
# so the correct .env.{env} file is loaded before any os.getenv() access.

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SUPPORTED_SOURCES = ("wos", "scopus", "crossref", "openalex", "zenodo", "epo")


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
def setup_logger(verbosity: int = 0) -> logging.Logger:
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)

    # Console handler (cron sees stdout/stderr)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbosity > 0 else logging.INFO)
    ch_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    ch.setFormatter(ch_formatter)

    # Rotating file handler
    fh = RotatingFileHandler(
        LOG_DIR / "pipeline.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    fh.setFormatter(fh_formatter)

    # Avoid duplicate handlers in case of re-import
    if not logger.handlers:
        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def parse_author_ids(arg: Optional[str]) -> List[str]:
    """Accepts a comma-separated string or a path to a file (one ID per line)."""
    if not arg:
        return []
    p = Path(arg)
    if p.exists() and p.is_file():
        return [
            line.strip()
            for line in p.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    # else treat as inline list
    raw = arg.replace(",", "\n").splitlines()
    return [x.strip() for x in raw if x.strip()]


def parse_sources(
    arg: Optional[str], logger: Optional[logging.Logger] = None
) -> List[str]:
    """
    Parse a comma-separated list of sources. If None or 'all', return all supported.
    Unknown sources are ignored with a warning.
    """
    if not arg or arg.strip().lower() == "all":
        return list(SUPPORTED_SOURCES)

    raw = [s.strip().lower() for s in arg.split(",") if s.strip()]
    valid = [s for s in raw if s in SUPPORTED_SOURCES]
    invalid = [s for s in raw if s not in SUPPORTED_SOURCES]

    if invalid and logger:
        logger.warning(
            f"Ignoring unknown sources: {', '.join(invalid)}. "
            f"Supported: {', '.join(SUPPORTED_SOURCES)}"
        )

    if not valid:
        if logger:
            logger.error(
                "No valid sources provided after filtering. "
                f"Supported: {', '.join(SUPPORTED_SOURCES)}"
            )
        return []
    return valid


def ensure_queries(override: Optional[Dict[str, str]]) -> Dict[str, str]:
    merged = dict(default_queries)
    if override:
        merged.update({k.lower(): v for k, v in override.items()})
    # make sure all expected keys exist
    for k in SUPPORTED_SOURCES:
        merged.setdefault(k, "")
    return merged


def save_csv(
    df: pd.DataFrame, filename: str, export_dir: Path, logger: logging.Logger
) -> Optional[Path]:
    """Saves a DataFrame to CSV if it's not empty and ensures the directory exists."""
    if isinstance(df, (list, tuple, dict)):
        df = pd.DataFrame(df)
    if df is None or df.empty:
        logger.debug(f"Skip empty dataframe: {filename}")
        return None
    export_dir.mkdir(parents=True, exist_ok=True)
    filepath = export_dir / filename
    df.to_csv(filepath, index=False, encoding="utf-8")
    logger.info(f"Saved CSV: {filepath} ({len(df)} rows)")
    return filepath


def date_range_from_window(
    window_days: int, end_date: Optional[str] = None
) -> Tuple[str, str]:
    """Compute [start,end] ISO dates for a sliding window (inclusive)."""
    if end_date:
        end = datetime.fromisoformat(end_date).date()
    else:
        end = datetime.now().date()
    start = end - timedelta(days=max(1, window_days) - 1)
    return start.isoformat(), end.isoformat()


# ---------- New helpers for ID-based harvesting --------------------------------
def build_id_queries(
    scopus_ids: List[str],
    wos_ids: List[str],
    orcids: List[str],
    openalex_ids: List[str] = [],
) -> Dict[str, str]:
    """
    Build per-source query strings targeting author identifiers.
    NOTE: Adapte au besoin selon la syntaxe attendue par tes harvesters.
    """
    id_queries: Dict[str, str] = {}

    # SCOPUS
    scopus_bits: List[str] = []
    if scopus_ids:
        scopus_bits += [f"AU-ID({aid})" for aid in scopus_ids]
    if orcids:
        scopus_bits += [f"ORCID({o})" for o in orcids]
    if scopus_bits:
        id_queries["scopus"] = " OR ".join(scopus_bits)

    wos_bits: List[str] = []
    # WOS: AI=() 
    if wos_ids:
        wos_bits += wos_ids
    if orcids:
        wos_bits += orcids
    if wos_bits:
        # Exemple: AI=(R-1234-2017 OR 0000-0002-1825-0097)
        id_queries["wos"] = f"AI=({ ' OR '.join(wos_bits) })"

    # CROSSREF: le harvester utilise field_queries; ici on encode une "pseudo" query.
    # Option 1 (souvent OK): filter orcid côté harvester en parsant ce motif.
    # Exemple valeur: FILTER_ORCID:0000-0001-...,0000-0002-...
    # if orcids:
    #     id_queries["crossref"] = "FILTER_ORCID:" + ",".join(orcids)

    # OPENALEX
    if openalex_ids:
        id_queries["openalex"] = "authorships.author.id:" + "|".join(openalex_ids)
    elif orcids:
        id_queries["openalex"] = "authorships.author.orcid:" + "|".join(orcids)
    # ZENODO: pas de recherche par auteur-id standard → ne change rien par défaut
    return id_queries


def merge_unique(*lists: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for lst in lists:
        for x in lst or []:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
    return out


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
def run_pipeline(
    logger: logging.Logger,
    start_date: str,
    end_date: str,
    queries: Dict[str, str],
    author_ids: List[str],
    output_dir: Path,
    dry_run: bool = False,
    no_email: bool = False,
    sources: Optional[List[str]] = None,
    run_id: Optional[str] = None,
) -> Dict[str, pd.DataFrame | str | None]:
    """
    Harvest, deduplicate, enrich, and (optionally) load data into DSpace.
    Returns dict of dataframes and report path.
    """
    active_sources = sources or list(SUPPORTED_SOURCES)
    if not active_sources:
        raise ValueError("No sources selected. Use --sources to specify at least one.")

    logger.info(
        f"=== Pipeline start | window: {start_date} → {end_date} | "
        f"dry_run={dry_run} | sources={','.join(active_sources)} ==="
    )

    # Use caller-supplied run_id (e.g. from UI) so DuckDB and the UI state
    # file share the same identifier; fall back to a local timestamp.
    execution_timestamp = run_id or datetime.now().strftime("%Y-%m-%d_%H-%M")
    export_dir = output_dir / execution_timestamp
    export_dir.mkdir(parents=True, exist_ok=True)

    # -------------------- Harvest
    def safe_harvest(name: str, fn) -> pd.DataFrame:
        try:
            df = pd.DataFrame(fn())
            logger.info(f"[Harvest] {name}: {len(df)} records")
            return df
        except Exception as e:
            logger.exception(f"[Harvest] {name} failed: {e}")
            return pd.DataFrame()

    # full registry
    registry = {
        "wos": lambda: WosHarvester(start_date, end_date, queries["wos"]).harvest(),
        "scopus": lambda: ScopusHarvester(
            start_date, end_date, queries["scopus"]
        ).harvest(),
        "crossref": lambda: CrossrefHarvester(
            start_date,
            end_date,
            query=None,
            field_queries={"query.affiliation": queries["crossref"]},
        ).harvest(),
        "openalex": lambda: OpenAlexCrossrefHarvester(
            start_date, end_date, queries["openalex"]
        ).harvest(),
        "zenodo": lambda: ZenodoHarvester(
            start_date, end_date, queries["zenodo"]
        ).harvest(),
        "epo": lambda: EPOHarvester(
            start_date, end_date, queries["epo"]
        ).harvest(),
    }

    # keep only selected
    harvesters = {k: v for k, v in registry.items() if k in active_sources}

    publications: Dict[str, pd.DataFrame] = {
        name: safe_harvest(name, fn) for name, fn in harvesters.items()
    }

    for name, df in publications.items():
        save_csv(df, f"Raw_{name.capitalize()}Items.csv", export_dir, logger)

    # -------------------- Deduplication
    non_empty = [df for df in publications.values() if not df.empty]
    if not non_empty:
        logger.warning("No harvested data from selected sources; nothing to process.")
        return {
            "df_metadata": pd.DataFrame(),
            "df_authors": pd.DataFrame(),
            "df_epfl_authors": pd.DataFrame(),
            "df_unloaded": pd.DataFrame(),
            "df_loaded": pd.DataFrame(),
            "df_rejected": pd.DataFrame(),
            "report_path": None,
        }

    deduplicator = DataFrameProcessor(*non_empty)
    df_deduplicated = deduplicator.deduplicate_dataframes()
    save_csv(df_deduplicated, "DeduplicatedItems.csv", export_dir, logger)

    # DSpace-aware dedup (what to import vs duplicates)
    if df_deduplicated.empty:
        logger.warning("Deduplicated dataframe is empty.")
        df_final, df_unloaded = pd.DataFrame(), pd.DataFrame()
    else:
        df_final, df_unloaded = deduplicator.deduplicate_infoscience(df_deduplicated)
    save_csv(df_unloaded, "UnloadedItems.csv", export_dir, logger)

    # -------------------- Build main dataframes
    if df_final.empty:
        df_metadata, df_authors = pd.DataFrame(), pd.DataFrame()
    else:
        df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)

    save_csv(df_metadata, "Items.csv", export_dir, logger)
    save_csv(df_authors, "AuthorsAndAffiliations.csv", export_dir, logger)

    # -------------------- Author enrichment
    if df_authors.empty:
        df_epfl_authors = pd.DataFrame()
    else:
        ap = AuthorProcessor(df_authors)
        df_epfl_authors = (
            ap.process(author_ids_to_check=author_ids)
            .filter_epfl_authors()
            .clean_authors()
            .nameparse_authors()
            .reconcile_authors(return_df=True)
        )

    save_csv(df_epfl_authors, "EpflAuthors.csv", export_dir, logger)

    # -------------------- Publication enrichment (OA, fulltexts, etc.)
    if df_metadata.empty:
        df_oa_metadata = pd.DataFrame()
    else:
        df_oa_metadata = PublicationProcessor(df_metadata).process(return_df=True)

    save_csv(df_oa_metadata, "ItemsWithOAMetadata.csv", export_dir, logger)

    # -------------------- Load into DSpace (unless dry-run)
    if dry_run or df_oa_metadata.empty:
        logger.info("Dry-run active or no enriched items → skip loading.")
        df_loaded = pd.DataFrame()
    else:
        loader = Loader(df_oa_metadata, df_epfl_authors, df_authors)
        df_loaded = loader.create_complete_publication()

    save_csv(df_loaded, "ImportedItems.csv", export_dir, logger)

    # -------------------- Rejected
    if df_oa_metadata.empty:
        df_rejected = pd.DataFrame()
    elif (
        "row_id" in df_oa_metadata.columns
        and "row_id" in getattr(df_loaded, "columns", [])
        and not df_loaded.empty
    ):
        df_rejected = df_oa_metadata[
            ~df_oa_metadata["row_id"].isin(df_loaded["row_id"])
        ]
    else:
        df_rejected = df_oa_metadata.copy()

    save_csv(df_rejected, "RejectedItems.csv", export_dir, logger)

    # -------------------- Persist to DuckDB
    run_id = execution_timestamp
    try:
        db = PipelineDB()
        db.start_run(run_id=run_id, window_start=start_date, window_end=end_date,
                     sources=active_sources, dry_run=dry_run)

        # Stats par source
        for src, df_src in publications.items():
            db.record_source_stats(run_id=run_id, source=src, harvested=len(df_src))
        db.record_source_stats(
            run_id=run_id, source="__total__",
            harvested=sum(len(d) for d in publications.values()),
            deduplicated=len(df_unloaded),
            loaded=len(df_loaded) if not df_loaded.empty else 0,
            rejected=len(df_rejected) if not df_rejected.empty else 0,
        )

        # Publications (importées + rejetées + dédoublonnées)
        db.record_publications(run_id, df_loaded, df_rejected,
                               df_deduplicated=df_unloaded if not df_unloaded.empty else None)

        # Auteurs EPFL et unités (upsert)
        if df_epfl_authors is not None and not df_epfl_authors.empty:
            db.record_epfl_authors(df_epfl_authors)
            db.record_units(df_epfl_authors)
            db.record_pub_author_links(run_id, df_epfl_authors)
            db.record_pub_unit_links(run_id, df_epfl_authors)
            db.record_detected_authors(run_id, df_epfl_authors)

        db.finish_run(run_id, status="completed")
        db.close()
        logger.info("DuckDB: run %s enregistré (%d importés, %d rejetés, %d dédoublonnés)",
                    run_id,
                    len(df_loaded) if not df_loaded.empty else 0,
                    len(df_rejected) if not df_rejected.empty else 0,
                    len(df_unloaded))
    except Exception as e:
        logger.warning("DuckDB: échec de l'enregistrement (non bloquant) — %s: %s",
                       type(e).__name__, e, exc_info=True)

    # -------------------- Report & email
    recipient_email = os.getenv("RECIPIENT_EMAIL")
    sender_email    = os.getenv("SENDER_EMAIL")
    smtp_server     = os.getenv("SMTP_SERVER")

    report_path = None
    can_report = any(
        not df.empty for df in [df_oa_metadata, df_unloaded, df_epfl_authors, df_loaded]
    )
    if can_report and "row_id" in getattr(df_loaded, "columns", []):
        try:
            generator = GenerateReports(
                df_oa_metadata, df_unloaded, df_epfl_authors, df_loaded
            )
            report_path = generator.generate_excel_report(output_dir=export_dir, run_id=execution_timestamp)
            logger.info(f"Report generated: {report_path}")

            if (
                not no_email
                and not dry_run
                and recipient_email
                and sender_email
                and smtp_server
            ):
                generator.send_report_by_email(
                    recipient_email=recipient_email,
                    sender_email=sender_email,
                    smtp_server=smtp_server,
                    import_start_date=start_date,
                    import_end_date=end_date,
                    file_path=report_path,
                    run_id=execution_timestamp,
                )
                logger.info(f"Report emailed to {recipient_email}")
            else:
                logger.info("Email sending skipped (no_email/dry_run or env not set).")
        except Exception as e:
            logger.exception(f"Failed to generate/send report: {e}")

    logger.info("=== Pipeline end ===")

    return {
        "df_metadata": df_oa_metadata,
        "df_authors": df_authors,
        "df_epfl_authors": df_epfl_authors,
        "df_unloaded": df_unloaded,
        "df_loaded": df_loaded,
        "df_rejected": df_rejected,
        "report_path": str(report_path) if report_path else None,
    }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run the publications data pipeline (harvest → dedup → enrich → load → report)."
    )
    # Windowing
    p.add_argument(
        "--window-days",
        type=int,
        default=15,
        help="Sliding window size in days (default: 15).",
    )
    p.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Override start date (YYYY-MM-DD). If set, --end-date is required.",
    )
    p.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Override end date (YYYY-MM-DD). Required if --start-date is set.",
    )

    # Queries override (simple)
    p.add_argument("--query-wos", type=str, default=None)
    p.add_argument("--query-scopus", type=str, default=None)
    p.add_argument("--query-crossref", type=str, default=None)
    p.add_argument("--query-openalex", type=str, default=None)
    p.add_argument("--query-zenodo", type=str, default=None)
    p.add_argument("--query-epo", type=str, default=None)

    # Authors (legacy, kept)
    p.add_argument(
        "--author-ids",
        type=str,
        default=None,
        help="Comma-separated list OR path to file (one per line). Used by AuthorProcessor.author_ids_to_check.",
    )

    # ---------- New: ID-based harvesting options ----------
    p.add_argument(
        "--scopus-ids",
        type=str,
        default=None,
        help="Scopus Author IDs (comma-separated or file path). Triggers ID-based harvesting for Scopus.",
    )
    p.add_argument(
        "--wos-ids",
        type=str,
        default=None,
        help="Web of Science ResearcherIDs (comma-separated or file path). Triggers ID-based harvesting for WoS.",
    )
    p.add_argument(
        "--orcid-ids",
        type=str,
        default=None,
        help="ORCID iDs (comma-separated or file path). Triggers ID-based harvesting for Crossref/OpenAlex (+Scopus/WoS where supported).",
    )
    p.add_argument(
        "--openalex-ids",
        type=str,
        default=None,
        help="OpenAlex Author IDs (comma-separated or file path). Triggers ID-based harvesting for OpenAlex.",
    )   

    # Source selection
    p.add_argument(
        "--sources",
        type=str,
        default="all",
        help=(
            "Comma-separated subset of sources to harvest "
            f"(supported: {', '.join(SUPPORTED_SOURCES)}). Default: all"
        ),
    )

    # Output & mode
    p.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR}).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Run everything but skip the final load and email.",
    )
    p.add_argument(
        "--no-email",
        action="store_true",
        help="Do not send the report email (even if env is set).",
    )

    # Environment
    p.add_argument(
        "--env",
        choices=env_loader.ENVIRONMENTS,
        default=None,
        help=(
            "Target environment — loads the corresponding .env.{env} file. "
            f"Choices: {', '.join(env_loader.ENVIRONMENTS)}. "
            "Defaults to the persisted selection (or 'dev' if none)."
        ),
    )

    # Internal — set by the UI to keep run_id consistent between the state
    # file and DuckDB; not intended for manual use.
    p.add_argument(
        "--run-id",
        type=str,
        default=None,
        help=argparse.SUPPRESS,
    )

    # Misc
    p.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase verbosity (use -vv for debug).",
    )

    return p


def _validate_env(logger: logging.Logger) -> None:
    """Warn early if key environment variables are missing.

    Nothing here is hard-blocking, but surfacing missing vars at startup
    avoids cryptic failures deep inside a harvesting or loading step.

    Variable names are taken directly from the client source files:
      - dspace_rest_client/client.py → DS_API_ENDPOINT, DS_API_TOKEN
      - dspace_client_wrapper.py     → DS_API_ENDPOINT
      - api_epfl_client.py           → API_EPFL_USER, API_EPFL_PWD
      - unpaywall_client.py          → ELS_API_KEY
      - scopus_client.py             → SCOPUS_API_KEY, SCOPUS_INST_TOKEN
      - wos_client_v2.py             → WOS_TOKEN
      - epo_ops_client.py            → EPO_OPS_KEY, EPO_OPS_SECRET
      - crossref/unpaywall/openalex  → CONTACT_API_EMAIL
      - openalex_client.py           → OPENALEX_API_KEY
      - zenodo_client.py             → ZENODO_API_KEY
      - orcid_client.py              → ORCID_API_TOKEN
      - main.py                      → RECIPIENT_EMAIL, SENDER_EMAIL, SMTP_SERVER
    """
    # Truly required: without the DSpace endpoint the loader cannot run at all.
    if not os.getenv("DS_API_ENDPOINT"):
        logger.warning(
            "DS_API_ENDPOINT is not set — DSpace loading will fail. "
            "Set it in your .env file (see dspace/.sample.env)."
        )

    # Per-source optional vars — warn only, never exit.
    optional = {
        "DS_API_TOKEN":     "DSpace REST API static token (dspace_rest_client auth)",
        "API_EPFL_USER":    "EPFL People API authentication (author reconciliation)",
        "API_EPFL_PWD":     "EPFL People API authentication (author reconciliation)",
        "ELS_API_KEY":      "Elsevier API key (PDF retrieval via Unpaywall)",
        "SCOPUS_API_KEY":   "Scopus harvesting",
        "SCOPUS_INST_TOKEN":"Scopus institutional token (full-text & extended metadata)",
        "WOS_TOKEN":        "Web of Science harvesting",
        "EPO_OPS_KEY":      "EPO Open Patent Services harvesting",
        "EPO_OPS_SECRET":   "EPO Open Patent Services harvesting",
        "CONTACT_API_EMAIL":"Crossref / Unpaywall / OpenAlex polite pool",
        "OPENALEX_API_KEY": "OpenAlex authenticated API access",
        "ZENODO_API_KEY":   "Zenodo harvesting (authenticated rate limit)",
        "ORCID_API_TOKEN":  "ORCID author reconciliation",
        "RECIPIENT_EMAIL":  "Email report delivery (recipient)",
        "SENDER_EMAIL":     "Email report delivery (sender)",
        "SMTP_SERVER":      "Email report delivery (SMTP server)",
    }
    missing_optional = [
        f"  {var}: {desc}"
        for var, desc in optional.items()
        if not os.getenv(var)
    ]
    if missing_optional:
        logger.debug(
            "Optional env vars not set (related sources will degrade gracefully):\n%s",
            "\n".join(missing_optional),
        )


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    # Load the correct .env.{env} before any os.getenv() access
    active_env = env_loader.load_env(args.env)

    logger = setup_logger(args.verbose)
    logger.info("Environment: %s (.env.%s)", active_env, active_env)

    # Validate environment before doing anything else
    _validate_env(logger)

    # Compute date window
    if args.start_date and not args.end_date:
        parser.error("--start-date requires --end-date")
    if args.end_date and not args.start_date:
        parser.error("--end-date requires --start-date")

    if args.start_date and args.end_date:
        start_date, end_date = args.start_date, args.end_date
    else:
        start_date, end_date = date_range_from_window(args.window_days)

    # Parse IDs for ID-based harvesting
    scopus_ids = parse_author_ids(args.scopus_ids)
    wos_ids = parse_author_ids(args.wos_ids)
    orcid_ids = parse_author_ids(args.orcid_ids)
    openalex_ids = parse_author_ids(args.openalex_ids)

    # Build query overrides
    override = {
        k: v
        for k, v in {
            "wos": args.query_wos,
            "scopus": args.query_scopus,
            "crossref": args.query_crossref,
            "openalex": args.query_openalex,
            "zenodo": args.query_zenodo,
            "epo": args.query_epo,
        }.items()
        if v is not None
    }

    # Si des IDs sont fournis, on fabrique des queries par identifiant
    if scopus_ids or wos_ids or orcid_ids or openalex_ids:
        id_q = build_id_queries(scopus_ids, wos_ids, orcid_ids, openalex_ids)
        # Remplace uniquement les sources concernées
        override.update(id_q)
        logger.info(
            "ID-based harvesting active for: "
            + ", ".join(sorted(id_q.keys()))  # ex: scopus,wos,crossref,openalex
        )

    queries = ensure_queries(override)

    output_dir = Path(args.output_dir).resolve()

    # Fusion des IDs pour AuthorProcessor.author_ids_to_check (legacy + nouveaux)
    author_ids_for_enrichment = merge_unique(
        parse_author_ids(args.author_ids), scopus_ids, wos_ids, orcid_ids, openalex_ids
    )

    selected_sources = parse_sources(args.sources, logger=logger)
    if not selected_sources:
        parser.error(
            "No valid sources selected. Use --sources with one or more of: "
            + ", ".join(SUPPORTED_SOURCES)
        )

    try:
        run_pipeline(
            logger=logger,
            start_date=start_date,
            end_date=end_date,
            queries=queries,
            author_ids=author_ids_for_enrichment,
            output_dir=output_dir,
            dry_run=args.dry_run,
            no_email=args.no_email,
            sources=selected_sources,
            run_id=args.run_id,
        )
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Pipeline crashed: {e}")
        sys.exit(1)
    finally:
        # Explicitly remove the UI run-lock so the live-log while loop in
        # the Streamlit UI exits promptly, even when this process becomes a
        # zombie before the UI calls os.kill() on it again.
        try:
            env_loader.run_lock_path().unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()

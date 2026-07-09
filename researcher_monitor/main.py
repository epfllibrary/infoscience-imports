"""Researcher Monitor CLI — registry sync, publication harvest, gap analysis.

Usage:
    python -m researcher_monitor.main --action sync [--env dev]
    python -m researcher_monitor.main --action harvest [--sciper 349140,120091] [--start-year 2020]
    python -m researcher_monitor.main --action analyze [--sciper 349140] [--use-cached]
    python -m researcher_monitor.main --action all [--sciper 349140]

Actions:
    sync     — sync researcher registry from EPFL People API (discover new + offboard departed)
    refresh  — re-enrich existing registry entries by SCIPER (no discovery, no offboarding)
    harvest  — harvest publications from OpenAlex / ORCID for active researchers
    analyze  — run gap analysis (compare harvested pubs vs Infoscience)
    all      — sync + harvest + analyze in sequence
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _setup_logger(verbose: bool = False) -> None:
    """Attach a stdout StreamHandler to the pipeline logger hierarchy.

    researcher_monitor runs as a subprocess whose stdout/stderr are redirected
    to a log file by the UI launcher — a StreamHandler on sys.stdout is enough.
    """
    root = logging.getLogger("pipeline")
    root.setLevel(logging.DEBUG)
    root.propagate = False
    if not root.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG if verbose else logging.INFO)
        ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))
        root.addHandler(ch)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Researcher Monitor — registry sync and gap analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--action",
        required=True,
        choices=["sync", "refresh", "harvest", "analyze", "import", "all"],
        help="Action to run",
    )
    p.add_argument("--sciper", default=None, help="Comma-separated SCIPER(s) (default: all active)")
    p.add_argument("--run-id", default=None, dest="run_id", help="Explicit run ID for import action")
    p.add_argument("--env", default="dev", help="Environment: dev / test / prod")
    p.add_argument("--start-year", type=int, default=None, help="Harvest start year")
    p.add_argument("--end-year", type=int, default=None, help="Harvest end year")
    p.add_argument("--class-id", default=None, help="Comma-separated EPFL accred class IDs (default from registry)")
    p.add_argument("--position-id", default=None, help="EPFL accred position ID (default from registry)")
    p.add_argument("--status-id", default=None, help="EPFL accred status filter: 1=internal, 2=hosted (default: all)")
    p.add_argument("--no-orcid", action="store_true", help="Skip ORCID enrichment in sync")
    p.add_argument("--no-dspace", action="store_true", help="Skip Infoscience profile enrichment in sync")
    p.add_argument("--no-openalex", action="store_true", help="Skip OpenAlex DOI inference in sync")
    p.add_argument("--use-cached", action="store_true", help="Use cached Infoscience outputs in analyze")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def _get_scipers(args, db) -> list[str]:
    if args.sciper:
        return [s.strip() for s in args.sciper.split(",") if s.strip()]
    return db.get_active_scipers()


def _action_sync(args, db) -> None:
    from researcher_monitor.registry import RegistrySync
    from clients.dspace_client_wrapper import DSpaceClientWrapper
    from clients.openalex_client import OpenAlexClient

    try:
        dspace_client = DSpaceClientWrapper()
    except Exception as exc:
        print(f"  Warning: DSpace client unavailable — Infoscience enrichment skipped ({exc})")
        dspace_client = None

    openalex_client = OpenAlexClient

    sync = RegistrySync(db=db, dspace_client=dspace_client, openalex_client=openalex_client)
    enrich_orcid    = not args.no_orcid
    enrich_dspace   = not args.no_dspace
    enrich_openalex = not args.no_openalex

    if args.sciper:
        for sciper in _get_scipers(args, db):
            result = sync.sync_one(
                sciper,
                enrich_orcid=enrich_orcid,
                enrich_dspace=enrich_dspace,
                enrich_openalex=enrich_openalex,
            )
            if result:
                print(f"  synced {sciper}: {result.get('full_name') or result.get('last_name')}")
            else:
                print(f"  {sciper}: not found in EPFL API")
    else:
        sync_kwargs = {
            "enrich_orcid": enrich_orcid,
            "enrich_dspace": enrich_dspace,
            "enrich_openalex": enrich_openalex,
        }
        if args.class_id:
            sync_kwargs["class_id"] = args.class_id
        if args.position_id:
            sync_kwargs["position_id"] = args.position_id
        if args.status_id:
            sync_kwargs["status_id"] = args.status_id
        scipers = sync.sync_all(**sync_kwargs)
        print(f"Registry sync complete: {len(scipers)} researchers upserted")


def _action_refresh(args, db) -> None:
    """Re-enrich existing registry entries by SCIPER (no discovery, no offboarding).

    Calls sync_one for every active SCIPER currently in researcher_registry.
    Unlike sync_all, this never adds new researchers and never offboards anyone.
    Useful for refreshing ORCID, DSpace, and OpenAlex data without a full API discovery run.
    """
    import logging
    from researcher_monitor.registry import RegistrySync
    from clients.dspace_client_wrapper import DSpaceClientWrapper
    from clients.openalex_client import OpenAlexClient

    log = logging.getLogger("pipeline.researcher_monitor.main")

    try:
        dspace_client = DSpaceClientWrapper()
    except Exception as exc:
        log.warning("DSpace client unavailable — Infoscience enrichment skipped (%s)", exc)
        dspace_client = None

    openalex_client = OpenAlexClient

    sync = RegistrySync(db=db, dspace_client=dspace_client, openalex_client=openalex_client)
    enrich_orcid    = not args.no_orcid
    enrich_dspace   = not args.no_dspace
    enrich_openalex = not args.no_openalex

    scipers = _get_scipers(args, db)
    if not scipers:
        log.warning("No active researchers in registry — run sync first.")
        return

    log.info(
        "Refresh start — %d researcher(s) in registry (orcid=%s dspace=%s openalex=%s)",
        len(scipers), enrich_orcid, enrich_dspace, enrich_openalex,
    )
    updated = 0
    not_found = 0
    for i, sciper in enumerate(scipers, 1):
        log.info("[%d/%d] Refreshing sciper %s …", i, len(scipers), sciper)
        result = sync.sync_one(
            sciper,
            enrich_orcid=enrich_orcid,
            enrich_dspace=enrich_dspace,
            enrich_openalex=enrich_openalex,
        )
        if result:
            updated += 1
            log.debug("  %s: %s", sciper, result.get("full_name") or result.get("last_name"))
        else:
            not_found += 1
            log.warning("  %s: not found in EPFL API (record kept as-is)", sciper)

    log.info(
        "Refresh complete — %d updated, %d not found in EPFL API",
        updated, not_found,
    )


def _epfl_year(dt_val) -> int | None:
    """Extract year from a date value (datetime, date, or ISO string). Returns None if absent."""
    if dt_val is None:
        return None
    s = str(dt_val).strip()
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _clamp_years(
    user_start: int | None,
    user_end: int | None,
    enrollment_date,
    offboarding_date,
) -> tuple[int | None, int | None]:
    """Clamp user-supplied year range to the researcher's EPFL tenure window.

    start = max(user_start, enrollment_year)  — never harvest before the researcher joined
    end   = min(user_end,   offboarding_year) — never harvest after the researcher left
    """
    enrollment_year  = _epfl_year(enrollment_date)
    offboarding_year = _epfl_year(offboarding_date)

    eff_start = user_start
    if enrollment_year is not None and (eff_start is None or enrollment_year > eff_start):
        eff_start = enrollment_year

    eff_end = user_end
    if offboarding_year is not None and (eff_end is None or offboarding_year < eff_end):
        eff_end = offboarding_year

    return eff_start, eff_end


def _action_harvest(args, db) -> None:
    import duckdb
    from researcher_monitor.person_harvester import PersonHarvester

    scipers = _get_scipers(args, db)
    if not scipers:
        print("No active researchers found — run sync first.")
        return

    con = duckdb.connect(db.db_path)
    rows = con.execute(
        "SELECT sciper, openalex_id, orcid, enrollment_date, offboarding_date "
        "FROM researcher_registry WHERE sciper IN (%s)"
        % ",".join(["?"] * len(scipers)),
        scipers,
    ).fetchall()
    con.close()

    harvester = PersonHarvester()
    total_new = 0
    for sciper, openalex_id, orcid, enrollment_date, offboarding_date in rows:
        eff_start, eff_end = _clamp_years(
            args.start_year, args.end_year, enrollment_date, offboarding_date
        )
        if args.verbose:
            clamped = (eff_start != args.start_year) or (eff_end != args.end_year)
            if clamped:
                print(
                    f"  {sciper}: year window clamped to "
                    f"{eff_start or '—'}–{eff_end or '—'} "
                    f"(enrollment={_epfl_year(enrollment_date)}, "
                    f"offboarding={_epfl_year(offboarding_date)})"
                )

        researcher = {"sciper": sciper, "openalex_id": openalex_id, "orcid": orcid}
        pubs = harvester.harvest(researcher, start_year=eff_start, end_year=eff_end)

        new_count = 0
        existing_ids = {p["pub_id"] for p in db.get_person_publications(sciper)}
        for pub in pubs:
            is_new = pub["pub_id"] not in existing_ids
            db.upsert_person_publication(
                sciper=sciper,
                pub_id=pub["pub_id"],
                doi=pub.get("doi"),
                doi_canonical=pub.get("doi_canonical"),
                title=pub.get("title"),
                pub_year=pub.get("pub_year"),
                dc_type=pub.get("dc_type"),
                journal_title=pub.get("journal_title"),
                sources_found=pub.get("sources_found"),
                primary_source=pub.get("primary_source"),
                orcid_infoscience_synced=bool(pub.get("orcid_infoscience_synced", False)),
                has_preprint_version=bool(pub.get("has_preprint_version", False)),
            )
            if is_new:
                new_count += 1
        total_new += new_count
        if args.verbose or pubs:
            print(f"  {sciper}: {len(pubs)} pubs ({new_count} new)")

    print(f"Harvest complete: {total_new} new publications across {len(rows)} researchers")


def _action_analyze(args, db) -> None:
    import logging
    from researcher_monitor.gap_analyzer import GapAnalyzer

    log = logging.getLogger("pipeline.researcher_monitor.main")

    scipers = _get_scipers(args, db)
    if not scipers:
        log.warning("No active researchers found — run sync first.")
        return

    log.info(
        "Gap analysis starting — %d researcher(s)%s",
        len(scipers),
        " [use-cached]" if args.use_cached else "",
    )

    analyzer = GapAnalyzer(db=db)
    total_missing = 0
    for i, sciper in enumerate(scipers, 1):
        log.info("[%d/%d] Analyzing sciper %s …", i, len(scipers), sciper)
        result = analyzer.analyze(sciper, use_cached=args.use_cached)
        total_missing += result["missing"]
        log.info(
            "  sciper %s — harvested: %d  in Infoscience: %d  missing: %d",
            sciper, result["total"], result["in_infoscience"], result["missing"],
        )

    log.info(
        "Gap analysis complete — %d missing publication(s) across %d researcher(s)",
        total_missing, len(scipers),
    )


def _action_import(args, db) -> None:
    """Trigger a targeted import run for a single researcher."""
    import logging
    from pathlib import Path
    from researcher_monitor.import_trigger import ImportTrigger

    log = logging.getLogger("pipeline.researcher_monitor.main")

    if not args.sciper:
        log.error("--sciper is required for the import action (single SCIPER only).")
        return

    scipers = [s.strip() for s in args.sciper.split(",") if s.strip()]
    if len(scipers) != 1:
        log.error("import action accepts exactly one --sciper at a time.")
        return

    sciper = scipers[0]
    import duckdb
    con = duckdb.connect(db.db_path)
    rows = con.execute(
        "SELECT sciper, full_name, openalex_id, scopus_author_id, "
        "enrollment_date, offboarding_date "
        "FROM researcher_registry WHERE sciper = ?",
        [sciper],
    ).fetchall()
    con.close()

    if not rows:
        log.error("sciper %s not found in researcher_registry.", sciper)
        return

    row = dict(zip(
        ["sciper", "full_name", "openalex_id", "scopus_author_id",
         "enrollment_date", "offboarding_date"],
        rows[0],
    ))

    trigger = ImportTrigger()
    if not trigger.can_trigger(row):
        log.error(
            "sciper %s has no openalex_id or scopus_author_id — cannot trigger import.",
            sciper,
        )
        return

    root = ROOT
    try:
        result = trigger.trigger(
            row=row, env=args.env, root=root, db=db, run_id=args.run_id,
        )
        log.info(
            "Import triggered for sciper %s — run_id=%s pid=%d",
            sciper, result["run_id"], result["pid"],
        )
    except RuntimeError as exc:
        log.error("Cannot trigger import: %s", exc)


def main() -> None:
    args = _parse_args()
    _setup_logger(verbose=args.verbose)

    import env_loader
    env_loader.load_env(args.env)

    from db.pipeline_db import PipelineDB
    db = PipelineDB()

    if args.action in ("sync", "all"):
        _action_sync(args, db)
    if args.action == "refresh":
        _action_refresh(args, db)
    if args.action in ("harvest", "all"):
        _action_harvest(args, db)
    if args.action in ("analyze", "all"):
        _action_analyze(args, db)
    if args.action == "import":
        _action_import(args, db)


if __name__ == "__main__":
    main()

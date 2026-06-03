"""Infoscience status sync — checks and updates imported item statuses.

Queries the database for workspace/workflow items from completed, reviewed runs
(collected within the last N months) that require a status or quality re-check,
then calls the DSpace API item by item to resolve their current state.

Terminal statuses (not rechecked on subsequent runs):
  withdrawn, deleted, rejected

Conditionally re-checked until the N-month window expires:
  published — re-checked while quality_abstract_ok=FALSE or quality_pdf_ok=FALSE,
               so that abstracts or OA PDFs added after publication are detected.

Non-terminal (always rechecked):
  still_pending, NULL (not yet checked)

Returns a summary dict: {checked, updated, errors, skipped}.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("pipeline.infoscience_sync")


def run_sync(
    db_path: "str | Path | None" = None,
    months: int = 3,
    run_id: "str | None" = None,
) -> dict:
    """Check and update Infoscience statuses for eligible imported items.

    Args:
        db_path: Path to the DuckDB database. Defaults to the active environment DB.
        months:  Only check items imported within the last N months.
        run_id:  Restrict the sync to a single run (optional).

    Returns:
        dict with keys: checked, updated, errors, skipped
    """
    from db.pipeline_db import PipelineDB
    from clients.dspace_client_wrapper import DSpaceClientWrapper

    db = PipelineDB(db_path)
    pending = db.get_pending_status_checks(months=months, run_id=run_id)

    summary = {"checked": 0, "updated": 0, "errors": 0, "skipped": 0}

    if pending.empty:
        logger.info("Infoscience sync: no pending items to check.")
        return summary

    logger.info("Infoscience sync: %d items to check.", len(pending))

    try:
        client = DSpaceClientWrapper()
    except Exception as exc:
        logger.error("Infoscience sync: cannot connect to DSpace — %s", exc)
        summary["errors"] = len(pending)
        return summary

    _CC_PREFIXES = ("cc-", "public-domain", "pd")

    for _, row in pending.iterrows():
        run_id  = row.get("run_id")
        pub_id  = row.get("pub_id")
        uuid    = row.get("dspace_item_uuid")
        ws_id   = row.get("workspace_id")
        wf_id   = row.get("workflow_id")
        license_val = str(row.get("upw_license") or "").lower().strip()
        is_cc = any(license_val.startswith(p) for p in _CC_PREFIXES)

        if not (uuid or ws_id or wf_id):
            summary["skipped"] += 1
            continue

        summary["checked"] += 1
        try:
            status, handle, quality = client.check_item_infoscience_status(
                uuid, ws_id, wf_id, check_quality=True,
            )
            db.update_infoscience_status(run_id, pub_id, status, handle)
            summary["updated"] += 1
            logger.debug(
                "Infoscience sync: run=%s pub=%s → %s",
                run_id, str(pub_id)[:40], status,
            )
            if status == "published" and quality is not None:
                pdf_ok = quality["pdf_ok"] if is_cc else None
                db.update_quality_checks(
                    run_id, pub_id, quality["abstract_ok"], pdf_ok,
                )
                logger.debug(
                    "Infoscience quality: run=%s pub=%s abstract=%s pdf=%s",
                    run_id, str(pub_id)[:40], quality["abstract_ok"], pdf_ok,
                )
        except Exception as exc:
            logger.error(
                "Infoscience sync error for run=%s pub=%s: %s",
                run_id, str(pub_id)[:40], exc,
            )
            summary["errors"] += 1

    logger.info(
        "Infoscience sync complete: checked=%d updated=%d errors=%d skipped=%d",
        summary["checked"], summary["updated"], summary["errors"], summary["skipped"],
    )
    return summary

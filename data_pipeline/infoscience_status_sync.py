"""Infoscience status sync — checks and updates imported item statuses.

Queries the database for workspace/workflow items from completed, reviewed runs
(collected within the last N months) that have not yet reached a terminal status,
then calls the DSpace API item by item to resolve their current state.

Terminal statuses (not rechecked on subsequent runs):
  published, withdrawn, deleted, rejected

Non-terminal:
  still_pending, NULL (not yet checked)

Returns a summary dict: {checked, updated, errors, skipped}.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("pipeline.infoscience_sync")


def run_sync(db_path: "str | Path | None" = None, months: int = 3) -> dict:
    """Check and update Infoscience statuses for eligible imported items.

    Args:
        db_path: Path to the DuckDB database. Defaults to the active environment DB.
        months:  Only check items imported within the last N months.

    Returns:
        dict with keys: checked, updated, errors, skipped
    """
    from db.pipeline_db import PipelineDB
    from clients.dspace_client_wrapper import DSpaceClientWrapper

    db = PipelineDB(db_path)
    pending = db.get_pending_status_checks(months=months)

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

    for _, row in pending.iterrows():
        run_id  = row.get("run_id")
        pub_id  = row.get("pub_id")
        uuid    = row.get("dspace_item_uuid")
        ws_id   = row.get("workspace_id")
        wf_id   = row.get("workflow_id")

        if not (uuid or ws_id or wf_id):
            summary["skipped"] += 1
            continue

        summary["checked"] += 1
        try:
            status, handle = client.check_item_infoscience_status(uuid, ws_id, wf_id)
            db.update_infoscience_status(run_id, pub_id, status, handle)
            summary["updated"] += 1
            logger.debug(
                "Infoscience sync: run=%s pub=%s → %s",
                run_id, str(pub_id)[:40], status,
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

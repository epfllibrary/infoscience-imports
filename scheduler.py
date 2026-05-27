#!/usr/bin/env python3
"""Background scheduler for the Infoscience Import Pipeline.

Reads ``data/schedules.json`` and fires pipeline runs according to each
schedule's cron expression.  Launched by ``run_ui.sh`` alongside Streamlit.

The JSON file is polled every RELOAD_INTERVAL seconds; any change (add,
edit, enable/disable, delete) is picked up automatically without restart.

Each run is launched as a subprocess (same as the manual UI) and the existing
per-environment run-lock is used to prevent concurrent executions.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from utils import make_run_id

ROOT            = Path(__file__).resolve().parent
SCHEDULES_FILE  = ROOT / "data" / "schedules.json"
PYTHON          = sys.executable
RELOAD_INTERVAL = 15          # seconds between mtime checks
TIMEZONE        = "Europe/Zurich"

(ROOT / "logs").mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [scheduler] %(levelname)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "logs" / "scheduler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("scheduler")


# ── Schedule file I/O ─────────────────────────────────────────────────────────

def _read_json() -> dict:
    if not SCHEDULES_FILE.exists():
        return {}
    try:
        return json.loads(SCHEDULES_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("Cannot read schedules.json: %s", exc)
        return {}


def _read_schedules() -> list[dict]:
    return _read_json().get("schedules", [])


def _write_json(data: dict) -> None:
    """Atomically write the full schedules.json, preserving all top-level keys."""
    SCHEDULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, ensure_ascii=False)
    tmp = Path(tempfile.mktemp(dir=SCHEDULES_FILE.parent, suffix=".tmp"))
    try:
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(SCHEDULES_FILE)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _write_schedules(schedules: list[dict]) -> None:
    data = _read_json()
    data["schedules"] = schedules
    _write_json(data)


def _patch_schedule(schedule_id: str, **fields) -> None:
    """Atomically update one schedule's fields in the JSON file."""
    schedules = _read_schedules()
    for s in schedules:
        if s["id"] == schedule_id:
            s.update(fields)
    _write_schedules(schedules)


def _patch_system_job(key: str, **fields) -> None:
    """Atomically update a system job's fields in the JSON file."""
    data = _read_json()
    data.setdefault("system_jobs", {}).setdefault(key, {}).update(fields)
    _write_json(data)


# ── Run helpers ───────────────────────────────────────────────────────────────

def _state_file(env: str) -> Path:
    return ROOT / "data" / f"run_active_{env}.json"


def _build_cmd(schedule: dict, run_id: str) -> list[str]:
    cmd = [PYTHON, str(ROOT / "data_pipeline" / "main.py")]
    cmd += ["--window-days", str(schedule.get("window_days", 15))]
    sources = schedule.get("sources") or []
    if sources:
        cmd += ["--sources", ",".join(sources)]
    cmd += ["--env",    schedule.get("env", "dev")]
    cmd += ["--run-id", run_id]
    if schedule.get("dry_run"):
        cmd.append("--dry-run")
    if schedule.get("no_email", True):
        cmd.append("--no-email")
    return cmd


# ── Job function (called by APScheduler in a thread) ─────────────────────────

def run_job(schedule_id: str) -> None:
    """Execute one scheduled pipeline run."""
    # Always re-read the schedule so edits made after scheduling take effect.
    schedules = _read_schedules()
    schedule  = next((s for s in schedules if s["id"] == schedule_id), None)
    if not schedule:
        logger.warning("Schedule %s not found — skipping", schedule_id)
        return
    if not schedule.get("enabled"):
        logger.info("Schedule %s is disabled — skipping", schedule_id)
        return

    env     = schedule.get("env", "dev")
    run_id  = make_run_id(schedule.get("name", ""))
    sf      = _state_file(env)
    log_file = ROOT / "logs" / f"run_{run_id}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    cmd      = _build_cmd(schedule, run_id)
    sources  = schedule.get("sources") or []
    dry_run  = bool(schedule.get("dry_run"))

    t_start = datetime.now()
    logger.info("Scheduled run %s starting (schedule %r)", run_id, schedule.get("name", schedule_id))

    # Acquire run lock (exclusive file creation — POSIX atomic)
    lock_payload = json.dumps({
        "run_id":     run_id,
        "pid":        0,
        "env":        env,
        "started_at": datetime.now().isoformat(),
        "sources":    sources,
        "dry_run":    dry_run,
        "log_file":   str(log_file),
        "cmd":        " ".join(cmd),
    }, indent=2)
    try:
        with open(sf, "x", encoding="utf-8") as fh:
            fh.write(lock_payload)
    except FileExistsError:
        logger.warning(
            "Skipping scheduled run %s: run lock %s already exists", run_id, sf
        )
        return

    _patch_schedule(schedule_id,
                    last_run_at=datetime.now().isoformat(),
                    last_run_id=run_id,
                    last_run_status="running")
    status = "failed"
    try:
        # Load the .env.{env} file (falling back to .env) so API keys and other
        # credentials are available to the subprocess.  dotenv_values() reads
        # the file without touching os.environ — safe for concurrent threads.
        _env_file = ROOT / f".env.{env}"
        if not _env_file.exists():
            _env_file = ROOT / ".env"
        _dotenv_vars = dict(dotenv_values(_env_file)) if _env_file.exists() else {}

        _subprocess_env = {
            **os.environ,
            **_dotenv_vars,
            "APP_ENV":    env,
            "PYTHONPATH": str(ROOT),
        }

        with open(log_file, "w", encoding="utf-8") as log_fh:
            proc = subprocess.Popen(
                cmd,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                cwd=str(ROOT),
                env=_subprocess_env,
            )
        # Update lock with real PID
        sf.write_text(
            json.dumps({
                "run_id":     run_id,
                "pid":        proc.pid,
                "env":        env,
                "started_at": datetime.now().isoformat(),
                "sources":    sources,
                "dry_run":    dry_run,
                "log_file":   str(log_file),
                "cmd":        " ".join(cmd),
            }, indent=2),
            encoding="utf-8",
        )
        proc.wait()
        status = "completed" if proc.returncode == 0 else "failed"
    except Exception as exc:
        logger.error("Scheduled run %s raised: %s", run_id, exc)
    finally:
        sf.unlink(missing_ok=True)

    elapsed = int((datetime.now() - t_start).total_seconds())
    logger.info("Scheduled run %s finished in %ds — %s", run_id, elapsed, status)
    _patch_schedule(schedule_id, last_run_status=status)


# ── Scheduler lifecycle ───────────────────────────────────────────────────────

def _sync_jobs(scheduler: BackgroundScheduler) -> None:
    """Add, reschedule, or remove APScheduler jobs to match schedules.json."""
    schedules  = _read_schedules()
    wanted_ids = set()

    for s in schedules:
        sid = s.get("id", "")
        if not sid:
            continue
        if not s.get("enabled"):
            if scheduler.get_job(sid):
                scheduler.remove_job(sid)
            continue

        cron_expr = (s.get("cron") or "").strip()
        if not cron_expr:
            continue
        try:
            trigger = CronTrigger.from_crontab(cron_expr, timezone=TIMEZONE)
        except Exception as exc:
            logger.error("Invalid cron %r for schedule %s: %s", cron_expr, sid, exc)
            continue

        wanted_ids.add(sid)
        if scheduler.get_job(sid):
            scheduler.reschedule_job(sid, trigger=trigger)
        else:
            scheduler.add_job(
                run_job,
                trigger=trigger,
                args=[sid],
                id=sid,
                replace_existing=True,
                name=s.get("name", sid),
                misfire_grace_time=600,
                coalesce=True,
            )
            logger.info(
                "Registered job %r (id=%s  cron=%r)", s.get("name", sid), sid, cron_expr
            )

    for job in scheduler.get_jobs():
        if job.id not in wanted_ids and job.id != _INFOSCIENCE_SYNC_JOB_ID:
            scheduler.remove_job(job.id)
            logger.info("Removed job %s", job.id)


_INFOSCIENCE_SYNC_JOB_KEY = "infoscience_sync"
_INFOSCIENCE_SYNC_JOB_ID  = "nightly_infoscience_sync"


def run_infoscience_sync() -> None:
    """Nightly job: check and update Infoscience statuses for imported items."""
    if not _read_json().get("system_jobs", {}).get(_INFOSCIENCE_SYNC_JOB_KEY, {}).get("enabled", True):
        logger.info("Infoscience status sync is disabled — skipping.")
        return

    logger.info("Infoscience status sync starting.")
    last_status = "failed"
    try:
        from data_pipeline.infoscience_status_sync import run_sync
        summary = run_sync()
        last_status = "completed"
        logger.info(
            "Infoscience status sync done: checked=%d updated=%d errors=%d skipped=%d",
            summary.get("checked", 0), summary.get("updated", 0),
            summary.get("errors", 0), summary.get("skipped", 0),
        )
    except Exception as exc:
        logger.error("Infoscience status sync failed: %s", exc)
    finally:
        _patch_system_job(
            _INFOSCIENCE_SYNC_JOB_KEY,
            last_run_at=datetime.now().isoformat(),
            last_run_status=last_status,
        )


def main() -> None:
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    _sync_jobs(scheduler)

    # Fixed nightly infoscience status sync — not user-configurable.
    scheduler.add_job(
        run_infoscience_sync,
        trigger=CronTrigger(hour=2, minute=30, timezone=TIMEZONE),
        id=_INFOSCIENCE_SYNC_JOB_ID,
        replace_existing=True,
        name="Nightly Infoscience status sync",
        misfire_grace_time=3600,
        coalesce=True,
    )
    logger.info("Registered nightly Infoscience sync job (02:30 %s)", TIMEZONE)

    scheduler.start()
    logger.info("Scheduler started — polling %s every %ds", SCHEDULES_FILE, RELOAD_INTERVAL)

    last_mtime: float = SCHEDULES_FILE.stat().st_mtime if SCHEDULES_FILE.exists() else 0.0
    try:
        while True:
            time.sleep(RELOAD_INTERVAL)
            mtime = SCHEDULES_FILE.stat().st_mtime if SCHEDULES_FILE.exists() else 0.0
            if mtime != last_mtime:
                last_mtime = mtime
                logger.info("schedules.json changed — reloading")
                _sync_jobs(scheduler)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()

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

def _read_schedules() -> list[dict]:
    if not SCHEDULES_FILE.exists():
        return []
    try:
        return json.loads(SCHEDULES_FILE.read_text(encoding="utf-8")).get("schedules", [])
    except Exception as exc:
        logger.error("Cannot read schedules.json: %s", exc)
        return []


def _write_schedules(schedules: list[dict]) -> None:
    SCHEDULES_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"schedules": schedules}, indent=2, ensure_ascii=False)
    tmp = Path(tempfile.mktemp(dir=SCHEDULES_FILE.parent, suffix=".tmp"))
    try:
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(SCHEDULES_FILE)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _patch_schedule(schedule_id: str, **fields) -> None:
    """Atomically update one schedule's fields in the JSON file."""
    schedules = _read_schedules()
    for s in schedules:
        if s["id"] == schedule_id:
            s.update(fields)
    _write_schedules(schedules)


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

    logger.info("Scheduled run %s finished — %s", run_id, status)
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
        if job.id not in wanted_ids:
            scheduler.remove_job(job.id)
            logger.info("Removed job %s", job.id)


def main() -> None:
    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    _sync_jobs(scheduler)
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

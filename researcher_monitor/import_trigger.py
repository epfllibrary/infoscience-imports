"""ImportTrigger — build and launch a targeted researcher import run (M5).

The import uses OpenAlex + Scopus sources only, targets the researcher by their
external IDs, and passes pre-rejected pub_ids as exclusions.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from utils import get_pipeline_logger

logger = get_pipeline_logger("researcher_monitor.import_trigger")


def _nn(v: object) -> bool:
    return v is not None and str(v).strip() not in ("", "nan", "None", "NaT")


def _extract_year(dt_val) -> int | None:
    """Extract the year from a date, datetime, or ISO string. Returns None if absent."""
    if dt_val is None:
        return None
    s = str(dt_val).strip()
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _accreditation_window(row: dict) -> tuple[str | None, str | None]:
    """Return (start_date, end_date) ISO strings based on accreditation dates.

    start = Jan 1st of enrollment year.
    end   = Dec 31st of offboarding year, or today when still active.
    Returns (None, None) when enrollment_date is absent.
    """
    enrollment_year  = _extract_year(row.get("enrollment_date"))
    offboarding_year = _extract_year(row.get("offboarding_date"))

    if enrollment_year is None:
        return None, None

    start = f"{enrollment_year}-01-01"
    end   = (
        f"{offboarding_year}-12-31"
        if offboarding_year is not None
        else date.today().isoformat()
    )
    return start, end


class ImportTrigger:
    """Build and launch a targeted import pipeline run for a single researcher."""

    def can_trigger(self, row: dict) -> bool:
        """Return True when the researcher has at least one harvestable external ID."""
        return (
            _nn(row.get("openalex_id"))
            or _nn(row.get("scopus_author_id"))
            or _nn(row.get("orcid"))
        )

    def build_pipeline_cmd(
        self,
        row: dict,
        env: str,
        root: Path,
        run_id: str,
        rejected_pub_ids: list[str] | None = None,
        override_start_year: int | None = None,
    ) -> list[str]:
        """Build the command list for a researcher_import pipeline run.

        Args:
            row:              Researcher registry row dict.
            env:              Active environment (dev / test / prod).
            root:             Project root path.
            run_id:           Run ID (used for log correlation).
            rejected_pub_ids: pub_ids marked as 'rejected' by the user — split
                              into DOIs (passed as --exclude-dois) and OpenAlex
                              work IDs (passed as --exclude-openalex-ids).

        Returns:
            List of strings suitable for subprocess.Popen.
        """
        sciper    = str(row.get("sciper", ""))
        fullname  = str(row.get("full_name", ""))
        oa_id     = str(row.get("openalex_id") or "").strip()
        scopus_id = str(row.get("scopus_author_id") or "").strip()
        orcid     = str(row.get("orcid") or "").strip()

        _sources = []
        if oa_id or orcid:
            _sources.append("openalex")
        if scopus_id or orcid:
            _sources.append("scopus")

        cmd = [sys.executable, str(root / "data_pipeline" / "main.py")]
        cmd += ["--sources", ",".join(_sources)]
        cmd += ["--run-type", "researcher_import"]
        cmd += ["--forced-sciper", f"{sciper}:{fullname}"]
        cmd += ["--run-id", run_id]
        cmd += ["--env", env]
        cmd += ["--no-email"]

        start_date, end_date = _accreditation_window(row)
        if override_start_year is not None:
            start_date = f"{override_start_year}-01-01"
            if end_date is None:
                end_date = date.today().isoformat()
        if start_date and end_date:
            cmd += ["--start-date", start_date, "--end-date", end_date]

        if oa_id:
            cmd += ["--openalex-ids", oa_id]
        if scopus_id:
            cmd += ["--scopus-ids", scopus_id]
        if orcid:
            cmd += ["--orcid-ids", orcid]

        rejected = rejected_pub_ids or []
        _dois    = []
        _oa_ids  = []
        _titles  = []
        for r in rejected:
            if r.get("doi"):
                _dois.append(r["doi"])
            elif r["pub_id"].startswith("W") and "/" not in r["pub_id"]:
                _oa_ids.append(r["pub_id"])
            elif r.get("title"):
                _titles.append(f"{r['title']}||{r.get('pub_year', '')}")
        if _dois:
            cmd += ["--exclude-dois", ",".join(_dois)]
        if _oa_ids:
            cmd += ["--exclude-openalex-ids", ",".join(_oa_ids)]
        if _titles:
            cmd += ["--exclude-titles", ",".join(_titles)]

        return cmd

    def trigger(
        self,
        row: dict,
        env: str,
        root: Path,
        db,
        run_id: str | None = None,
        log_file: Path | None = None,
        override_start_year: int | None = None,
    ) -> dict:
        """Launch the import as a subprocess, acquire researcher job lock.

        Returns:
            dict with keys: run_id, pid, log_file.

        Raises:
            RuntimeError: if a pipeline run lock is already held.
            ValueError:   if the researcher cannot be triggered (no external IDs).
        """
        if not self.can_trigger(row):
            raise ValueError(
                f"sciper {row.get('sciper')} has no openalex_id or scopus_author_id — "
                "cannot trigger targeted import."
            )

        from db.pipeline_db import PipelineDB

        lock_file = root / "data" / f"run_active_{env}.json"
        if lock_file.exists():
            raise RuntimeError(
                f"A pipeline run is already active ({lock_file}). "
                "Wait for it to finish before triggering a new import."
            )

        sciper = str(row.get("sciper", ""))
        ts     = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        run_id = run_id or f"researcher_import_{sciper}_{ts}"

        if log_file is None:
            log_file = root / "logs" / f"run_{run_id}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        rejected_pub_ids = db.get_rejected_pub_ids(sciper)
        cmd = self.build_pipeline_cmd(
            row=row,
            env=env,
            root=root,
            run_id=run_id,
            rejected_pub_ids=rejected_pub_ids,
            override_start_year=override_start_year,
        )

        logger.info(
            "Launching researcher import for sciper %s — run_id=%s",
            sciper, run_id,
        )

        log_fh = open(log_file, "w", encoding="utf-8")
        proc   = subprocess.Popen(
            cmd, stdout=log_fh, stderr=subprocess.STDOUT, cwd=str(root),
        )
        log_fh.close()

        return {"run_id": run_id, "pid": proc.pid, "log_file": str(log_file)}

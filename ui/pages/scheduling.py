"""Scheduling page — CRUD for scheduled pipeline runs."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

from ui.helpers import missing_required_env, page_title, sh, _make_run_id
from ui.run_state import get_state_file, try_acquire_run_lock

try:
    from apscheduler.triggers.cron import CronTrigger as _CronTrigger
    _HAS_APSCHEDULER = True
except ImportError:
    _HAS_APSCHEDULER = False

_STATUS_ICON: dict[str | None, str] = {
    "completed": "✅", "running": "⏳", "failed": "❌", "killed": "🛑", None: "—",
}

_INFOSCIENCE_SYNC_JOB_KEY = "infoscience_sync"
_INFOSCIENCE_SYNC_CRON    = "30 2 * * *"
_INFOSCIENCE_SYNC_NAME    = "Synchronisation statuts Infoscience"

_CRON_PRESETS: dict[str, str] = {
    "Quotidien à 06:00":           "0 6 * * *",
    "Quotidien à 22:00":           "0 22 * * *",
    "Hebdomadaire (lun. 06:00)":   "0 6 * * 1",
    "Bi-hebdomadaire (lun.+jeu.)": "0 6 * * 1,4",
    "Toutes les 6 heures":         "0 */6 * * *",
    "Mensuel (1er du mois 06:00)": "0 6 1 * *",
    "Personnalisé…":               "",
}


def _render_infoscience_sync_card(sched_file: Path, root: Path, active_env: str) -> None:
    """Render the fixed Infoscience status sync system job card (no delete, fixed cron)."""
    job     = _load_system_jobs(sched_file).get(_INFOSCIENCE_SYNC_JOB_KEY, {})
    enabled = job.get("enabled", True)
    last_at = (job.get("last_run_at") or "—")[:16].replace("T", " ")
    last_icon = _STATUS_ICON.get(job.get("last_run_status"), "—")

    with st.container(border=True):
        _ca, _cb, _cc, _cd = st.columns([4, 3, 3, 2])
        with _ca:
            st.markdown(
                f"**{_INFOSCIENCE_SYNC_NAME}**  "
                f"<span style='background:#F1F5F9;color:#475569;border-radius:4px;"
                f"padding:1px 7px;font-size:.78rem;font-weight:700'>SYSTÈME</span>",
                unsafe_allow_html=True,
            )
            st.caption(
                f"Statuts post-import : published / rejected / deleted / still_pending.  |  "
                f"Cron : `{_INFOSCIENCE_SYNC_CRON}` (02h30 chaque nuit, non modifiable)"
            )
        with _cb:
            st.markdown(f"**Prochain run**  \n{_next_run_str(_INFOSCIENCE_SYNC_CRON)}")
        with _cc:
            st.markdown(f"**Dernier run**  \n{last_icon} {last_at}")
        with _cd:
            new_enabled = st.toggle("Actif", value=enabled, key="tog_ifs_sync")
            if new_enabled != enabled:
                _save_system_job(sched_file, _INFOSCIENCE_SYNC_JOB_KEY, {"enabled": new_enabled})
                st.rerun()
            if st.button("▶ Now", key="run_ifs_sync", use_container_width=True,
                         help="Lancer la synchronisation maintenant"):
                with st.spinner("Synchronisation en cours…"):
                    try:
                        from data_pipeline.infoscience_status_sync import run_sync
                        _r = run_sync(db_path=root / "data" / f"pipeline_{active_env}.duckdb")
                        _save_system_job(sched_file, _INFOSCIENCE_SYNC_JOB_KEY, {
                            "last_run_at": datetime.now().isoformat(),
                            "last_run_status": "completed",
                        })
                        st.success(
                            f"{_r['checked']} vérifiés · "
                            f"{_r['updated']} mis à jour · "
                            f"{_r['errors']} erreurs"
                        )
                    except Exception as _exc:
                        _save_system_job(sched_file, _INFOSCIENCE_SYNC_JOB_KEY, {
                            "last_run_at": datetime.now().isoformat(),
                            "last_run_status": "failed",
                        })
                        st.error(f"Erreur : {_exc}")


def render(active_env: str, root: Path, sources: list[str], username: str) -> None:
    """Render the scheduling page — CRUD for scheduled runs and scheduler status."""
    page_title("schedule", "Programmation des runs")

    sched_file = root / "data" / "schedules.json"
    schedules = _load(sched_file)

    _render_scheduler_status(root)

    if schedules:
        st.markdown(sh("event_repeat", "Schedules configurés"), unsafe_allow_html=True)
        for sched in schedules:
            schedules = _render_schedule_card(
                sched, schedules, sched_file, active_env, root, sources
            )

    else:
        st.info("Aucun schedule configuré. Créez-en un ci-dessous.")

    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("➕ Nouveau schedule", expanded=not schedules):
        _render_new_schedule_form(schedules, sched_file, active_env, sources, username)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(sh("settings_applications", "Tâches système"), unsafe_allow_html=True)
    _render_infoscience_sync_card(sched_file, root, active_env)

    sched_log = root / "logs" / "scheduler.log"
    if sched_log.exists():
        with st.expander("Logs du scheduler (50 dernières lignes)"):
            _log_lines = sched_log.read_text(encoding="utf-8", errors="replace").splitlines()
            st.code("\n".join(_log_lines[-50:]), language=None)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("schedules", [])
    except Exception:
        return []


def _save(path: Path, schedules: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    existing["schedules"] = schedules
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    tmp = Path(tmp_path)
    try:
        os.close(fd)
        tmp.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _load_system_jobs(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("system_jobs", {})
    except Exception:
        return {}


def _save_system_job(path: Path, key: str, updates: dict) -> None:
    existing: dict = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    existing.setdefault("system_jobs", {}).setdefault(key, {}).update(updates)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    tmp = Path(tmp_path)
    try:
        os.close(fd)
        tmp.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _next_run_str(cron_expr: str) -> str:
    if not _HAS_APSCHEDULER:
        return "—"
    try:
        trig = _CronTrigger.from_crontab(cron_expr.strip(), timezone="Europe/Zurich")
        nxt  = trig.get_next_fire_time(None, datetime.now(timezone.utc))
        return nxt.strftime("%Y-%m-%d %H:%M") if nxt else "—"
    except Exception:
        return "⚠ expression invalide"


def _render_scheduler_status(root: Path) -> None:
    is_alive = False
    try:
        import psutil
        for proc in psutil.process_iter(["pid", "cmdline"]):
            if proc.info["cmdline"] and "scheduler.py" in " ".join(proc.info["cmdline"]):
                is_alive = True
                break
    except ImportError:
        pass

    if is_alive:
        st.success("🟢 Scheduler en cours d'exécution", icon=None)
    else:
        st.info(
            "⚪ Scheduler non détecté. Lancez l'UI via `./run_ui.sh` pour activer "
            "l'exécution automatique des schedules.",
            icon=None,
        )


def _render_schedule_card(
    sched: dict,
    schedules: list[dict],
    sched_file: Path,
    active_env: str,
    root: Path,
    sources: list[str],
) -> list[dict]:
    """Render one schedule card and return the (possibly mutated) schedules list."""
    import os
    sid = sched["id"]
    env_badge_style = {
        "dev":  "background:#dff0c8;color:#3a5a10",
        "test": "background:#fdefd5;color:#7a4400",
        "prod": "background:#ffd5d5;color:#7a0000",
    }.get(sched.get("env", "dev"), "")
    last_icon = _STATUS_ICON.get(sched.get("last_run_status"), "—")

    with st.container(border=True):
        _ca, _cb, _cc, _cd = st.columns([4, 3, 3, 2])
        with _ca:
            st.markdown(
                f"**{sched.get('name', sid)}**  "
                f"<span style='{env_badge_style};border-radius:4px;padding:1px 7px;"
                f"font-size:.78rem;font-weight:700'>{sched.get('env','dev').upper()}</span>",
                unsafe_allow_html=True,
            )
            st.caption(
                f"Sources : {', '.join(sched.get('sources') or [])}  |  "
                f"Fenêtre : {sched.get('window_days', 15)} j  |  "
                f"{'Dry-run  |  ' if sched.get('dry_run') else ''}"
                f"Cron : `{sched.get('cron', '')}`"
            )
        with _cb:
            st.markdown(f"**Prochain run**  \n{_next_run_str(sched.get('cron', ''))}")
        with _cc:
            last_at = (sched.get("last_run_at") or "—")[:16].replace("T", " ")
            st.markdown(
                f"**Dernier run**  \n{last_icon} {last_at}"
                + (f"  \n`{sched.get('last_run_id', '')}`" if sched.get("last_run_id") else "")
            )
        with _cd:
            new_enabled = st.toggle("Actif", value=bool(sched.get("enabled")), key=f"tog_{sid}")
            if new_enabled != bool(sched.get("enabled")):
                for x in schedules:
                    if x["id"] == sid:
                        x["enabled"] = new_enabled
                _save(sched_file, schedules)
                st.rerun()

            _btn1, _btn2 = st.columns(2)
            with _btn1:
                if st.button("▶ Now", key=f"run_{sid}", help="Lance ce run immédiatement",
                             use_container_width=True):
                    _sched_env = sched.get("env", "dev")
                    _missing = missing_required_env(_sched_env, root)
                    if _missing:
                        st.error(
                            f"⛔ Variables requises manquantes dans `.env.{_sched_env}` : "
                            f"`{'`, `'.join(_missing)}`"
                        )
                        st.stop()
                    now_id  = _make_run_id(sched.get("name", ""))
                    now_log = root / "logs" / f"run_{now_id}.log"
                    now_log.parent.mkdir(parents=True, exist_ok=True)
                    now_cmd = [
                        __import__("sys").executable,
                        str(root / "data_pipeline" / "main.py"),
                        "--window-days", str(sched.get("window_days", 15)),
                        "--env", _sched_env,
                        "--run-id", now_id,
                    ]
                    if sched.get("sources"):
                        now_cmd += ["--sources", ",".join(sched["sources"])]
                    if sched.get("dry_run"):
                        now_cmd.append("--dry-run")
                    if sched.get("no_email", True):
                        now_cmd.append("--no-email")
                    acquired = try_acquire_run_lock(
                        run_id=now_id, pid=0,
                        sources=sched.get("sources") or sources,
                        dry_run=bool(sched.get("dry_run")),
                        log_file=str(now_log), cmd=now_cmd,
                    )
                    if not acquired:
                        st.error("⛔ Un run est déjà en cours.")
                    else:
                        p = subprocess.Popen(
                            now_cmd,
                            stdout=open(now_log, "w"),
                            stderr=subprocess.STDOUT,
                            cwd=str(root),
                            env={**os.environ, "APP_ENV": _sched_env},
                        )
                        get_state_file().write_text(
                            json.dumps({
                                "run_id": now_id, "pid": p.pid,
                                "env": sched.get("env", "dev"),
                                "started_at": datetime.now().isoformat(),
                                "sources": sched.get("sources") or sources,
                                "dry_run": bool(sched.get("dry_run")),
                                "log_file": str(now_log),
                                "cmd": " ".join(now_cmd),
                            }, indent=2), encoding="utf-8",
                        )
                        st.success(f"Run `{now_id}` démarré.")
                        __import__("time").sleep(0.5)
                        st.rerun()

            with _btn2:
                if st.button("🗑", key=f"del_{sid}", help="Supprimer ce schedule",
                             use_container_width=True):
                    _save(sched_file, [x for x in schedules if x["id"] != sid])
                    st.rerun()

    return schedules


def _render_new_schedule_form(
    schedules: list[dict],
    sched_file: Path,
    active_env: str,
    sources: list[str],
    username: str,
) -> None:
    import env_loader

    preset_choice = st.selectbox(
        "Fréquence", list(_CRON_PRESETS.keys()), key="sched_preset",
        help="Sélectionnez un preset ou 'Personnalisé…' pour saisir une expression cron.",
    )
    cron_default = _CRON_PRESETS[preset_choice]
    if cron_default:
        st.caption(f"Prochain déclenchement : **{_next_run_str(cron_default)}**")

    with st.form("new_schedule_form", clear_on_submit=True):
        _fn1, _fn2 = st.columns(2)
        with _fn1:
            new_name = st.text_input("Nom", placeholder="Daily main run")
        with _fn2:
            new_env = st.selectbox(
                "Environnement",
                list(env_loader.ENVIRONMENTS),
                index=list(env_loader.ENVIRONMENTS).index(active_env),
            )

        new_sources = st.multiselect("Sources", sources, default=["scopus", "crossref", "openalex"])
        new_window  = st.number_input("Fenêtre glissante (jours)", min_value=1, max_value=365, value=20)
        new_cron    = st.text_input(
            "Expression cron", value=cron_default, placeholder="0 6 * * *",
            help="Format : minute heure jour_mois mois jour_semaine",
        )
        _fo1, _fo2 = st.columns(2)
        with _fo1:
            new_dry   = st.checkbox("Dry-run (sans import DSpace)", value=False)
        with _fo2:
            new_email = st.checkbox("Désactiver l'envoi d'e-mail", value=True)

        submitted = st.form_submit_button("Créer le schedule", use_container_width=True)

    if not submitted:
        return

    errors = []
    if not new_name.strip():
        errors.append("Le nom est obligatoire.")
    if not new_cron.strip():
        errors.append("L'expression cron est obligatoire.")
    elif _HAS_APSCHEDULER:
        try:
            _CronTrigger.from_crontab(new_cron.strip())
        except Exception:
            errors.append(f"Expression cron invalide : `{new_cron}`")
    for err in errors:
        st.error(err)
    if errors:
        return

    schedules.append({
        "id":              str(uuid.uuid4()),
        "name":            new_name.strip(),
        "enabled":         True,
        "sources":         new_sources or sources,
        "window_days":     int(new_window),
        "cron":            new_cron.strip(),
        "env":             new_env,
        "dry_run":         new_dry,
        "no_email":        new_email,
        "created_by":      username,
        "created_at":      datetime.now().isoformat(),
        "last_run_at":     None,
        "last_run_id":     None,
        "last_run_status": None,
    })
    _save(sched_file, schedules)
    st.success(
        f"Schedule **{new_name}** créé. Prochain run : {_next_run_str(new_cron.strip())}"
    )
    __import__("time").sleep(0.5)
    st.rerun()

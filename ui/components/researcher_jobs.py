"""researcher_jobs — job lock helpers and launch functions for researcher actions.

Extracted from ui/pages/researcher_monitor.py to keep the page as a pure
layout module. Covers: process lock management, job launching (sync / harvest /
analyze / import), live log view, and the manual action form.
"""

from __future__ import annotations

import html as _html
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st

from ui.helpers import sh


# ── Job lock file ─────────────────────────────────────────────────────────────

def job_lock_file(root: Path) -> Path:
    return root / "data" / "researcher_job_active.json"


def is_pid_running(pid: int) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        waited, _ = os.waitpid(pid, os.WNOHANG)
        if waited == pid:
            return False
    except (ChildProcessError, OSError):
        pass
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def read_active_researcher_job(root: Path) -> dict | None:
    lock = job_lock_file(root)
    if not lock.exists():
        return None
    try:
        data = json.loads(lock.read_text(encoding="utf-8"))
    except Exception:
        lock.unlink(missing_ok=True)
        return None
    pid = data.get("pid")
    if pid and is_pid_running(int(pid)):
        return data
    lock.unlink(missing_ok=True)
    return None


def write_researcher_job_lock(
    root: Path,
    action: str,
    pid: int,
    log_file: Path,
    cmd: list[str],
    env: str,
    sciper: str | None = None,
) -> None:
    lock = job_lock_file(root)
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(
        json.dumps({
            "action":     action,
            "pid":        pid,
            "log_file":   str(log_file),
            "started_at": datetime.now().isoformat(),
            "env":        env,
            "cmd":        " ".join(cmd),
            "sciper":     sciper,
        }, indent=2),
        encoding="utf-8",
    )


# ── Live log view ─────────────────────────────────────────────────────────────

def render_researcher_job_running(active_job: dict, root: Path) -> None:
    """Live log view for an active researcher job."""
    action   = active_job.get("action", "?")
    started  = (active_job.get("started_at") or "")[:16].replace("T", " ")
    log_file = Path(active_job.get("log_file", ""))
    pid      = active_job.get("pid")

    info_col, stop_col = st.columns([6, 2])
    with info_col:
        st.info(f"⏳ Tâche **{action}** en cours depuis {started}…")
    with stop_col:
        if pid and st.button("⛔ Arrêter", key="btn_stop_researcher", type="secondary"):
            try:
                os.kill(int(pid), signal.SIGTERM)
                st.warning("Signal d'arrêt envoyé au processus.")
                time.sleep(1)
                job_lock_file(root).unlink(missing_ok=True)
                st.rerun()
            except Exception as exc:
                st.error(f"Impossible d'arrêter le processus : {exc}")

    st.markdown(sh("terminal", "Logs en direct"), unsafe_allow_html=True)
    log_box  = st.empty()
    info_box = st.empty()

    while True:
        current = read_active_researcher_job(root)
        if log_file.exists():
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
            tail  = "\n".join(lines[-300:])
        else:
            tail = "(log non encore disponible…)"
        log_box.markdown(
            f'<div class="log-console">{_html.escape(tail)}</div>',
            unsafe_allow_html=True,
        )
        if current is None:
            info_box.empty()
            break
        info_box.caption("Actualisation dans 2 secondes…")
        time.sleep(2)

    st.success(f"✅ Tâche **{action}** terminée.")
    st.rerun()


# ── Launch helpers ────────────────────────────────────────────────────────────

def launch_researcher_job(
    root: Path,
    active_env: str,
    action: str,
    no_orcid: bool = False,
    no_dspace: bool = False,
    no_openalex: bool = False,
    start_year: int | None = None,
    end_year: int | None = None,
    sciper: str | None = None,
    use_cached: bool = False,
) -> None:
    if read_active_researcher_job(root):
        st.error("⛔ Une tâche est déjà en cours. Attendez sa fin avant d'en lancer une nouvelle.")
        return

    cmd = [
        sys.executable,
        str(root / "researcher_monitor" / "main.py"),
        "--action", action, "--env", active_env,
    ]
    if no_orcid:
        cmd.append("--no-orcid")
    if no_dspace:
        cmd.append("--no-dspace")
    if no_openalex:
        cmd.append("--no-openalex")
    if start_year:
        cmd += ["--start-year", str(start_year)]
    if end_year:
        cmd += ["--end-year", str(end_year)]
    if sciper:
        cmd += ["--sciper", sciper]
    if use_cached:
        cmd.append("--use-cached")

    log_path = root / "logs" / f"researcher_{action}_{datetime.now():%Y%m%d_%H%M%S}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=open(log_path, "w", encoding="utf-8"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        write_researcher_job_lock(root, action, proc.pid, log_path, cmd, active_env, sciper=sciper)
        st.rerun()
    except Exception as exc:
        st.error(f"Erreur au lancement : {exc}")


def launch_import_job(
    root: Path,
    active_env: str,
    db,
    row: dict,
    sciper: str,
    override_start_year: int | None = None,
) -> None:
    """Trigger a targeted researcher import pipeline run."""
    from researcher_monitor.import_trigger import ImportTrigger

    if read_active_researcher_job(root):
        st.error("⛔ Une tâche est déjà en cours. Attendez sa fin avant d'en lancer une nouvelle.")
        return

    lock_file = root / "data" / f"run_active_{active_env}.json"
    if lock_file.exists():
        st.error("⛔ Un run de pipeline est déjà actif. Attendez sa fin avant de lancer un import.")
        return

    trigger = ImportTrigger()
    if not trigger.can_trigger(row):
        st.error("⛔ Ce chercheur n'a pas d'identifiant OpenAlex ou Scopus — import impossible.")
        return

    try:
        result = trigger.trigger(
            row=row, env=active_env, root=root, db=db,
            override_start_year=override_start_year,
        )
        write_researcher_job_lock(
            root, "import", result["pid"],
            Path(result["log_file"]),
            trigger.build_pipeline_cmd(
                row=row, env=active_env, root=root, run_id=result["run_id"],
                override_start_year=override_start_year,
            ),
            active_env,
            sciper=sciper,
        )
        st.rerun()
    except (RuntimeError, ValueError) as exc:
        st.error(f"Erreur au lancement : {exc}")
    except Exception as exc:
        st.error(f"Erreur inattendue : {exc}")


# ── Manual action form ────────────────────────────────────────────────────────

def render_researcher_job_form(
    active_env: str,
    root: Path,
    db=None,
) -> None:
    """Launch buttons for manual registry sync, harvest, and gap analysis."""
    sc1, sc2 = st.columns(2)

    with sc1:
        with st.container(border=True):
            st.markdown("**Sync registre (EPFL People API)**")
            st.caption(
                "Découverte complète depuis l'API EPFL People : "
                "ajoute les nouveaux membres, offboarde les partants."
            )
            no_orcid = st.checkbox(
                "Sans enrichissement ORCID", key="sync_no_orcid",
                help="Plus rapide, ne contacte pas l'API ORCID",
            )
            no_dspace = st.checkbox(
                "Sans enrichissement Infoscience", key="sync_no_dspace",
                help="Ne récupère pas le profil Infoscience (uuid, OpenAlex IS, variantes de noms)",
            )
            no_openalex = st.checkbox(
                "Sans inférence OpenAlex", key="sync_no_openalex",
                help="Ne tente pas d'inférer l'OpenAlex ID par recoupement de DOIs",
            )
            if st.button(
                "Synchroniser le registre", key="btn_sync_registry",
                icon=":material/people:", type="primary",
            ):
                launch_researcher_job(
                    root, active_env, "sync",
                    no_orcid=no_orcid,
                    no_dspace=no_dspace,
                    no_openalex=no_openalex,
                )

    with sc2:
        with st.container(border=True):
            st.markdown("**Actualiser les données du registre**")
            st.caption(
                "Ré-enrichit les entrées déjà présentes (SCIPER connus) sans découverte "
                "ni offboarding. Utile pour mettre à jour ORCID, profils Infoscience "
                "et identifiants OpenAlex sur le périmètre existant."
            )
            ref_no_orcid = st.checkbox(
                "Sans enrichissement ORCID", key="ref_no_orcid",
                help="Plus rapide, ne contacte pas l'API ORCID",
            )
            ref_no_dspace = st.checkbox(
                "Sans enrichissement Infoscience", key="ref_no_dspace",
            )
            ref_no_openalex = st.checkbox(
                "Sans inférence OpenAlex", key="ref_no_openalex",
            )
            ref_sciper_input = st.text_input(
                "Restreindre à des SCIPERs (optionnel)",
                key="ref_sciper_input",
                placeholder="349140, 120091, …",
                help="Laisser vide pour actualiser tous les chercheurs actifs du registre",
            )
            if st.button(
                "Actualiser", key="btn_refresh_registry",
                icon=":material/refresh:", type="secondary",
            ):
                sciper_arg = ref_sciper_input.strip() or None
                launch_researcher_job(
                    root, active_env, "refresh",
                    no_orcid=ref_no_orcid,
                    no_dspace=ref_no_dspace,
                    no_openalex=ref_no_openalex,
                    sciper=sciper_arg,
                )

    sc3, _ = st.columns(2)
    with sc3:
        with st.container(border=True):
            st.markdown("**Moissonner les publications**")
            st.caption(
                "Récupère les publications depuis OpenAlex et ORCID. "
                "La fenêtre est automatiquement restreinte à la période d'affiliation EPFL "
                "de chaque chercheur (enrollment → offboarding)."
            )
            col_y1, col_y2 = st.columns(2)
            with col_y1:
                start_y = st.number_input(
                    "Depuis (borne max)", min_value=2000, max_value=2030,
                    value=datetime.now().year - 3, key="harvest_start",
                )
            with col_y2:
                end_y = st.number_input(
                    "Jusqu'à (borne max)", min_value=2000, max_value=2030,
                    value=datetime.now().year, key="harvest_end",
                )
            if st.button("Moissonner", key="btn_harvest", icon=":material/cloud_download:", type="primary"):
                launch_researcher_job(
                    root, active_env, "harvest",
                    start_year=int(start_y), end_year=int(end_y),
                )

    with st.container(border=True):
        st.markdown("**Analyse des lacunes Infoscience**")
        st.caption(
            "Compare les publications moissonnées aux items Infoscience liés à chaque chercheur "
            "via son profil CRIS (relation ``RELATION.Person.researchoutputs``)."
        )
        _ALL_OPT = "Tous les chercheurs avec publications"
        if db is not None:
            reg_df = db.get_researcher_registry_df(active_only=True)
            with_pubs = reg_df[reg_df["harvested_pubs"] > 0]
            analyze_opts = [_ALL_OPT] + [
                f"{row['full_name']} ({row['sciper']})"
                for _, row in with_pubs.iterrows()
            ]
        else:
            analyze_opts = [_ALL_OPT]
        selected_analyze = st.selectbox("Chercheur", analyze_opts, key="analyze_researcher_sel")
        if st.button("Analyser", key="btn_analyze", icon=":material/analytics:", type="primary"):
            sciper_arg = (
                None if selected_analyze == _ALL_OPT
                else selected_analyze.split("(")[-1].rstrip(")")
            )
            launch_researcher_job(root, active_env, "analyze", sciper=sciper_arg)

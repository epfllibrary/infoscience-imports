"""Run launcher page — form to configure and start a pipeline run with live log streaming."""

from __future__ import annotations

import html as _html
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import streamlit as st

from config import default_queries
from db.pipeline_db import PipelineDB
from ui.helpers import get_db, missing_required_env, page_title, sh, _make_run_id
from ui.run_state import (
    get_state_file,
    kill_active_run,
    read_active_run,
    try_acquire_run_lock,
)


def render(active_env: str, root: Path, sources: list[str]) -> None:
    """Render the run launcher page — live log view when a run is active, form otherwise."""
    page_title("play_circle", "Lancer un run")

    active = read_active_run()

    if active:
        _render_running(active, root)
    else:
        _render_form(active_env, root, sources)


def _render_running(active: dict, root: Path) -> None:
    log_file = Path(active.get("log_file", ""))
    run_id = active["run_id"]

    st.success(f"⏳ Run **{run_id}** en cours…")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.caption(f"Commande : `{active.get('cmd', '')}`")
    with col2:
        if st.button("⛔ Arrêter le run", type="secondary"):
            if kill_active_run():
                try:
                    _db = PipelineDB()
                    _db.finish_run(run_id, status="killed")
                    _db.close()
                except Exception:
                    pass
                st.warning("Signal d'arrêt envoyé au processus.")
                time.sleep(1)
                st.cache_resource.clear()
                st.rerun()

    st.markdown(sh("terminal", "Logs en direct"), unsafe_allow_html=True)
    log_box = st.empty()
    info_box = st.empty()

    while True:
        current = read_active_run()
        if log_file.exists():
            lines = log_file.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = "\n".join(lines[-300:])
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

    st.cache_resource.clear()
    db2 = get_db()
    runs_df = db2.get_runs(limit=5)
    matching = runs_df[runs_df["run_id"] == run_id] if not runs_df.empty else runs_df
    if not matching.empty:
        status = matching.iloc[0]["status"]
        if status == "completed":
            st.success("✅ Run terminé avec succès. Consultez les pages Publications et Statistiques.")
            st.balloons()
        else:
            st.error(f"❌ Run terminé avec statut : {status}")
    else:
        st.info("Run terminé.")


def _render_form(active_env: str, root: Path, sources: list[str]) -> None:
    missing = missing_required_env(active_env, root)
    if missing:
        st.error(
            f"⛔ Impossible de lancer un run sur **{active_env.upper()}** : "
            f"variable(s) requise(s) non définie(s) dans `.env.{active_env}` : "
            f"`{'`, `'.join(missing)}`"
        )
        st.info("Configurez ces variables dans le fichier `.env` correspondant ou changez d'environnement.")
        return

    st.markdown("Configure les paramètres et lance le pipeline.")

    # Time window — outside the form so preset changes trigger an immediate rerun.
    st.markdown(sh("date_range", "Fenêtre temporelle"), unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    with col1:
        window_mode = st.radio("Mode", ["Fenêtre glissante", "Dates fixes"], horizontal=True)
    with col2:
        if window_mode == "Fenêtre glissante":
            window_days = st.number_input("Jours", min_value=1, max_value=365, value=15)
        else:
            start_date_input = st.date_input("Date de début", value=date.today() - timedelta(days=14))
    with col3:
        if window_mode != "Fenêtre glissante":
            end_date_input = st.date_input("Date de fin", value=date.today())

    with st.form("run_form"):
        st.markdown(sh("label", "Nom du run (optionnel)"), unsafe_allow_html=True)
        run_name_input = st.text_input(
            "Nom", placeholder="ex : tests-scopus-janvier",
            help="Inclus dans l'identifiant du run. Laissez vide pour utiliser uniquement la date.",
        )

        st.markdown(sh("hub", "Sources"), unsafe_allow_html=True)
        selected_sources = st.multiselect(
            "Sources à inclure", options=sources, default=sources,
            help="Laissez vide pour utiliser toutes les sources.",
        )

        st.markdown(sh("search", "Requêtes (optionnel)"), unsafe_allow_html=True)
        with st.expander("Personnaliser les requêtes par source", expanded=False):
            st.caption("Laissez vide pour utiliser les requêtes par défaut de `config.py`.")
            query_fields: dict[str, str] = {}
            active_src = selected_sources or sources
            q_cols = st.columns(2)
            for i, src in enumerate(active_src):
                with q_cols[i % 2]:
                    query_fields[src] = st.text_area(
                        src.upper(), value="",
                        placeholder=default_queries.get(src, ""),
                        height=88, key=f"query_{src}",
                    )

        st.markdown(sh("person_search", "Identifiants auteurs (optionnel)"), unsafe_allow_html=True)
        col_a, col_b = st.columns(2)
        with col_a:
            scopus_ids = st.text_area("Scopus Author IDs", placeholder="7004212771\n57201854951", height=80)
        with col_b:
            wos_ids = st.text_area("WoS ResearcherIDs", placeholder="A-1234-2010", height=80)
        col_c, col_d = st.columns(2)
        with col_c:
            orcid_ids = st.text_area("ORCID iDs", placeholder="0000-0002-1825-0097", height=80)
        with col_d:
            openalex_ids = st.text_area("OpenAlex Author IDs", placeholder="A5023888391", height=80)

        st.markdown(sh("tune", "Options"), unsafe_allow_html=True)
        col_o1, col_o2, col_o3 = st.columns(3)
        with col_o1:
            dry_run = st.checkbox("Dry-run (sans import DSpace)", value=False)
        with col_o2:
            no_email = st.checkbox("Désactiver l'envoi d'e-mail", value=True)
        with col_o3:
            verbose = st.checkbox("Verbose (-vv)", value=False)

        submitted = st.form_submit_button("▶ Lancer le pipeline", width="stretch")

    if not submitted:
        return

    run_id = _make_run_id(run_name_input)
    log_file = root / "logs" / f"run_{run_id}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = [sys.executable, str(root / "data_pipeline" / "main.py")]
    if window_mode == "Fenêtre glissante":
        cmd += ["--window-days", str(window_days)]
    else:
        cmd += ["--start-date", str(start_date_input), "--end-date", str(end_date_input)]
    if selected_sources:
        cmd += ["--sources", ",".join(selected_sources)]
    cmd += ["--env", active_env, "--run-id", run_id]
    for src, qval in query_fields.items():
        if qval.strip():
            cmd += [f"--query-{src}", qval.strip()]
    if scopus_ids.strip():
        cmd += ["--scopus-ids", scopus_ids.strip().replace("\n", ",")]
    if wos_ids.strip():
        cmd += ["--wos-ids", wos_ids.strip().replace("\n", ",")]
    if orcid_ids.strip():
        cmd += ["--orcid-ids", orcid_ids.strip().replace("\n", ",")]
    if openalex_ids.strip():
        cmd += ["--openalex-ids", openalex_ids.strip().replace("\n", ",")]
    if dry_run:
        cmd.append("--dry-run")
    if no_email:
        cmd.append("--no-email")
    if verbose:
        cmd.append("-vv")

    st.code(" ".join(cmd), language="bash")

    log_fh = open(log_file, "w", encoding="utf-8")
    acquired = try_acquire_run_lock(
        run_id=run_id, pid=0,
        sources=selected_sources or sources,
        dry_run=dry_run,
        log_file=str(log_file),
        cmd=cmd,
    )
    if not acquired:
        log_fh.close()
        log_file.unlink(missing_ok=True)
        st.error(
            "⛔ Un run est déjà en cours (lancé par un autre utilisateur). "
            "Attendez sa fin avant d'en démarrer un nouveau."
        )
        st.stop()

    proc = subprocess.Popen(
        cmd, stdout=log_fh, stderr=subprocess.STDOUT,
        cwd=str(root), env={**os.environ},
    )
    get_state_file().write_text(
        json.dumps({
            "run_id":     run_id,
            "pid":        proc.pid,
            "env":        active_env,
            "started_at": datetime.now().isoformat(),
            "sources":    selected_sources or sources,
            "dry_run":    dry_run,
            "log_file":   str(log_file),
            "cmd":        " ".join(cmd),
        }, indent=2),
        encoding="utf-8",
    )
    log_fh.close()
    st.rerun()

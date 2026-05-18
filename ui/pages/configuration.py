"""Configuration page — env vars status, DuckDB info, .env template."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import dotenv_values

from db.pipeline_db import PipelineDB
from ui.helpers import page_title, sh

_ROOT = Path(__file__).resolve().parent.parent.parent


_ENV_VARS: dict[str, tuple[str, bool]] = {
    "DS_API_ENDPOINT":       ("DSpace REST API URL", True),
    "DS_API_TOKEN":          ("DSpace REST API static token", True),
    "DS_ACCESS_TOKEN":       ("DSpace session cookie token (alt. auth)", False),
    "API_EPFL_USER":         ("EPFL People API user", False),
    "API_EPFL_PWD":          ("EPFL People API password", False),
    "SCOPUS_API_KEY":        ("Scopus API key", False),
    "SCOPUS_INST_TOKEN":     ("Scopus Inst. token", False),
    "WOS_TOKEN":             ("WoS API token", False),
    "EPO_OPS_KEY":           ("EPO OPS key", False),
    "EPO_OPS_SECRET":        ("EPO OPS secret", False),
    "OPENALEX_API_KEY":      ("OpenAlex API key", False),
    "OPENALEX_DATA_VERSION": ("OpenAlex data version (default: 2)", False),
    "ZENODO_API_KEY":        ("Zenodo API key", False),
    "ORCID_API_TOKEN":       ("ORCID API token", False),
    "ELS_API_KEY":           ("Elsevier API key (Unpaywall PDF)", False),
    "CONTACT_API_EMAIL":     ("E-mail polite pool APIs", False),
    "USER_AGENT":            ("HTTP User-Agent header", False),
    "RECIPIENT_EMAIL":       ("E-mail rapport", False),
    "SENDER_EMAIL":          ("E-mail expéditeur", False),
    "SMTP_SERVER":           ("Serveur SMTP", False),
}

_ENV_TEMPLATE = """\
# Infoscience Import Pipeline — Variables d'environnement
# Copier ce fichier en .env à la racine du projet

# ── DSpace REST API (requis) ──────────────────────────────────────────────────
DS_API_ENDPOINT=https://<domain>/server/api
DS_API_TOKEN=<static_token>
# DS_ACCESS_TOKEN=<session_cookie_token>  # alternative auth après login

# ── EPFL People API ───────────────────────────────────────────────────────────
API_EPFL_USER=<username>
API_EPFL_PWD=<password>

# ── Scopus (Elsevier) ─────────────────────────────────────────────────────────
SCOPUS_API_KEY=<key>
SCOPUS_INST_TOKEN=<institutional_token>
ELS_API_KEY=<elsevier_key>  # PDF retrieval via Unpaywall

# ── Web of Science ────────────────────────────────────────────────────────────
WOS_TOKEN=<token>

# ── EPO Open Patent Services ──────────────────────────────────────────────────
EPO_OPS_KEY=<key>
EPO_OPS_SECRET=<secret>

# ── OpenAlex ──────────────────────────────────────────────────────────────────
OPENALEX_API_KEY=<key>
# OPENALEX_DATA_VERSION=2

# ── Zenodo ────────────────────────────────────────────────────────────────────
ZENODO_API_KEY=<key>

# ── ORCID ─────────────────────────────────────────────────────────────────────
ORCID_API_TOKEN=<token>

# ── Polite pool (Crossref, Unpaywall, OpenAlex) ───────────────────────────────
CONTACT_API_EMAIL=<your_email>
# USER_AGENT=EPFL-Infoscience-imports/1.0 (mailto:<your_email>)

# ── Rapport e-mail (optionnel) ────────────────────────────────────────────────
RECIPIENT_EMAIL=<recipient>
SENDER_EMAIL=<sender>
SMTP_SERVER=<smtp_host>
"""


def render(db: PipelineDB, active_env: str) -> None:
    """Render the configuration page — env vars status, DuckDB info, and .env template."""
    page_title("settings", "Configuration")
    st.markdown("Variables d'environnement et état des connexions.")

    # ── Env vars status ───────────────────────────────────────────────────────
    # Read directly from the env-specific file so switching environments shows
    # the correct values without contamination from a previously loaded env.
    env_file = _ROOT / f".env.{active_env}"
    fallback  = _ROOT / ".env"
    env_values: dict[str, str | None] = {}
    if env_file.exists():
        env_values = dotenv_values(env_file)
        source_label = f"`.env.{active_env}`"
    elif fallback.exists():
        env_values = dotenv_values(fallback)
        source_label = "`.env` (fallback)"
    else:
        source_label = "aucun fichier .env trouvé"

    st.caption(f"Source : {source_label}")
    st.markdown(sh("key", "Variables d'environnement"), unsafe_allow_html=True)
    _CLEARTEXT_VARS = {
        "DS_API_ENDPOINT", "CONTACT_API_EMAIL", "USER_AGENT",
        "RECIPIENT_EMAIL", "SENDER_EMAIL", "SMTP_SERVER",
    }

    rows = []
    for var, (desc, required) in _ENV_VARS.items():
        val = env_values.get(var) or None
        set_icon = "✅" if val else ("🔴" if required else "⚪")
        if var in _CLEARTEXT_VARS:
            display = val or "—"
        else:
            display = ("*" * 8 + val[-4:]) if val and len(val) > 4 else ("***" if val else "—")
        rows.append({"Variable": var, "Description": desc,
                     "Requis": "●" if required else "", "Valeur": display, "État": set_icon})
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

    # ── DuckDB info ───────────────────────────────────────────────────────────
    st.markdown(sh("storage", "Base de données DuckDB"), unsafe_allow_html=True)
    db_path = db.db_path
    col1, col2 = st.columns(2)
    col1.metric("Chemin", str(db_path))
    size_mb = db_path.stat().st_size / 1024 / 1024 if db_path.exists() else 0
    col2.metric("Taille", f"{size_mb:.2f} MB")

    # ── .env template ─────────────────────────────────────────────────────────
    st.markdown(sh("code", "Modèle .env"), unsafe_allow_html=True)
    st.code(_ENV_TEMPLATE, language="bash")

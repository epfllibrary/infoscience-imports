"""Shared UI helper functions — icons, badges, cards, formatting, DB accessor."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from db.pipeline_db import PipelineDB
from utils import make_run_id


def _make_run_id(name: str = "") -> str:
    return make_run_id(name)


def mi(name: str, extra_class: str = "") -> str:
    """Return a Material Symbols Outlined icon span."""
    cls = f"ms {extra_class}".strip()
    return f'<span class="{cls}">{name}</span>'


def page_title(icon: str, label: str) -> None:
    """Render an h1 page title with a Material Symbols icon."""
    st.markdown(
        f'<h1 class="page-title">{mi(icon)}{label}</h1>',
        unsafe_allow_html=True,
    )


def sh(icon: str, label: str) -> str:
    """Return a section-header div with a Material Symbols icon."""
    return f'<div class="section-header">{mi(icon)}{label}</div>'


def metric_card(label: str, value, sub: str = "") -> str:
    """Return an HTML metric card with label, value, and optional sub-text."""
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {"<div class='metric-sub'>" + sub + "</div>" if sub else ""}
    </div>"""


def badge(status: str) -> str:
    """Return an HTML status badge span."""
    return f'<span class="badge badge-{status}">{status}</span>'


def fmt_dur(s) -> str:
    """Format a duration in seconds as human-readable string."""
    if s is None or pd.isna(s) or s <= 0:
        return "—"
    s = int(s)
    if s >= 3600:
        return f"{s // 3600}h {(s % 3600) // 60}m {s % 60}s"
    return f"{s // 60}m {s % 60}s"


def fmt_dt(v) -> str:
    """Format a datetime value (ISO string or datetime) as 'YYYY-MM-DD HH:MM:SS'."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return str(v)[:19].replace("T", " ")


def missing_required_env(env_name: str, root: Path) -> list[str]:
    """Return names of required DSpace variables absent from .env.{env_name}."""
    from dotenv import dotenv_values
    _REQUIRED = ("DS_API_ENDPOINT", "DS_API_TOKEN")
    env_file = root / f".env.{env_name}"
    fallback  = root / ".env"
    values = dotenv_values(env_file if env_file.exists() else fallback)
    return [k for k in _REQUIRED if not values.get(k)]


# PipelineDB opens no persistent connection — caching the instance is safe.
@st.cache_resource
def get_db() -> PipelineDB:
    """Return a cached read-only PipelineDB instance."""
    return PipelineDB(read_only=True)


def db_lock_guard(fn):
    """Run fn(); show a friendly warning instead of crashing on DuckDB lock conflicts.

    Use this to wrap any Streamlit render function that makes DB calls, so that
    a concurrent write lock held by a running pipeline subprocess does not crash
    the page.
    """
    import duckdb as _duckdb
    try:
        fn()
    except _duckdb.IOException as exc:
        if "Conflicting lock" in str(exc):
            st.warning(
                "Une synchronisation est en cours — la base de données est temporairement "
                "verrouillée. Actualisez la page dans quelques secondes.",
                icon="⏳",
            )
        else:
            raise

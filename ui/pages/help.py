"""Help page — renders the curator guide from docs/guide-curateurs.md."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from ui.helpers import page_title


def render(root: Path) -> None:
    """Render the help page from the curator guide markdown file."""
    page_title("menu_book", "Aide — Guide curateurs")

    doc_path = root / "docs" / "documentation.md"
    if not doc_path.exists():
        st.error(
            f"Fichier de documentation introuvable : `{doc_path.relative_to(root)}`"
        )
        return

    content = doc_path.read_text(encoding="utf-8")

    # Table of contents anchor navigation
    sections = [
        ("1. Vue d'ensemble", "#1-vue-densemble-du-workflow"),
        ("2. Connexion", "#2-connexion-et-navigation"),
        ("3. Tableau de bord", "#3-tableau-de-bord"),
        ("4. Lancer un run", "#4-lancer-un-run"),
        ("5. Programmation", "#5-programmation-des-runs"),
        ("6. Publications", "#6-publications"),
        ("7. Statistiques", "#7-statistiques"),
        ("8. Configuration", "#8-configuration"),
        ("9. Workflow pas à pas", "#9-workflow-de-curation-pas-à-pas"),
        ("10. Référence rapide", "#10-référence-rapide--statuts-et-icônes"),
    ]

    toc_items = " &nbsp;·&nbsp; ".join(
        f'<a href="{href}" style="color:#6366F1;text-decoration:none">{label}</a>'
        for label, href in sections
    )
    st.markdown(
        f'<div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:8px;'
        f'padding:10px 16px;font-size:0.82rem;line-height:1.9;margin-bottom:16px">'
        f'<strong>Sommaire rapide :</strong> {toc_items}</div>',
        unsafe_allow_html=True,
    )

    st.markdown(content)

"""Cleanup page — search and remove workspace/workflow items from DSpace.

Workflow:
  1. Fill search form (date range + submitter UUID + optional free query).
  2. Click "Rechercher" → preview table.
  3. Select items via checkboxes.
  4. Choose action: reject to draft (workflow only) or delete permanently.
  5. Confirm in modal → execute sequentially with progress bar.
"""

from __future__ import annotations

import html as _html
import os
from datetime import date
from urllib.parse import urlencode

import streamlit as st

from ui.helpers import page_title, mi

# ── Session-state keys ────────────────────────────────────────────────────────
_KEY_RESULTS        = "_cleanup_results"        # rows for the current UI page
_KEY_SELECTED       = "_cleanup_selected"
_KEY_QUERY          = "_cleanup_last_query"
_KEY_PAGE           = "_cleanup_page"           # 1-indexed UI page
_KEY_LOADED_PAGE    = "_cleanup_loaded_page"    # last page actually fetched from DSpace
_KEY_TOTAL_PAGES    = "_cleanup_total_pages"
_KEY_TOTAL_ELEMS    = "_cleanup_total_elements"
_KEY_SEARCH_PARAMS  = "_cleanup_search_params"  # stored params for page-nav re-fetch
_KEY_PAGE_SIZE      = "_cleanup_page_size"
_PAGE_SIZE_OPTIONS  = [10, 20, 50, 100]
_PAGE_SIZE_DEFAULT  = 20

# ── Default submitter UUID (pipeline account) ─────────────────────────────────
_DEFAULT_SUBMITTER_UUID = "4e8d183f-1309-470c-955e-c45a99c6f1b8"


# ── Metadata helpers ──────────────────────────────────────────────────────────

def _meta_val(metadata: dict, field: str) -> str:
    entries = metadata.get(field, [])
    if entries and isinstance(entries, list):
        return entries[0].get("value", "") or ""
    return ""


def _parse_search_hit(obj: dict) -> dict:
    """Parse one search result object from the supervision discovery endpoint.

    Expects the object shape returned by
    GET /discover/search/objects?configuration=supervision&embed=item,submitter
    &projection=preventMetadataSecurity
    """
    hit = obj.get("_embedded", {}).get("indexableObject", {})
    hit_emb = hit.get("_embedded", {})
    item = hit_emb.get("item", {})
    md = item.get("metadata", {})
    submitter_name = item.get("submitterName", "")
    if not submitter_name:
        submitter = hit_emb.get("submitter", {})
        sub_meta = submitter.get("metadata", {})
        fn = _meta_val(sub_meta, "eperson.firstname")
        ln = _meta_val(sub_meta, "eperson.lastname")
        submitter_name = f"{fn} {ln}".strip() or submitter.get("email", "")
    return {
        "id":           hit.get("id"),
        "uuid":         item.get("uuid", ""),
        "item_type":    hit.get("type", ""),
        "submitter":    submitter_name,
        "date_created": _meta_val(md, "dc.date.created") or _meta_val(md, "dc.date.accessioned"),
        "title":        _meta_val(md, "dc.title"),
        "doi":          _meta_val(md, "dc.identifier.doi") or _meta_val(md, "dc.identifier.uri"),
        "dc_type":      _meta_val(md, "dc.type"),
    }


def _parse_item_response(raw: dict) -> dict:
    """Parse a workspaceitem/workflowitem API response (with embed=item,submitter)."""
    emb = raw.get("_embedded", {})
    item = emb.get("item", {})
    md = item.get("metadata", {})
    submitter = emb.get("submitter", {})
    sub_meta = submitter.get("metadata", {})
    firstname = _meta_val(sub_meta, "eperson.firstname")
    lastname = _meta_val(sub_meta, "eperson.lastname")
    submitter_name = (f"{firstname} {lastname}".strip()
                      or submitter.get("email", ""))
    return {
        "id":           raw.get("id"),
        "uuid":         item.get("uuid", ""),
        "item_type":    raw.get("type", ""),
        "submitter":    submitter_name,
        "date_created": _meta_val(md, "dc.date.created") or _meta_val(md, "dc.date.accessioned"),
        "title":        _meta_val(md, "dc.title"),
        "doi":          _meta_val(md, "dc.identifier.doi") or _meta_val(md, "dc.identifier.uri"),
        "dc_type":      _meta_val(md, "dc.type"),
    }


# ── DSpace search ─────────────────────────────────────────────────────────────

def _run_search(
    query: str,
    submitter_uuid: str,
    item_types: list[str],
    page_num: int = 0,
    page_size: int = _PAGE_SIZE_DEFAULT,
) -> tuple[list[dict], int, int]:
    """Fetch one page of supervision search results from DSpace.

    Returns (rows, total_pages, total_elements).
    Uses embed=item,submitter + projection=preventMetadataSecurity so all
    metadata comes in a single HTTP call — no per-item fetches.
    """
    from clients.dspace_client_wrapper import DSpaceClientWrapper

    try:
        client = DSpaceClientWrapper()
    except Exception as exc:
        st.error(f"Impossible de se connecter à DSpace : {exc}")
        return [], 1, 0

    # DSpace supervision search: "workspace" / "workflow" as namedresourcetype values.
    # Combining both in a single filter returns no results — only add for exactly one type.
    named_types = [t for t in item_types if t in ("workspace", "workflow")]

    params: dict = {
        "configuration": "supervision",
        "query": query,
        "projection": "preventMetadataSecurity",
        "embed": "item,submitter",
        "size": str(page_size),
        "page": str(page_num),
    }
    if len(named_types) == 1:
        params["f.namedresourcetype"] = f"{named_types[0]},authority"
    if submitter_uuid.strip():
        params["f.submitter"] = f"{submitter_uuid.strip()},authority"

    url = f"{client.client.API_ENDPOINT}/discover/search/objects?{urlencode(params)}"
    try:
        resp = client.client.api_get(url)
    except Exception as exc:
        st.error(f"Erreur lors de la recherche : {exc}")
        return [], 1, 0

    if resp.status_code != 200:
        st.error(f"DSpace a retourné HTTP {resp.status_code}.")
        return [], 1, 0

    sr = resp.json().get("_embedded", {}).get("searchResult", {})
    page_info = sr.get("page", {})
    total_pages = max(1, page_info.get("totalPages", 1))
    total_elements = page_info.get("totalElements", 0)
    objects = sr.get("_embedded", {}).get("objects", [])
    return [_parse_search_hit(obj) for obj in objects], total_pages, total_elements


# ── Query builder ─────────────────────────────────────────────────────────────

def _build_date_query(from_date: date, to_date: date) -> str:
    return (
        f"dc.date.created:[{from_date.strftime('%Y-%m-%d')} "
        f"TO {to_date.strftime('%Y-%m-%d')}]"
    )


# ── Modals ────────────────────────────────────────────────────────────────────

@st.dialog("Confirmer l'action", width="large")
def _confirm_modal(
    action: str,
    selected_rows: list[dict],
) -> None:
    n = len(selected_rows)
    action_label = {
        "reject":  "Renvoyer en draft",
        "delete":  "Supprimer définitivement",
    }.get(action, action)

    st.markdown(
        f'<p>Action : <b>{_html.escape(action_label)}</b> sur <b>{n}</b> item(s)</p>',
        unsafe_allow_html=True,
    )
    for r in selected_rows[:8]:
        t = _html.escape((r.get("title") or r.get("uuid") or "—")[:80])
        badge = "workspace" if "workspace" in (r.get("item_type") or "") else "workflow"
        st.markdown(f"• `{badge}` — {t}", unsafe_allow_html=True)
    if n > 8:
        st.caption(f"… et {n - 8} autre(s).")

    if action == "delete":
        st.warning("Cette action est **irréversible**.", icon=":material/warning:")

    col1, col2 = st.columns(2)
    with col1:
        btn_label = f"{action_label} ({n})"
        btn_type  = "primary"
        if st.button(btn_label, type=btn_type, width="stretch",
                     icon=":material/delete_forever:" if action == "delete" else ":material/undo:"):
            _execute_action(action, selected_rows)
    with col2:
        if st.button("Annuler", width="stretch"):
            st.rerun()


def _execute_action(action: str, rows: list[dict]) -> None:
    from clients.dspace_client_wrapper import DSpaceClientWrapper

    try:
        client = DSpaceClientWrapper()
    except Exception as exc:
        st.error(f"Connexion DSpace impossible : {exc}")
        return

    progress = st.progress(0.0, text="Exécution en cours…")
    errors: list[str] = []
    ok_count = 0

    for i, row in enumerate(rows):
        item_id   = row.get("id")
        item_type = row.get("item_type", "")
        title     = (row.get("title") or row.get("uuid") or "?")[:40]
        progress.progress((i + 1) / len(rows), text=f"Traitement de «{title}»…")

        if action == "reject":
            if "workflow" in item_type:
                ok, msg, new_ws_id = client.reject_to_workspace(item_id, row.get("uuid") or None)
                if ok:
                    # workflow_id is gone; update the row with the new workspace ID so
                    # any subsequent action (e.g. delete) uses the correct reference
                    results: list[dict] = st.session_state.get(_KEY_RESULTS, [])
                    for r in results:
                        if str(r.get("id")) == str(item_id):
                            r["id"]        = new_ws_id if new_ws_id is not None else r["id"]
                            r["item_type"] = "workspaceitem"
                            break
                    st.session_state[_KEY_RESULTS] = results
            else:
                ok, msg = True, "Item déjà en workspace — ignoré."
        elif action == "delete":
            if "workflow" in item_type:
                ok, msg = client.delete_item(None, str(item_id), row.get("uuid") or None)
            else:
                ok, msg = client.delete_item(str(item_id), None, row.get("uuid") or None)
        else:
            ok, msg = False, f"Action inconnue : {action}"

        if ok:
            ok_count += 1
        else:
            errors.append(f"{title}: {msg}")

    st.session_state.pop(_KEY_SELECTED, None)
    if errors:
        st.error("Erreurs :\n" + "\n".join(f"• {e}" for e in errors))
    else:
        label = "renvoyé(s) en draft" if action == "reject" else "supprimé(s)"
        st.session_state["_cleanup_toast"] = f"{ok_count} item(s) {label} avec succès."
        st.rerun()


# ── Page render ───────────────────────────────────────────────────────────────

def render() -> None:
    page_title("cleaning_services", "Nettoyage des drafts et imports")

    if toast := st.session_state.pop("_cleanup_toast", None):
        st.toast(toast, icon="✅")

    # ── Search form ───────────────────────────────────────────────────────────
    with st.expander("Paramètres de recherche", expanded=True):
        c1, c2 = st.columns([2, 1])

        with c1:
            today = date.today()
            fd, td = st.columns(2)
            with fd:
                from_date = st.date_input("Date de création — du", value=today, key="cleanup_from")
            with td:
                to_date   = st.date_input("au", value=today, key="cleanup_to")

            auto_query = _build_date_query(from_date, to_date)
            # Sync the text widget when dates change, but only if the user hasn't
            # diverged from the previous auto-generated value.
            _last_auto = st.session_state.get("_cleanup_last_auto_query", "")
            if auto_query != _last_auto:
                current_q = st.session_state.get("cleanup_query", "")
                if current_q == _last_auto or not current_q:
                    st.session_state["cleanup_query"] = auto_query
                st.session_state["_cleanup_last_auto_query"] = auto_query
            free_query = st.text_input(
                "Query Solr (modifiable)",
                key="cleanup_query",
                help="Générée automatiquement depuis les dates. Vous pouvez la modifier librement.",
            )

        with c2:
            item_types = st.multiselect(
                "Type d'items",
                options=["workspace", "workflow"],
                default=["workspace", "workflow"],
                key="cleanup_types",
            )
            submitter_uuid = st.text_input(
                "UUID submitter",
                value=_DEFAULT_SUBMITTER_UUID,
                key="cleanup_submitter",
                help="UUID de l'eperson DSpace. Laissez vide pour ne pas filtrer.",
            )
            st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)
            if st.button(
                "Rechercher",
                icon=":material/search:",
                type="primary",
                key="cleanup_search_btn",
            ):
                if not item_types:
                    st.warning("Sélectionnez au moins un type d'item.")
                else:
                    _page_size = st.session_state.get(_KEY_PAGE_SIZE, _PAGE_SIZE_DEFAULT)
                    with st.spinner("Recherche en cours…"):
                        rows, total_pages, total_elements = _run_search(
                            free_query, submitter_uuid, item_types,
                            page_num=0, page_size=_page_size,
                        )
                    for _k in list(st.session_state.keys()):
                        if _k.startswith("cleanup_chk_"):
                            del st.session_state[_k]
                    st.session_state[_KEY_RESULTS]       = rows
                    st.session_state[_KEY_SELECTED]      = {}
                    st.session_state[_KEY_QUERY]         = free_query
                    st.session_state[_KEY_PAGE]          = 1
                    st.session_state[_KEY_LOADED_PAGE]   = 1
                    st.session_state[_KEY_TOTAL_PAGES]   = total_pages
                    st.session_state[_KEY_TOTAL_ELEMS]   = total_elements
                    st.session_state[_KEY_SEARCH_PARAMS] = {
                        "query":          free_query,
                        "submitter_uuid": submitter_uuid,
                        "item_types":     item_types,
                        "page_size":      _page_size,
                    }

    # ── Results ───────────────────────────────────────────────────────────────
    # Detect page navigation: _KEY_PAGE changed since last DSpace fetch → re-fetch.
    _cur_page    = st.session_state.get(_KEY_PAGE, 1)
    _loaded_page = st.session_state.get(_KEY_LOADED_PAGE, 1)
    _params      = st.session_state.get(_KEY_SEARCH_PARAMS)

    if _params and _cur_page != _loaded_page:
        with st.spinner(f"Chargement page {_cur_page}…"):
            _rows, _total_pages, _total_elements = _run_search(
                _params["query"],
                _params["submitter_uuid"],
                _params["item_types"],
                page_num=_cur_page - 1,  # DSpace pages are 0-indexed
                page_size=_params.get("page_size", _PAGE_SIZE_DEFAULT),
            )
        for _k in list(st.session_state.keys()):
            if _k.startswith("cleanup_chk_"):
                del st.session_state[_k]
        st.session_state[_KEY_RESULTS]      = _rows
        st.session_state[_KEY_TOTAL_PAGES]  = _total_pages
        st.session_state[_KEY_TOTAL_ELEMS]  = _total_elements
        st.session_state[_KEY_LOADED_PAGE]  = _cur_page

    results: list[dict] = st.session_state.get(_KEY_RESULTS, [])
    n_pages       = st.session_state.get(_KEY_TOTAL_PAGES, 1)
    total_elements = st.session_state.get(_KEY_TOTAL_ELEMS, 0)
    page          = max(1, min(st.session_state.get(_KEY_PAGE, 1), n_pages))
    st.session_state[_KEY_PAGE] = page

    if not results and _KEY_QUERY in st.session_state:
        st.info("Aucun item trouvé pour ces critères.")
        return

    if not results:
        return

    # The entire results list IS the current page (server-side pagination).
    page_items = results
    selected: dict = st.session_state.setdefault(_KEY_SELECTED, {})

    n_ws = sum(1 for r in page_items if "workspace" in (r.get("item_type") or ""))
    n_wf = sum(1 for r in page_items if "workflow"  in (r.get("item_type") or ""))

    # ── Toolbar ───────────────────────────────────────────────────────────────
    tb1, tb2, tb_sel_info, tb_size, tb_info = st.columns([1.4, 1.4, 1.6, 2.2, 2.2])
    with tb1:
        if st.button("Sélectionner la page", icon=":material/check_box:", key="cleanup_sel_page"):
            for r in page_items:
                selected[str(r["id"])] = r
            st.rerun()
    with tb2:
        if st.button("Tout désélectionner", icon=":material/deselect:", key="cleanup_desel",
                     disabled=not selected):
            st.session_state[_KEY_SELECTED] = {}
            for _k in list(st.session_state.keys()):
                if _k.startswith("cleanup_chk_"):
                    del st.session_state[_k]
            st.rerun()
    with tb_sel_info:
        n_sel = len(selected)
        if n_sel:
            n_sel_wf_tb = sum(1 for r in selected.values() if "workflow" in (r.get("item_type") or ""))
            n_sel_ws_tb = n_sel - n_sel_wf_tb
            st.markdown(
                f'<span style="line-height:38px;font-size:0.84rem;color:#374151">'
                f'<b>{n_sel}</b> sélectionné(s)'
                f'<span style="color:#9CA3AF;font-size:0.76rem"> '
                f'({n_sel_ws_tb} ws · {n_sel_wf_tb} wf)</span></span>',
                unsafe_allow_html=True,
            )
    with tb_size:
        _prev_size = st.session_state.get(_KEY_PAGE_SIZE, _PAGE_SIZE_DEFAULT)
        _new_size = st.segmented_control(
            "Par page",
            options=_PAGE_SIZE_OPTIONS,
            default=_prev_size,
            key="cleanup_page_size_ctrl",
            label_visibility="collapsed",
        )
        if _new_size and _new_size != _prev_size and _params:
            st.session_state[_KEY_PAGE_SIZE] = _new_size
            _params["page_size"] = _new_size
            st.session_state[_KEY_SEARCH_PARAMS] = _params
            st.session_state[_KEY_PAGE] = 1
            st.session_state[_KEY_LOADED_PAGE] = 0  # force re-fetch
            st.rerun()
        elif _new_size:
            st.session_state[_KEY_PAGE_SIZE] = _new_size
    with tb_info:
        st.markdown(
            f'<div style="line-height:38px;font-size:0.84rem;color:#6B7280;text-align:right">'
            f'<b>{total_elements}</b> résultat(s) — '
            f'<span style="color:#3B82F6">{n_ws} workspace</span> · '
            f'<span style="color:#8B5CF6">{n_wf} workflow</span>'
            f' <span style="color:#D1D5DB">· page {page}/{n_pages}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<hr style="margin:2px 0 6px">', unsafe_allow_html=True)

    # ── Table rows ────────────────────────────────────────────────────────────
    _COL_W = [0.38, 5.5, 0.85]
    for i, row in enumerate(page_items):
        row_id    = str(row["id"])
        item_type = row.get("item_type", "")
        is_wf     = "workflow" in item_type
        chk_key   = f"cleanup_chk_{row_id}"

        if chk_key not in st.session_state:
            st.session_state[chk_key] = row_id in selected

        cols = st.columns(_COL_W)

        def _on_chk(rid=row_id, r=row):
            sel: dict = st.session_state.setdefault(_KEY_SELECTED, {})
            if st.session_state.get(f"cleanup_chk_{rid}"):
                sel[rid] = r
            else:
                sel.pop(rid, None)

        cols[0].checkbox("Sélectionner", key=chk_key, label_visibility="hidden", on_change=_on_chk)

        # Card: title + type chip on first line, meta row below
        title   = _html.escape((row.get("title") or "—")[:120])
        dc_t    = _html.escape((row.get("dc_type") or "")[:35])
        date_str = _html.escape((row.get("date_created") or "")[:10])
        sub     = _html.escape((row.get("submitter") or "")[:40])
        doi     = row.get("doi") or ""
        uuid    = row.get("uuid") or ""
        _ds_base = os.environ.get("DS_API_ENDPOINT", "").split("/server")[0]

        type_chip = (
            f'<span class="ctbl-type">{dc_t}</span>' if dc_t else ""
        )
        doi_link = (
            f'<span class="ctbl-sep">·</span>'
            f'<a href="https://doi.org/{doi}" target="_blank">'
            f'{_html.escape(doi[:45])}</a>'
            if doi else ""
        )
        uuid_chip = (
            f'<span class="ctbl-sep">·</span>'
            f'<a href="{_ds_base}/items/{uuid}" target="_blank" class="ctbl-uuid">'
            f'{_html.escape(uuid)}</a>'
            if uuid else ""
        )
        date_part = f'<span>{date_str}</span>' if date_str else ""
        sub_part  = (
            f'<span class="ctbl-sep">·</span>'
            f'<span class="ctbl-submitter">'
            f'<span class="ms ms-neutral" style="font-size:13px;margin-right:0">person</span>'
            f'{sub}</span>'
            if sub else ""
        )

        cols[1].markdown(
            f'<div class="ctbl-row">'
            f'<div class="ctbl-title">{title}{type_chip}</div>'
            f'<div class="ctbl-meta">{date_part}{sub_part}{doi_link}{uuid_chip}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        badge_cls   = "ctbl-badge-wf" if is_wf else "ctbl-badge-ws"
        badge_label = "workflow" if is_wf else "workspace"
        cols[2].markdown(
            f'<div style="padding-top:6px"><span class="{badge_cls}">{badge_label}</span></div>',
            unsafe_allow_html=True,
        )

        if i < len(page_items) - 1:
            st.markdown('<hr style="margin:2px 0">', unsafe_allow_html=True)

    # ── Pagination controls ───────────────────────────────────────────────────
    if n_pages > 1:
        st.markdown('<hr style="margin:8px 0 4px">', unsafe_allow_html=True)
        pc1, pc2, pc3 = st.columns([1, 2, 1])
        with pc1:
            if st.button("← Précédent", key="cleanup_prev", disabled=page <= 1):
                st.session_state[_KEY_PAGE] = page - 1
                st.rerun()
        with pc2:
            st.markdown(
                f'<div style="text-align:center;font-size:0.84rem;color:#6B7280;'
                f'line-height:38px">Page <b>{page}</b> / {n_pages}'
                f' <span style="color:#D1D5DB">·</span> '
                f'{total_elements} résultat(s)</div>',
                unsafe_allow_html=True,
            )
        with pc3:
            if st.button("Suivant →", key="cleanup_next", disabled=page >= n_pages):
                st.session_state[_KEY_PAGE] = page + 1
                st.rerun()

    # ── Action bar ────────────────────────────────────────────────────────────
    if not selected:
        return

    selected_rows = list(selected.values())
    n_sel_wf = sum(1 for r in selected_rows if "workflow" in (r.get("item_type") or ""))
    n_sel_ws = sum(1 for r in selected_rows if "workspace" in (r.get("item_type") or ""))

    st.markdown('<hr style="margin:10px 0 8px">', unsafe_allow_html=True)
    st.markdown(
        f'<div style="font-size:0.9rem;font-weight:600;margin-bottom:8px">'
        f'{len(selected_rows)} item(s) sélectionné(s) — '
        f'{n_sel_ws} workspace · {n_sel_wf} workflow</div>',
        unsafe_allow_html=True,
    )

    ab1, ab2, ab3 = st.columns([1.5, 1.7, 4])
    with ab1:
        reject_disabled = n_sel_wf == 0
        if st.button(
            "Renvoyer en draft",
            icon=":material/undo:",
            key="cleanup_reject_btn",
            disabled=reject_disabled,
            help="Renvoie les items workflow sélectionnés en workspace (draft) sans les supprimer."
                 if not reject_disabled else "Aucun item workflow sélectionné.",
        ):
            _confirm_modal("reject", [r for r in selected_rows if "workflow" in (r.get("item_type") or "")])

    with ab2:
        if st.button(
            "Supprimer définitivement",
            icon=":material/delete_forever:",
            type="primary",
            key="cleanup_delete_btn",
        ):
            _confirm_modal("delete", selected_rows)

    if n_sel_wf > 0 and n_sel_ws > 0:
        st.caption(
            "ℹ️ «Renvoyer en draft» n'agit que sur les items workflow. "
            "«Supprimer» traite tous les items sélectionnés."
        )
    elif n_sel_ws > 0 and n_sel_wf == 0:
        st.caption("ℹ️ Tous les items sélectionnés sont en workspace — «Renvoyer en draft» est désactivé.")

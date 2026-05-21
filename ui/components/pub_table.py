"""Publications table component — HTML iframe renderer with inline modals.

Renders as a self-contained HTML document via st.iframe (data URI) so that
<dialog> modals and clipboard JS work correctly inside the iframe context.

The delete action communicates with the parent Streamlit page via
window.parent.postMessage(), which is the only cross-origin messaging API
permitted from a data: URI iframe by modern browsers.
"""

from __future__ import annotations

import base64
import html as _html
import json

import pandas as pd
import streamlit as st

from ui.constants import (
    SOURCE_TAGS,
    DEDUP_LABELS,
    DB_META_SECTIONS,
    RAW_META_SECTIONS,
)


# ── Inline CSS for the iframe component ──────────────────────────────────────

_CSS = """
*,*::before,*::after{box-sizing:border-box}
*{margin:0;padding:0}
html,body{font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:13px;background:#fff;color:#1D2939;-webkit-font-smoothing:antialiased}
a{color:#632CA6;text-decoration:none}a:hover{text-decoration:underline}
.wrap{border-radius:12px;border:1px solid #E4E7EC;box-shadow:0 2px 8px rgba(16,24,40,.05);overflow:hidden}
table{width:100%;border-collapse:collapse}
th{background:#F9FAFB;color:#667085;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;padding:9px 10px;text-align:left;border-bottom:2px solid #E4E7EC;white-space:nowrap}
td{padding:8px 10px;vertical-align:middle;border-bottom:1px solid #F2F4F7;line-height:1.4}
tr:last-child td{border-bottom:none}
tr:hover td{background:#F8F9FC}
.c-act{width:82px}.c-run{width:90px;font-size:11px;color:#667085}
.c-yr{width:46px;font-weight:600;font-size:13px;white-space:nowrap}
.c-ttl{min-width:200px}.c-oa{width:112px}.c-auth{width:140px}.c-unit{width:110px;font-size:11.5px;color:#374151}
.c-lk{width:140px}.c-btn{width:30px;text-align:center;padding:6px 3px}
.doi-row{display:flex;align-items:center;gap:4px;margin-bottom:3px}
.doi-lk{font-family:'SF Mono','Roboto Mono',monospace;font-size:10.5px;color:#632CA6;word-break:break-all;flex:1;min-width:0}
.copy-btn{flex-shrink:0;background:none;border:1px solid #E4E7EC;border-radius:4px;padding:1px 5px;font-size:12px;cursor:pointer;color:#667085;transition:background .1s,border-color .1s,color .1s;line-height:1.4}
.copy-btn:hover{background:#F0F4FF;border-color:#C4B5FD;color:#632CA6}
.copy-btn.copied{background:#DCFCE7;border-color:#86EFAC;color:#15803D}
.src{display:inline-block;padding:1px 6px;border-radius:4px;font-size:9.5px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;vertical-align:middle}
.s-scopus{background:#DBEAFE;color:#1E40AF}.s-wos{background:#EDE9FE;color:#5B21B6}
.s-crossref{background:#CCFBF1;color:#0F766E}.s-openalex{background:#DCFCE7;color:#15803D}
.s-zenodo{background:#FED7AA;color:#9A3412}.s-epo{background:#F3F4F6;color:#374151}
.s-datacite{background:#F3E8FF;color:#7E22CE}.s-def{background:#F1F5F9;color:#64748B}
.badge{display:inline-block;padding:1px 7px;border-radius:999px;font-size:10px;font-weight:600;vertical-align:middle}
.st-workflow{background:#EDE9FE;color:#6D28D9}.st-workspace{background:#FEF9C3;color:#854D0E}
.st-deduplicated{background:#DBEAFE;color:#1D4ED8}.st-rejected{background:#FEE2E2;color:#B91C1C}
.st-error{background:#FEE2E2;color:#B91C1C}.st-deleted{background:#F1F5F9;color:#64748B}
.ttype{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;background:#F1F5F9;color:#475569;max-width:150px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;vertical-align:middle}
.ttags{display:flex;flex-wrap:wrap;gap:3px;margin-bottom:4px;align-items:center}
.ttl{font-weight:500;color:#101828;font-size:12.5px;line-height:1.4}
.abl{display:flex;flex-direction:column;gap:3px}
.dash{color:#D0D5DD}
.ab{display:block;padding:3px 8px;border-radius:5px;font-size:10.5px;font-weight:600;text-decoration:none!important;text-align:center;border:1px solid;transition:filter .1s,transform .1s;line-height:1.4}
.ab:hover{filter:brightness(.88);transform:translateY(-1px);text-decoration:none!important}
.av{background:#DCFCE7;color:#15803D!important;border-color:#86EFAC}
.ae{background:#FEF9C3;color:#854D0E!important;border-color:#FDE68A}
.ac{background:#EDE9FE;color:#6D28D9!important;border-color:#C4B5FD}
.oa-v{display:block;font-weight:600;color:#16A34A;font-size:11px}
.lic-v{display:block;color:#667085;font-size:11px}
.pdf-tag{display:inline-block;padding:1px 5px;border-radius:3px;font-size:10px;font-weight:700;background:#DCFCE7;color:#15803D;margin-top:2px}
.auth-n{font-size:11.5px;line-height:1.4;margin-bottom:1px}
td.c-lk a{color:#632CA6;font-weight:500;font-size:11px;margin-right:4px}
.mbtn{background:#F9FAFB;border:1px solid #E4E7EC;border-radius:5px;padding:3px 6px;font-size:13px;cursor:pointer;color:#374151;line-height:1;transition:background .1s,border-color .1s;display:block;width:100%}
.mbtn:hover{background:#F0F4FF;border-color:#C4B5FD}
.mbtn:disabled{opacity:.3;cursor:not-allowed}
.flag-btn:hover{background:#FFF5F5;border-color:#FECACA}
dialog{border:none;border-radius:16px;padding:0;max-width:640px;width:90vw;max-height:80vh;box-shadow:0 24px 64px rgba(16,24,40,.22);overflow:hidden;position:fixed;top:24px;left:50%;transform:translateX(-50%);margin:0}
dialog::backdrop{background:rgba(16,24,40,.5);backdrop-filter:blur(3px);position:fixed;inset:0}
.mbox{display:flex;flex-direction:column;max-height:80vh}
.mhd{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid #E4E7EC;font-weight:700;font-size:13.5px;color:#101828;background:#FAFAFA;flex-shrink:0}
.mx{background:none;border:none;font-size:15px;color:#667085;cursor:pointer;padding:2px 6px;border-radius:4px;line-height:1}
.mx:hover{background:#F3F4F6}
.mbd{overflow-y:auto;padding:18px;flex:1}
.m-ttl{font-weight:600;color:#101828;margin:0 0 12px;font-size:13px;line-height:1.4}
.m-note{color:#B91C1C;font-weight:500;font-size:12px;margin:0 0 12px}
.m-info{color:#667085;font-size:12px;font-style:italic;padding:6px 0}
.m-sec{font-size:9.5px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#667085;margin:14px 0 5px;padding-bottom:4px;border-bottom:1px solid #F2F4F7}
.m-sec:first-of-type{margin-top:0}
.m-row{display:flex;gap:10px;padding:4px 0;border-bottom:1px solid #FAFAFA;font-size:11.5px}
.m-row-w{flex-direction:column;gap:3px}
.m-key{color:#667085;min-width:120px;flex-shrink:0;font-size:11px}
.m-val{color:#1D2939;word-break:break-all}
.m-pre{margin:0;white-space:pre-wrap;word-break:break-word;background:#F8FAFC;border:1px solid #E4E7EC;border-radius:5px;padding:7px 9px;font-size:11px;font-family:inherit;max-height:150px;overflow-y:auto;line-height:1.5;color:#1D2939}
.pma-list{display:flex;flex-direction:column;gap:8px}
.pma-card{border:1px solid #E4E7EC;border-radius:8px;padding:10px 12px;background:#FAFAFA}
.pma-weak{border-color:#FDE68A;background:#FFFBEB}
.pma-name{font-weight:600;font-size:13px;color:#101828;margin-bottom:6px}
.pma-meta{display:grid;grid-template-columns:1fr 1fr;gap:3px 12px;font-size:11px}
.pma-meta span{color:#475569}.pma-meta b{color:#101828;margin-right:3px}
.pma-wb{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700;background:#FEF3C7;color:#92400E;margin-left:5px;vertical-align:middle}
.pma-st-weak{color:#92400E;font-weight:600}
.pmf-list{display:flex;flex-direction:column;gap:8px}
.pmf-card{border:1px solid #FECACA;border-radius:8px;padding:10px 12px;background:#FFF5F5;font-size:12px;display:flex;flex-direction:column;gap:5px}
.pmf-card b{color:#101828;margin-right:3px}
code{background:#F1F5F9;padding:1px 5px;border-radius:3px;font-size:10.5px;color:#475569;font-family:inherit}
"""

_JS = """
document.querySelectorAll('[data-copy]').forEach(b=>{
  b.addEventListener('click',e=>{
    e.stopPropagation();
    const txt=b.dataset.copy;
    (navigator.clipboard?.writeText(txt)||Promise.reject()).then(()=>{
      b.classList.add('copied');b.textContent='✓';
      setTimeout(()=>{b.classList.remove('copied');b.textContent='⎘';},1600);
    }).catch(()=>{
      const ta=document.createElement('textarea');ta.value=txt;
      document.body.appendChild(ta);ta.select();document.execCommand('copy');
      document.body.removeChild(ta);
      b.classList.add('copied');b.textContent='✓';
      setTimeout(()=>{b.classList.remove('copied');b.textContent='⎘';},1600);
    });
  });
});
document.querySelectorAll('[data-modal]').forEach(b=>{
  b.addEventListener('click',e=>{
    e.stopPropagation();
    document.getElementById(b.dataset.modal)?.showModal();
    window.scrollTo({top:0,behavior:'smooth'});
  });
});
document.querySelectorAll('[data-close]').forEach(b=>{
  b.addEventListener('click',()=>document.getElementById(b.dataset.close)?.close());
});
document.querySelectorAll('dialog').forEach(d=>{
  d.addEventListener('click',e=>{if(e.target===d)d.close();});
});
"""

# ── Source-tag CSS lookup (internal, mirrors SOURCE_TAGS keys) ────────────────
_SRC_CSS: dict[str, str] = {k: v[0] for k, v in SOURCE_TAGS.items()}
_SRC_LBL: dict[str, str] = {k: v[1] for k, v in SOURCE_TAGS.items()}


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _e(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return _html.escape(str(v).strip())


def _t(v, n: int = 90) -> str:
    s = _e(v)
    full = _html.escape(str(v or ""))
    return f'<span title="{full}">{s[:n]}…</span>' if len(s) > n else s


def _nn(v) -> bool:
    return v is not None and not (isinstance(v, float) and pd.isna(v)) and str(v).strip() not in ("", "nan", "None")


def _yr(v) -> str:
    if not _nn(v):
        return ""
    try:
        return str(int(float(str(v).strip())))
    except (ValueError, OverflowError):
        return _e(v)


def _src_tag(src, url: str | None = None) -> str:
    s = str(src or "").lower().strip()
    label = _html.escape(_SRC_LBL.get(s, src or "?"))
    css = _SRC_CSS.get(s, "s-def")
    if url:
        return f'<a href="{_html.escape(str(url))}" target="_blank" class="src {css}" style="text-decoration:none">{label}</a>'
    return f'<span class="src {css}">{label}</span>'


def _status_badge(st_raw) -> str:
    s = str(st_raw or "").lower().strip()
    return f'<span class="badge st-{s}">{_html.escape(s)}</span>' if s else ""


def _type_tag(dc) -> str:
    if not _nn(dc):
        return ""
    parts = str(dc).split("::")
    lbl = parts[-1].strip() if len(parts) > 1 else parts[0].strip()
    return f'<span class="ttype" title="{_e(dc)}">{_html.escape(lbl[:28])}</span>'


def _action_btns(row: dict) -> str:
    parts = []
    for col, lbl, cls in [("item_url", "View", "av"), ("ws_url", "Edit", "ae"), ("wf_url", "Claim", "ac")]:
        u = row.get(col)
        if _nn(u):
            parts.append(f'<a href="{_e(u)}" target="_blank" class="ab {cls}">{lbl}</a>')

    return '<div class="abl">' + "".join(parts) + "</div>" if parts else '<span class="dash">—</span>'


# ── Modal content builders ────────────────────────────────────────────────────

def _meta_content(row: dict) -> str:
    rm = row.get("raw_metadata")
    meta: dict = {}
    sections = DB_META_SECTIONS
    if _nn(rm):
        try:
            meta = json.loads(str(rm))
            sections = RAW_META_SECTIONS
        except Exception:
            pass
    src = meta if meta else row
    parts = [f'<p class="m-ttl">{_t(row.get("title"), 100)}</p>']
    if not meta:
        parts.append('<p class="m-info">Métadonnées complètes disponibles à partir des prochains runs.</p>')
    for sec, keys in sections:
        items = [(k, str(src[k])) for k in keys if k in src and _nn(src.get(k))]
        if not items:
            continue
        parts.append(f'<div class="m-sec">{_html.escape(sec)}</div>')
        for k, v in items:
            if len(v) > 200:
                parts.append(
                    f'<div class="m-row m-row-w">'
                    f'<span class="m-key">{_html.escape(k)}</span>'
                    f'<pre class="m-pre">{_html.escape(v)}</pre>'
                    f'</div>'
                )
            else:
                parts.append(
                    f'<div class="m-row">'
                    f'<span class="m-key">{_html.escape(k)}</span>'
                    f'<span class="m-val">{_e(v)}</span>'
                    f'</div>'
                )
    return "".join(parts)


def _authors_content(row: dict, authors: list) -> str:
    def _v(x: str) -> str:
        return _e(x) if x else ""

    parts = [f'<p class="m-ttl">{_t(row.get("title"), 100)}</p>']
    if not authors:
        parts.append('<p class="m-info">Aucun auteur EPFL réconcilié.</p>')
        return "".join(parts)
    for a in authors:
        sciper = _v(a.get("sciper"))
        orcid  = _v(a.get("orcid"))
        name   = _v(a.get("name")) or sciper or "?"
        status = _v(a.get("epfl_status"))
        pos    = _v(a.get("epfl_position"))
        unit   = _v(a.get("main_unit"))
        weak   = a.get("weak", False)
        sc_lk  = f'<a href="https://people.epfl.ch/{sciper}" target="_blank">{sciper}</a>' if sciper else "—"
        or_lk  = f'<a href="https://orcid.org/{orcid}" target="_blank">{orcid}</a>' if orcid else "—"
        wb     = '<span class="pma-wb">⚠️ Statut faible</span>' if weak else ""
        st_html = f'<span class="pma-st-weak">{status or "—"}</span>' if weak else (status or "—")
        parts.append(
            f'<div class="pma-card{"  pma-weak" if weak else ""}">'
            f'<div class="pma-name">{name} {wb}</div>'
            f'<div class="pma-meta">'
            f'<span><b>SCIPER</b> {sc_lk}</span>'
            f'<span><b>Statut</b> {st_html}</span>'
            f'<span><b>Position</b> {pos or "—"}</span>'
            f'<span><b>Unité</b> {unit or "—"}</span>'
            f'<span><b>ORCID</b> {or_lk}</span>'
            f'</div></div>'
        )
    return '<div class="pma-list">' + "".join(parts) + "</div>"


def _flagged_content(row: dict, ds_base: str) -> str:
    raw  = row.get("flagged_publication")
    note = str(row.get("dedup_note") or "")
    label = DEDUP_LABELS.get(note, note or "Signalé")
    parts = [
        f'<p class="m-ttl">{_t(row.get("title"), 100)}</p>',
        f'<p class="m-note">{_e(label)}</p>',
    ]
    try:
        items = json.loads(str(raw))
    except Exception:
        items = []
    if not isinstance(items, list):
        items = [items]
    for it in items:
        uuid_val = _e(it.get("uuid"))
        doi      = _e(it.get("doi"))
        dct      = _e(it.get("dc_type"))
        ul = f'<a href="{_e(ds_base)}/items/{uuid_val}" target="_blank">{uuid_val}</a>' if uuid_val else "—"
        dl = f'<a href="https://doi.org/{doi}" target="_blank">{doi}</a>' if doi else "—"
        parts.append(
            f'<div class="pmf-card">'
            f'<div><b>UUID</b> {ul}</div>'
            f'<div><b>DOI</b> {dl}</div>'
            f'<div><b>Type</b> <code>{dct}</code></div>'
            f'</div>'
        )
    return '<div class="pmf-list">' + "".join(parts) + "</div>"


# ── Public rendering functions ────────────────────────────────────────────────

def render_pub_component(
    d: pd.DataFrame,
    cols: list,
    authors_by_row: dict,
    ds_base: str,
) -> None:
    """Render publications as a self-contained iframe with inline <dialog> modals."""
    has_run = "run_id" in cols
    dialogs: list[str] = []
    trows:   list[str] = []

    for idx, row in enumerate(d.to_dict("records")):
        auths    = authors_by_row.get(str(row.get("row_id") or ""), [])
        has_flag = _nn(row.get("flagged_publication"))

        for mid, title, content, disabled in [
            (f"pm{idx}", "📋 Métadonnées collectées", _meta_content(row),             False),
            (f"pa{idx}", "👤 Auteurs EPFL",           _authors_content(row, auths),   not auths),
            (f"pf{idx}", "🚩 Doublon Infoscience",    _flagged_content(row, ds_base), not has_flag),
        ]:
            if not disabled:
                dialogs.append(
                    f'<dialog id="{mid}">'
                    f'<div class="mbox">'
                    f'<div class="mhd"><span>{title}</span>'
                    f'<button data-close="{mid}" class="mx">✕</button></div>'
                    f'<div class="mbd">{content}</div>'
                    f'</div></dialog>'
                )

        run_td   = f'<td class="c-run">{_e(row.get("run_id"))}</td>' if has_run else ""
        pdf_tag  = '<span class="pdf-tag">PDF</span>' if row.get("PDF") else ""
        warn_ic  = ' <span title="Statut faible">⚠️</span>' if row.get("⚠️") else ""
        doi_u    = row.get("doi_url")
        src_u    = row.get("src_url")
        doi_val  = str(row.get("doi") or "").strip()
        doi_display = doi_val[:32] + "…" if len(doi_val) > 32 else doi_val
        lks = (
            f'<div class="doi-row">'
            f'<a href="{_e(doi_u)}" target="_blank" class="doi-lk" title="{_html.escape(doi_val)}">'
            f'{_html.escape(doi_display)}</a>'
            f'<button class="copy-btn" data-copy="{_html.escape(doi_val)}" title="Copier le DOI">⎘</button>'
            f'</div>'
        ) if _nn(doi_u) and doi_val else ""
        trows.append(
            f"<tr>"
            f'<td class="c-act">{_action_btns(row)}</td>'
            f"{run_td}"
            f'<td class="c-yr">{_yr(row.get("pub_year"))}</td>'
            f'<td class="c-ttl">'
            f'<div class="ttags">{_src_tag(row.get("source"), src_u if _nn(src_u) else None)}'
            f'{_status_badge(row.get("status"))}{_type_tag(row.get("dc_type"))}</div>'
            f'<span class="ttl">{_t(row.get("title"), 105)}</span>'
            f'</td>'
            f'<td class="c-oa">'
            f'<span class="oa-v">{_e(row.get("OA"))}</span>'
            f'<span class="lic-v">{_t(row.get("Licence"), 20)}</span>'
            f'{pdf_tag}</td>'
            f'<td class="c-auth"><div class="auth-n">{_t(row.get("Auteurs EPFL"), 65)}{warn_ic}</div></td>'
            f'<td class="c-unit">{_t(row.get("Unités"), 30)}</td>'
            f'<td class="c-lk">{lks}</td>'
            f'<td class="c-btn"><button data-modal="pm{idx}" class="mbtn">📋</button></td>'
            + ('<td class="c-btn"><button data-modal="pa' + str(idx) + '" class="mbtn">👤</button></td>'
               if auths else '<td class="c-btn"><button class="mbtn" disabled>👤</button></td>')
            + ('<td class="c-btn"><button data-modal="pf' + str(idx) + '" class="mbtn flag-btn">🚩</button></td>'
               if has_flag else '<td class="c-btn"><button class="mbtn" disabled>🚩</button></td>')
            + "</tr>"
        )

    run_th = "<th>Run</th>" if has_run else ""
    html_doc = (
        "<!DOCTYPE html><html><head>"
        '<meta charset="UTF-8">'
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">'
        f"<style>{_CSS}</style>"
        "</head><body>"
        + "".join(dialogs)
        + '<div class="wrap"><table>'
        + "<thead><tr>"
        + f"<th>Actions</th>{run_th}<th>Année</th>"
        + '<th style="min-width:220px">Titre</th>'
        + "<th>OA / Licence</th><th>Auteurs EPFL</th><th>Unités</th><th>DOI</th>"
        + '<th title="Métadonnées">📋</th><th title="Auteurs EPFL">👤</th><th title="Doublon">🚩</th>'
        + "</tr></thead>"
        + "<tbody>" + "".join(trows) + "</tbody>"
        + "</table></div>"
        + f"<script>{_JS}</script>"
        + "</body></html>"
    )

    b64_src = "data:text/html;base64," + base64.b64encode(html_doc.encode("utf-8")).decode("ascii")
    st.iframe(b64_src, height=max(320, len(d) * 66 + 100))

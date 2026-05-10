import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


# ─── Imports ──────────────────────────────────────────────────────────────────
@app.cell
def _():
    import marimo as mo
    import requests
    import pandas as pd
    import time
    import re
    import io
    from datetime import datetime
    return datetime, io, mo, pd, re, requests, time


# ─── En-tête ──────────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # 🔍 DOI Resolution Checker

    Vérifie si une liste de DOIs est **active et résolue** via le Handle API de doi.org.

    **API :** `https://doi.org/api/handles/{doi}` · **Réf. :** [DOI Resolution Documentation](https://www.doi.org/the-identifier/resources/factsheets/doi-resolution-documentation)

    | Code | État | Description |
    |-----:|------|-------------|
    | `1`   | ✅ Résolu           | Handle trouvé — URL de redirection présente |
    | `100` | ❌ Introuvable      | Handle Not Found — DOI inexistant ou supprimé |
    | `200` | ⚠️ Valeurs absentes | Handle existant, aucune valeur du type demandé |
    | `2`   | ⚠️ Erreur générique | Erreur non spécifiée |
    | `300` | ⏳ Serveur occupé   | Server Too Busy |
    | `400` | 💥 Échec inattendu  | Unexpected Failure |
    | `402` | 🔒 Non autorisé     | Unauthorized |
    | `500` | 🔌 Connexion échouée | Cannot Connect to Handle Server |
    """)


# ─── Widgets d'entrée ─────────────────────────────────────────────────────────
# mo.ui.tabs est l'approche idiomatique pour switcher entre modes dans Marimo.
# tabs.value retourne le label de l'onglet actif et déclenche la réactivité.
# csv_upload et doi_textarea sont définis ici (une seule fois) et embarqués
# directement dans les onglets — pas besoin d'affichage conditionnel séparé.
@app.cell(hide_code=True)
def _(mo):
    csv_upload = mo.ui.file(
        filetypes=[".csv", ".tsv", ".txt"],
        label="Déposer le fichier CSV/TSV",
        multiple=False,
    )
    doi_textarea = mo.ui.text_area(
        value=(
            "10.5075/epfl-thesis-10490\n"
            "10.5075/epfl-thesis-1285\n"
            "10.5075/epfl-thesis-1167\n"
            "10.5075/epfl-thesis-532\n"
            "10.1000/1\n"
            "10.5281/zenodo.9999999999"
        ),
        label="DOIs à vérifier — un par ligne",
        rows=10,
        full_width=True,
    )
    input_tabs = mo.ui.tabs({
        "📂 Fichier CSV / TSV": mo.vstack([
            mo.md("""
**Format attendu** — CSV ou TSV avec au minimum :

| Colonne | Description |
|---------|-------------|
| `id` | Identifiant unique (UUID ou autre) |
| `dc.identifier.doi` | DOI brut, ex. `10.5075/epfl-thesis-1234` |

Séparateur détecté automatiquement (`,` ou `\\t`).
Colonnes supplémentaires conservées dans l'export.
            """),
            csv_upload,
        ]),
        "✏️ Saisie manuelle": mo.vstack([
            mo.md(
                "Entrez un DOI par ligne.  \n"
                "Formats acceptés : brut (`10.xxx/yyy`), `https://doi.org/…`, `doi:…`  \n"
                "L'identifiant sera le numéro de ligne."
            ),
            doi_textarea,
        ]),
    })
    return csv_upload, doi_textarea, input_tabs


# ─── Affichage ────────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(input_tabs, mo):
    mo.vstack([mo.md("## ① Source des DOIs"), input_tabs], gap=1)


# ─── Parsing → liste de dicts {record_id, doi_raw, ...extra} ─────────────────
@app.cell
def _(csv_upload, doi_textarea, input_tabs, mo, pd):
    _TAB_CSV = "📂 Fichier CSV / TSV"

    _parse_error = None
    doi_items    = []

    if input_tabs.value == _TAB_CSV:
        if not csv_upload.value:
            mo.stop(True, mo.callout(
                mo.md("⬆️ Déposez votre fichier CSV/TSV pour continuer."),
                kind="info",
            ))

        _raw_text = csv_upload.value[0].contents.decode("utf-8-sig")
        _sep      = "\t" if "\t" in _raw_text.split("\n")[0] else ","

        try:
            _df = pd.read_csv(
                pd.io.common.StringIO(_raw_text),
                sep=_sep,
                dtype=str,
            ).fillna("")

            _missing = [c for c in ("id", "dc.identifier.doi") if c not in _df.columns]
            if _missing:
                _parse_error = (
                    f"Colonnes manquantes : {_missing}. "
                    f"Colonnes trouvées : {list(_df.columns)}"
                )
            else:
                _extra_cols = [
                    c for c in _df.columns
                    if c not in ("id", "dc.identifier.doi")
                ]
                for _, _row in _df.iterrows():
                    _raw_doi = _row["dc.identifier.doi"].strip()
                    if not _raw_doi:
                        continue
                    _item = {
                        "record_id": _row["id"].strip(),
                        "doi_raw":   _raw_doi,
                    }
                    for _c in _extra_cols:
                        _item[_c] = _row[_c]
                    doi_items.append(_item)

        except Exception as _e:
            _parse_error = f"Erreur de lecture : {type(_e).__name__}: {_e}"

    else:
        _lines = [l.strip() for l in doi_textarea.value.splitlines() if l.strip()]
        doi_items = [
            {"record_id": str(i + 1), "doi_raw": line}
            for i, line in enumerate(_lines)
        ]

    if _parse_error:
        mo.stop(True, mo.callout(
            mo.md(f"❌ **Erreur de parsing** : {_parse_error}"),
            kind="danger",
        ))

    doi_items
    return (doi_items,)


# ─── Aperçu ───────────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(doi_items, mo):
    mo.stop(not doi_items)
    mo.callout(
        mo.md(f"**{len(doi_items)} enregistrement(s)** prêt(s) à vérifier."),
        kind="success",
    )


# ─── Paramètres & déclenchement ───────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    delay_slider = mo.ui.slider(
        start=0.0, stop=2.0, step=0.1, value=0.3,
        label="Délai entre requêtes (s)",
    )
    timeout_slider = mo.ui.slider(
        start=3, stop=30, step=1, value=10,
        label="Timeout HTTP (s)",
    )
    verify_switch = mo.ui.switch(
        value=False,
        label="Vérifier l'accessibilité de l'URL cible (HEAD — plus lent)",
    )
    run_button = mo.ui.run_button(label="🚀 Lancer la vérification")

    mo.vstack([
        mo.md("## ② Paramètres"),
        mo.hstack([
            mo.vstack([delay_slider, timeout_slider, verify_switch]),
            mo.vstack([run_button], align="end"),
        ], justify="space-between", gap=2),
    ], gap=1)


# ─── Fonctions métier ─────────────────────────────────────────────────────────
@app.cell
def _(re, requests):
    HANDLE_RESPONSE_CODES = {
        1:   ("✅", "Résolu",             "Handle trouvé — valeurs retournées"),
        2:   ("⚠️", "Erreur générique",  "Erreur non spécifiée"),
        100: ("❌", "Introuvable",        "Handle Not Found — DOI inexistant"),
        200: ("⚠️", "Valeurs absentes",  "Handle existant, aucune valeur du type demandé"),
        300: ("⏳", "Serveur occupé",    "Server Too Busy"),
        301: ("⚠️", "Erreur protocole",  "Protocol Error"),
        302: ("⚠️", "Non supporté",      "Operation Not Supported"),
        400: ("💥", "Échec inattendu",   "Unexpected Failure"),
        401: ("⚠️", "Format invalide",   "Message Format Error"),
        402: ("🔒", "Non autorisé",      "Unauthorized"),
        500: ("🔌", "Connexion échouée", "Cannot Connect to Handle Server"),
    }

    def normalize_doi(raw: str) -> str:
        s = raw.strip()
        s = re.sub(r'^https?://(dx\.)?doi\.org/', '', s, flags=re.IGNORECASE)
        s = re.sub(r'^doi:', '', s, flags=re.IGNORECASE)
        return s.strip()

    def extract_url(values: list) -> str | None:
        for v in values:
            if v.get("type") == "URL":
                return v.get("data", {}).get("value")
        return None

    def check_url_reachable(url: str, timeout: int = 5) -> str:
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True)
            return f"HTTP {r.status_code}"
        except requests.exceptions.ConnectionError:
            return "Connexion refusée"
        except requests.exceptions.Timeout:
            return "Timeout"
        except Exception as e:
            return f"{type(e).__name__}"

    def check_doi(record_id: str, raw_doi: str,
                  extra: dict | None = None,
                  verify_url: bool = False,
                  timeout: int = 10) -> dict:
        doi     = normalize_doi(raw_doi)
        api_url = f"https://doi.org/api/handles/{doi}"
        result  = {
            "record_id":      record_id,
            "doi_input":      raw_doi,
            "doi_normalized": doi,
            "response_code":  None,
            "emoji":          "❓",
            "statut":         "Inconnu",
            "description":    "",
            "url_cible":      None,
            "url_accessible": None,
            "timestamp":      None,
            "http_status":    None,
            "erreur":         None,
        }
        if extra:
            result.update(extra)
        try:
            resp = requests.get(api_url, timeout=timeout)
            result["http_status"] = resp.status_code
            data = resp.json()
            rc   = data.get("responseCode")
            result["response_code"] = rc
            if rc in HANDLE_RESPONSE_CODES:
                emoji, statut, desc = HANDLE_RESPONSE_CODES[rc]
            else:
                emoji, statut, desc = ("❓", "Code inconnu",
                                       f"responseCode={rc} non documenté")
            result.update({"emoji": emoji, "statut": statut, "description": desc})
            values = data.get("values", [])
            if values:
                result["url_cible"] = extract_url(values)
                for v in values:
                    if v.get("type") == "URL":
                        result["timestamp"] = v.get("timestamp")
                        break
            if data.get("message"):
                result["description"] += f" | {data['message']}"
            if verify_url and result["url_cible"]:
                result["url_accessible"] = check_url_reachable(result["url_cible"])
        except requests.exceptions.Timeout:
            result.update({"emoji": "⏳", "statut": "Timeout",
                           "erreur": f"Timeout après {timeout}s"})
        except requests.exceptions.ConnectionError as e:
            result.update({"emoji": "🔌", "statut": "Connexion échouée",
                           "erreur": str(e)[:100]})
        except requests.exceptions.JSONDecodeError:
            result.update({"emoji": "⚠️", "statut": "Réponse invalide",
                           "erreur": f"HTTP {result['http_status']} — non JSON"})
        except Exception as e:
            result.update({"emoji": "💥", "statut": "Erreur inattendue",
                           "erreur": f"{type(e).__name__}: {e}"})
        return result

    return HANDLE_RESPONSE_CODES, check_doi, normalize_doi


# ─── Exécution ────────────────────────────────────────────────────────────────
@app.cell
def _(
    check_doi,
    datetime,
    delay_slider,
    doi_items,
    mo,
    run_button,
    time,
    timeout_slider,
    verify_switch,
):
    mo.stop(not run_button.value, mo.callout(
        mo.md("⬆️ Cliquez sur **🚀 Lancer la vérification** pour démarrer."),
        kind="info",
    ))

    total   = len(doi_items)
    results = []

    with mo.status.progress_bar(
        total=total,
        title="Vérification en cours…",
        subtitle="Interrogation de doi.org/api/handles/",
    ) as bar:
        for i, item in enumerate(doi_items, start=1):
            record_id = item["record_id"]
            raw_doi   = item["doi_raw"]
            extra     = {k: v for k, v in item.items()
                         if k not in ("record_id", "doi_raw")}
            bar.update(
                increment=1,
                title=f"[{i}/{total}] {record_id[:36]} — {raw_doi[:45]}",
            )
            _r = check_doi(
                record_id=record_id,
                raw_doi=raw_doi,
                extra=extra or None,
                verify_url=verify_switch.value,
                timeout=timeout_slider.value,
            )
            results.append(_r)
            if i < total:
                time.sleep(delay_slider.value)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return results, run_ts, total


# ─── Résumé statistique ───────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo, results, run_ts, total):
    n_ok    = sum(1 for r in results if r["response_code"] == 1)
    n_ko    = sum(1 for r in results if r["response_code"] == 100)
    n_other = total - n_ok - n_ko

    mo.vstack([
        mo.md("## ③ Résultats"),
        mo.hstack([
            mo.stat(label="Total",              value=str(total),   bordered=True),
            mo.stat(label="✅ Résolus",         value=str(n_ok),    bordered=True),
            mo.stat(label="❌ Introuvables",    value=str(n_ko),    bordered=True),
            mo.stat(label="⚠️ Autres/erreurs", value=str(n_other), bordered=True),
            mo.stat(label="Exécuté le",         value=run_ts,       bordered=True),
        ], gap=1),
    ], gap=1)


# ─── Tableau des résultats ────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo, results, verify_switch):
    def _row_color(rc):
        if rc == 1:   return "#d4edda"
        if rc == 100: return "#f8d7da"
        if rc in (300, 301, 302, 400, 401, 402, 500): return "#fff3cd"
        if rc == 200: return "#fff0d0"
        return "#f2f2f2"

    _CORE = {"record_id", "doi_input", "doi_normalized", "response_code",
             "emoji", "statut", "description", "url_cible", "url_accessible",
             "timestamp", "http_status", "erreur"}
    extra_keys = [k for k in results[0] if k not in _CORE] if results else []

    rows = []
    for _r in results:
        rc  = _r["response_code"]
        bg  = _row_color(rc)
        url = _r["url_cible"]
        url_cell = (
            f'<a href="{url}" target="_blank" '
            f'style="word-break:break-all;font-size:11px">{url}</a>'
            if url else "—"
        )
        row = {"ID": f'<code style="font-size:10px;color:#555">{_r["record_id"]}</code>'}
        for k in extra_keys:
            row[k] = _r.get(k, "—") or "—"
        row.update({
            "État":          _r["emoji"],
            "DOI":           f'<code style="font-size:11px">{_r["doi_normalized"]}</code>',
            "Code":          str(rc) if rc is not None else "—",
            "Statut":        _r["statut"],
            "URL cible":     url_cell,
            "Enregistré le": _r["timestamp"] or "—",
            "HTTP API":      str(_r["http_status"]) if _r["http_status"] else "—",
            "Erreur":        _r["erreur"] or "—",
        })
        if verify_switch.value:
            row["URL accessible"] = _r["url_accessible"] or "—"
        rows.append((row, bg))

    if not rows:
        mo.md("_Aucun résultat._")
    else:
        headers = list(rows[0][0].keys())
        th_html = "".join(
            f'<th style="background:#343a40;color:#fff;padding:8px 12px;'
            f'text-align:left;font-size:12px;white-space:nowrap">{h}</th>'
            for h in headers
        )
        tbody_html = ""
        for row, bg in rows:
            tds = "".join(
                f'<td style="background:{bg};padding:6px 10px;font-size:12px;'
                f'border-bottom:1px solid #dee2e6;vertical-align:middle">{v}</td>'
                for v in row.values()
            )
            tbody_html += f"<tr>{tds}</tr>"

        mo.Html(f"""
        <div style="overflow-x:auto;margin-top:8px">
          <table style="border-collapse:collapse;width:100%;font-family:sans-serif">
            <thead><tr>{th_html}</tr></thead>
            <tbody>{tbody_html}</tbody>
          </table>
        </div>
        """)


# ─── Export CSV ───────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(datetime, io, mo, pd, results):
    mo.md("## ④ Export")

    df_e = pd.DataFrame(results)
    _BASE = ["record_id", "doi_normalized", "doi_input", "response_code",
             "statut", "description", "url_cible", "timestamp",
             "http_status", "url_accessible", "erreur"]
    _KNOWN = set(_BASE) | {"emoji"}
    _extra_export = [c for c in df_e.columns if c not in _KNOWN]
    _ordered = (
        ["record_id"] + _extra_export
        + [c for c in _BASE[1:] if c in df_e.columns]
    )
    df_e = df_e[[c for c in _ordered if c in df_e.columns]]

    buf = io.StringIO()
    df_e.to_csv(buf, index=False)
    csv_bytes = buf.getvalue().encode("utf-8-sig")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    mo.download(
        data=csv_bytes,
        filename=f"doi_resolution_check_{ts}.csv",
        mimetype="text/csv",
        label="⬇️ Télécharger CSV (UTF-8 BOM, compatible Excel)",
    )


if __name__ == "__main__":
    app.run()

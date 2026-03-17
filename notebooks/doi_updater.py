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
    # ✏️ DataCite DOI URL Updater

    Met à jour l'URL cible de DOIs DataCite via le REST API (PUT `/dois/{id}`).

    **Ref. :** [Updating DOIs with the REST API](https://support.datacite.org/docs/updating-metadata-with-the-rest-api)

    > ⚠️ **Cette opération modifie des données en production.** Utilisez le mode **Test** pour valider votre workflow avant toute mise à jour réelle.

    ---

    **Workflow recommandé :**
    1. Exporter le rapport du *DOI Resolution Checker* (colonne `uri_match = ⚠️ Différent`)
    2. Charger ce CSV ici — la colonne `dc.identifier.uri` sera utilisée comme **nouvelle URL cible**
    3. Prévisualiser, puis lancer les mises à jour
    """)


# ─── Credentials & environnement ──────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    mo.md("## ① Authentification DataCite")


@app.cell(hide_code=True)
def _(mo):
    repo_id_input = mo.ui.text(
        placeholder="ex. EPFL.INFOSCIENCE",
        label="**Repository ID** (DataCite Fabrica username)",
        full_width=True,
    )
    password_input = mo.ui.text(
        placeholder="••••••••",
        label="**Password**",
        kind="password",
        full_width=True,
    )
    test_mode_switch = mo.ui.switch(
        value=True,
        label="Mode **Test** (`api.test.datacite.org`) — désactiver pour la production",
    )
    delay_slider = mo.ui.slider(
        start=0.5, stop=5.0, step=0.5, value=1.0,
        label="Délai entre requêtes (s) — respecter le rate limit DataCite",
    )

    mo.vstack([
        mo.hstack([repo_id_input, password_input], gap=2),
        mo.hstack([test_mode_switch, delay_slider], gap=4),
    ], gap=2)


# ─── Widgets d'entrée (tabs CSV / manuel) ────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo):
    csv_upload = mo.ui.file(
        filetypes=[".csv", ".tsv", ".txt"],
        label="Déposer le fichier CSV/TSV",
        multiple=False,
    )
    manual_textarea = mo.ui.text_area(
        value=(
            "10.5075/epfl-thesis-1285 → https://infoscience.epfl.ch/handle/20.500.14299/XXXXX\n"
            "10.5075/epfl-thesis-1167 → https://infoscience.epfl.ch/handle/20.500.14299/YYYYY"
        ),
        label="Un DOI et son URL cible par ligne — séparateur : ` → ` (flèche)",
        rows=8,
        full_width=True,
    )
    input_tabs = mo.ui.tabs({
        "📂 Fichier CSV / TSV": mo.vstack([
            mo.md("""
**Colonnes requises :**

| Colonne | Rôle |
|---------|------|
| `dc.identifier.doi` | DOI à mettre à jour |
| `dc.identifier.uri` | Nouvelle URL cible |

Utilisez directement l'export du *DOI Resolution Checker* (filtré sur `uri_match = ⚠️ Différent`).
            """),
            csv_upload,
        ]),
        "✏️ Saisie manuelle": mo.vstack([
            mo.md(
                "Format : `10.xxxx/yyyy → https://ma-nouvelle-url.org/...`  \n"
                "Un enregistrement par ligne. Séparateur : ` → ` (espace + flèche + espace).  \n"
                "Idéal pour tester quelques DOIs avant un batch complet."
            ),
            manual_textarea,
        ]),
    })
    return csv_upload, input_tabs, manual_textarea


# ─── Affichage ──────────────────────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(input_tabs, mo):
    mo.vstack([mo.md("## ② DOIs à mettre à jour"), input_tabs], gap=1)


# ─── Parsing → update_items ─────────────────────────────────────────────────────────────────
@app.cell
def _(csv_upload, input_tabs, manual_textarea, mo, pd, re):
    _TAB_CSV = "📂 Fichier CSV / TSV"
    _parse_error = None
    update_items = []

    if input_tabs.value == _TAB_CSV:
        # ── Mode CSV ──────────────────────────────────────────────────────────────────
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

            _required = ["dc.identifier.doi", "dc.identifier.uri"]
            _missing  = [c for c in _required if c not in _df.columns]
            if _missing:
                _parse_error = (
                    f"Colonnes manquantes : {_missing}. "
                    f"Colonnes trouvées : {list(_df.columns)}"
                )
            else:
                for _, _row in _df.iterrows():
                    _doi = _row["dc.identifier.doi"].strip()
                    _uri = _row["dc.identifier.uri"].strip()
                    if not _doi or not _uri:
                        continue
                    _doi_norm = re.sub(
                        r'^https?://(dx\.)?doi\.org/', '', _doi, flags=re.IGNORECASE
                    ).strip()
                    _doi_norm = re.sub(r'^doi:', '', _doi_norm, flags=re.IGNORECASE).strip()
                    _current_url = _row.get("url_cible", "").strip()
                    update_items.append({
                        "doi":         _doi_norm,
                        "new_url":     _uri,
                        "current_url": _current_url or "—",
                    })

        except Exception as _e:
            _parse_error = f"Erreur de lecture : {type(_e).__name__}: {_e}"

    else:
        # ── Mode manuel ────────────────────────────────────────────────────────────
        for _line in manual_textarea.value.splitlines():
            _line = _line.strip()
            if not _line or " → " not in _line:
                continue
            _parts = _line.split(" → ", 1)
            if len(_parts) != 2:
                continue
            _doi_raw, _uri = _parts[0].strip(), _parts[1].strip()
            if not _doi_raw or not _uri:
                continue
            _doi_norm = re.sub(
                r'^https?://(dx\.)?doi\.org/', '', _doi_raw, flags=re.IGNORECASE
            ).strip()
            _doi_norm = re.sub(r'^doi:', '', _doi_norm, flags=re.IGNORECASE).strip()
            update_items.append({
                "doi":         _doi_norm,
                "new_url":     _uri,
                "current_url": "—",
            })

        if not update_items and manual_textarea.value.strip():
            _parse_error = (
                "Aucune ligne valide trouvée. "
                "Format attendu : `10.xxxx/yyyy → https://...`"
            )

    if _parse_error:
        mo.stop(True, mo.callout(
            mo.md(f"❌ **Erreur de parsing** : {_parse_error}"),
            kind="danger",
        ))

    update_items
    return (update_items,)



# ─── Prévisualisation du plan de mise à jour ──────────────────────────────────
@app.cell(hide_code=True)
def _(mo, update_items):
    mo.stop(not update_items)

    mo.md("## ③ Plan de mise à jour")

    _rows_html = ""
    for _item in update_items:
        _rows_html += (
            f"<tr>"
            f'<td style="padding:6px 10px;font-size:12px;border-bottom:1px solid #dee2e6">'
            f'<code>{_item["doi"]}</code></td>'
            f'<td style="padding:6px 10px;font-size:12px;border-bottom:1px solid #dee2e6;'
            f'color:#856404">{_item["current_url"]}</td>'
            f'<td style="padding:6px 10px;font-size:12px;border-bottom:1px solid #dee2e6;'
            f'color:#155724">{_item["new_url"]}</td>'
            f"</tr>"
        )

    mo.Html(f"""
    <div style="overflow-x:auto">
      <table style="border-collapse:collapse;width:100%;font-family:sans-serif">
        <thead>
          <tr>
            <th style="background:#343a40;color:#fff;padding:8px 12px;text-align:left;font-size:12px">DOI</th>
            <th style="background:#343a40;color:#fff;padding:8px 12px;text-align:left;font-size:12px">URL actuelle (Handle)</th>
            <th style="background:#343a40;color:#fff;padding:8px 12px;text-align:left;font-size:12px">Nouvelle URL cible (dc.identifier.uri)</th>
          </tr>
        </thead>
        <tbody>{_rows_html}</tbody>
      </table>
    </div>
    <p style="margin-top:8px;font-size:12px;color:#6c757d">
      {len(update_items)} DOI(s) à mettre à jour.
    </p>
    """)


# ─── Confirmation & lancement ─────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo, repo_id_input, test_mode_switch, update_items):
    mo.stop(not update_items)

    _env_label = "🧪 TEST (`api.test.datacite.org`)" if test_mode_switch.value \
                 else "🔴 PRODUCTION (`api.datacite.org`)"
    _env_kind  = "warn" if not test_mode_switch.value else "info"
    _cred_ok   = bool(repo_id_input.value.strip())

    mo.vstack([
        mo.md("## ④ Lancement"),
        mo.callout(
            mo.md(f"**Environnement cible :** {_env_label}"),
            kind=_env_kind,
        ),
        mo.callout(
            mo.md("⚠️ **Credentials manquants** — saisissez votre Repository ID et password."),
            kind="danger",
        ) if not _cred_ok else mo.md(""),
    ], gap=1)


@app.cell(hide_code=True)
def _(mo, update_items):
    mo.stop(not update_items)
    run_button = mo.ui.run_button(
        label="🚀 Lancer les mises à jour",
        disabled=False,
    )
    run_button


# ─── Exécution ────────────────────────────────────────────────────────────────
@app.cell
def _(
    datetime,
    delay_slider,
    mo,
    password_input,
    repo_id_input,
    requests,
    run_button,
    test_mode_switch,
    time,
    update_items,
):
    mo.stop(not run_button.value, mo.callout(
        mo.md("⬆️ Vérifiez le plan ci-dessus puis cliquez sur **🚀 Lancer les mises à jour**."),
        kind="info",
    ))
    mo.stop(
        not repo_id_input.value.strip() or not password_input.value.strip(),
        mo.callout(
            mo.md("❌ **Repository ID et/ou password manquants.**"),
            kind="danger",
        ),
    )

    _base_url = (
        "https://api.test.datacite.org/dois/"
        if test_mode_switch.value
        else "https://api.datacite.org/dois/"
    )
    _auth    = (repo_id_input.value.strip(), password_input.value.strip())
    _headers = {
        "Content-Type": "application/vnd.api+json",
        "Accept":       "application/vnd.api+json",
        "User-Agent":   "MarimoDoiUpdater/1.0 (marimo notebook; doi url update tool)",
    }

    total   = len(update_items)
    results = []

    with mo.status.progress_bar(
        total=total,
        title="Mise à jour en cours…",
        subtitle=f"{'TEST' if test_mode_switch.value else 'PRODUCTION'} — api DataCite",
    ) as bar:
        for i, item in enumerate(update_items, start=1):
            _doi     = item["doi"]
            _new_url = item["new_url"]

            bar.update(
                increment=1,
                title=f"[{i}/{total}] {_doi}",
            )

            _payload = {
                "data": {
                    "type": "dois",
                    "attributes": {
                        "url": _new_url,
                    },
                }
            }

            _result = {
                "doi":         _doi,
                "new_url":     _new_url,
                "current_url": item["current_url"],
                "http_status": None,
                "statut":      "—",
                "emoji":       "❓",
                "erreur":      None,
                "url_retour":  None,
            }

            try:
                _resp = requests.put(
                    f"{_base_url}{_doi}",
                    json=_payload,
                    auth=_auth,
                    headers=_headers,
                    timeout=15,
                )
                _result["http_status"] = _resp.status_code

                if _resp.status_code == 200:
                    _data = _resp.json()
                    _url_retour = (
                        _data.get("data", {})
                            .get("attributes", {})
                            .get("url", "")
                    )
                    _result.update({
                        "emoji":      "✅",
                        "statut":     "Mis à jour",
                        "url_retour": _url_retour,
                    })
                elif _resp.status_code == 401:
                    _result.update({
                        "emoji":  "🔒",
                        "statut": "Non autorisé",
                        "erreur": "Vérifiez Repository ID et password",
                    })
                elif _resp.status_code == 403:
                    _result.update({
                        "emoji":  "🔒",
                        "statut": "Accès refusé",
                        "erreur": "Ce DOI n'appartient pas à votre repository",
                    })
                elif _resp.status_code == 404:
                    _result.update({
                        "emoji":  "❌",
                        "statut": "DOI introuvable",
                        "erreur": f"404 — DOI absent de {'test' if test_mode_switch.value else 'prod'}",
                    })
                elif _resp.status_code == 422:
                    _detail = ""
                    try:
                        _detail = str(_resp.json().get("errors", ""))
                    except Exception:
                        pass
                    _result.update({
                        "emoji":  "⚠️",
                        "statut": "Données invalides",
                        "erreur": f"422 Unprocessable Entity — {_detail[:120]}",
                    })
                else:
                    _result.update({
                        "emoji":  "⚠️",
                        "statut": f"HTTP {_resp.status_code}",
                        "erreur": _resp.text[:120],
                    })

            except requests.exceptions.Timeout:
                _result.update({
                    "emoji":  "⏳",
                    "statut": "Timeout",
                    "erreur": "Pas de réponse dans les 15s",
                })
            except requests.exceptions.ConnectionError as e:
                _result.update({
                    "emoji":  "🔌",
                    "statut": "Connexion échouée",
                    "erreur": str(e)[:100],
                })
            except Exception as e:
                _result.update({
                    "emoji":  "💥",
                    "statut": "Erreur inattendue",
                    "erreur": f"{type(e).__name__}: {e}",
                })

            results.append(_result)

            if i < total:
                time.sleep(delay_slider.value)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return results, run_ts, total


# ─── Résumé ───────────────────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo, results, run_ts, test_mode_switch, total):
    n_ok    = sum(1 for r in results if r["http_status"] == 200)
    n_err   = total - n_ok
    _env    = "TEST" if test_mode_switch.value else "PRODUCTION"

    mo.vstack([
        mo.md("## ⑤ Résultats"),
        mo.hstack([
            mo.stat(label="Total traités",   value=str(total),  bordered=True),
            mo.stat(label="✅ Mis à jour",   value=str(n_ok),   bordered=True),
            mo.stat(label="❌ Erreurs",      value=str(n_err),  bordered=True),
            mo.stat(label="Environnement",   value=_env,        bordered=True),
            mo.stat(label="Exécuté le",      value=run_ts,      bordered=True),
        ], gap=1),
    ], gap=1)


# ─── Tableau des résultats ────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(mo, results):
    def _row_color(r):
        if r["http_status"] == 200: return "#d4edda"
        if r["http_status"] in (401, 403): return "#f8d7da"
        if r["http_status"] == 404: return "#f8d7da"
        if r["http_status"] == 422: return "#fff3cd"
        if r["erreur"]:             return "#fff3cd"
        return "#f2f2f2"

    _th = lambda h: (
        f'<th style="background:#343a40;color:#fff;padding:8px 12px;'
        f'text-align:left;font-size:12px;white-space:nowrap">{h}</th>'
    )
    _headers = ["État", "DOI", "HTTP", "Statut", "URL confirmée (retour API)", "Erreur"]
    _head = "".join(_th(h) for h in _headers)

    _body = ""
    for _r in results:
        _bg   = _row_color(_r)
        _url_r = (
            f'<a href="{_r["url_retour"]}" target="_blank" '
            f'style="font-size:11px;word-break:break-all">{_r["url_retour"]}</a>'
            if _r["url_retour"] else "—"
        )
        _cells = [
            _r["emoji"],
            f'<code style="font-size:11px">{_r["doi"]}</code>',
            str(_r["http_status"]) if _r["http_status"] else "—",
            _r["statut"],
            _url_r,
            _r["erreur"] or "—",
        ]
        _tds = "".join(
            f'<td style="background:{_bg};padding:6px 10px;font-size:12px;'
            f'border-bottom:1px solid #dee2e6;vertical-align:middle">{c}</td>'
            for c in _cells
        )
        _body += f"<tr>{_tds}</tr>"

    mo.Html(f"""
    <div style="overflow-x:auto;margin-top:8px">
      <table style="border-collapse:collapse;width:100%;font-family:sans-serif">
        <thead><tr>{_head}</tr></thead>
        <tbody>{_body}</tbody>
      </table>
    </div>
    """)


# ─── Export rapport CSV ───────────────────────────────────────────────────────
@app.cell(hide_code=True)
def _(datetime, io, mo, pd, results, test_mode_switch):
    mo.md("## ⑥ Export du rapport")

    _df = pd.DataFrame(results).drop(columns=["emoji"], errors="ignore")
    _df.insert(0, "environnement",
               "test" if test_mode_switch.value else "production")

    _buf = io.StringIO()
    _df.to_csv(_buf, index=False)
    _csv_bytes = _buf.getvalue().encode("utf-8-sig")

    _ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    _env = "test" if test_mode_switch.value else "prod"

    mo.download(
        data=_csv_bytes,
        filename=f"doi_update_report_{_env}_{_ts}.csv",
        mimetype="text/csv",
        label="⬇️ Télécharger le rapport CSV",
    )


if __name__ == "__main__":
    app.run()

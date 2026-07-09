# Infoscience Import Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-007480.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)

Automated harvesting, deduplication, enrichment, and loading of publication data from multiple external sources into **[Infoscience](https://infoscience.epfl.ch) / [DSpace-CRIS](https://wiki.lyrasis.org/display/DSDOC7x)**.

**Sources:** [Scopus](https://dev.elsevier.com/) · [Web of Science](https://developer.clarivate.com/) · [Crossref](https://www.crossref.org/documentation/retrieve-metadata/rest-api/) · [OpenAlex](https://docs.openalex.org/) · [DataCite](https://support.datacite.org/docs/api) · [Zenodo](https://developers.zenodo.org/) · [EPO OPS](https://developers.epo.org/)

Two companion subsystems share the same codebase and infrastructure: **Researcher Monitor** (per-researcher publication tracking and gap analysis against Infoscience) and **OA Monitor** (institution-wide Open Access compliance monitoring against the Swiss NOAM typology, plus Read & Publish / APC tracking).

---

## Table of contents

1. [What it does](#what-it-does)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Quick start](#quick-start)
5. [Streamlit supervision UI](#streamlit-supervision-ui)
6. [Authentication](#authentication)
7. [Scheduled runs](#scheduled-runs)
8. [CLI reference](#cli-reference)
9. [Researcher Monitor](#researcher-monitor)
10. [OA Monitor](#oa-monitor)
11. [Environments](#environments-dev--test--prod)
12. [Environment variables](#environment-variables)
13. [Architecture](#architecture)
14. [Output structure](#output-structure)
15. [Incremental logic](#incremental-logic)
16. [License](#license)
17. [Citation](#citation)

---

## What it does

The pipeline runs a linear sequence of stages on every execution:

| Stage | Description |
|---|---|
| **Harvest** | Queries each enabled source API within the configured time window |
| **Deduplicate** | Cross-source dedup (DOI, then title + year with type-aware rules for preprints and datasets), then type-scoped dedup against existing Infoscience items; ambiguous cases are flagged and forwarded to the DSpace workspace |
| **Enrich** | EPFL author reconciliation (People API, ORCID); OA/full-text metadata ([Unpaywall](https://unpaywall.org/), OpenAlex) |
| **Load** | Builds DSpace-CRIS item payloads and ingests them (skipped in `--dry-run`) |
| **Report** | Generates a timestamped Excel report and optionally sends it by email |
| **Persist** | Writes run history, per-source stats, publications, and EPFL author data to [DuckDB](https://duckdb.org/) |

The pipeline is **stateless**: incrementality comes from the sliding time window and deduplication against what is already in Infoscience.

---

## Prerequisites

- Python **3.11+**
- A `.env.dev` / `.env.test` / `.env.prod` credentials file at the project root (see [Environment variables](#environment-variables))
- Network access to the APIs you intend to harvest

---

## Installation

```bash
git clone <repo-url> && cd infoscience-imports
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Create your first credentials file
cp .sample.env .env.dev   # edit and fill in values
```

---

## Quick start

```bash
# Dry-run on dev — harvests and enriches but does not load into DSpace
python3 data_pipeline/main.py --env dev --dry-run --no-email -vv

# Real run on dev with a 7-day window
python3 data_pipeline/main.py --env dev --window-days 7

# Production run
python3 data_pipeline/main.py --env prod
```

---

## Streamlit supervision UI

A web dashboard for monitoring run history, browsing and curating publications, and launching pipeline runs without touching the CLI.

```bash
./run_ui.sh          # default port 8500
./run_ui.sh 8502     # custom port
# or directly:
streamlit run app.py
```

### Pages

| Page | Role | Description |
|---|---|---|
| 🏠 Tableau de bord | all | KPIs, 30-day trend chart, recent runs, per-source breakdown, charts by type / OA status / year / unit / journal / PDF proportion |
| 🚀 Lancer un run | admin, curator | Form to configure and launch the pipeline; live log streaming with stop button |
| ⏰ Programmation | admin | Create and manage scheduled runs; cron-based triggers; enable/disable toggle; run-now button; scheduler status indicator |
| 📋 Publications | all | Paginated, filterable datatable with inline modals; OA / licence / PDF badges; EPFL author + unit aggregation; weak-status flag; direct links to DSpace workspace, workflow (`mydspace`), and published item; CSV and Excel report download |
| 📊 Statistiques | all | Per-run funnel by source, publication type breakdown, EPFL author and unit tabs |
| 🧑‍🔬 Chercheurs | all | Researcher Monitor UI — registry (search/filter by unit, school, ORCID/OpenAlex/Infoscience linkage), per-researcher gap analysis, manual sync/harvest/analyze actions with live logs (see [Researcher Monitor](#researcher-monitor)) |
| 🔎 OA Monitor | all | Harvest, enrichment, R&P (Read & Publish/APC) tracking, and NOAM export UI (see [OA Monitor](#oa-monitor)) |
| 🧹 Nettoyage | admin, curator | Search and bulk-remove stray workspace/workflow items directly in DSpace (reject to draft or delete permanently) |
| ⚙️ Configuration | admin | Environment variable status read directly from `.env.{env}` ; DuckDB file path and size; `.env` template |
| 📖 Aide | all | Curator guide, rendered from a local Markdown documentation file |

### Dashboard charts

The dashboard includes six analytical visualisations, all scoped to the selected run or global:

- **Types de documents** — horizontal bar, top-15
- **Statut Open Access** — donut (OA + PDF / OA sans PDF / OA non-libre / Non-OA / Non défini)
- **Publications par année** — bar chart by publication year
- **Proportion PDF récupéré** — donut with summary count
- **Top unités EPFL** — horizontal bar, top-15
- **Top journaux** — horizontal bar, top-15

### Publication curation features

The Publications page supports detailed curation:

- **OA column** — `OA` / `Non-OA` / `Non-libre` (publisher-specific licences are never treated as OA)
- **Licence column** — CC-BY variant displayed (e.g. `CC-BY-NC-ND`); prefix-matched so all `cc-*` variants are covered
- **PDF ✓ column** — checkbox; only `True` when a valid PDF was retrieved under an open licence
- **Auteurs EPFL** — reconciled EPFL authors with status/position; for rejected publications, shows pre-detected unreconciled authors
- **Unités** — EPFL units with type in parentheses
- **⚠️ column** — flags publications where *all* matched EPFL authors have a "weak" status (Hôte, Hors EPFL, Étudiant, or Personnel with non-permanent position)
- **Note dédup** (`dedup_note`) — set when a record was let through despite a potential conflict in Infoscience; possible values: `supersedes_preprint`, `published_version_exists`, `cross_type_doi`, `dataset_in_other_collection`
- **🚩 Doublon Infoscience** (`flagged_publication`) — JSON list of existing Infoscience items that triggered the flag, each with `uuid`, `doi`, and `dc_type`; use the UUID to locate the item directly in Infoscience

Use the **Signalement dédup** filter (options: *Tous* / *🚩 Flaggés* / specific note value) to isolate flagged records for curation. Flagged records also appear in the dedicated **Flagged Publications** sheet of the Excel report.

The environment selector in the sidebar switches between `dev`, `test`, and `prod` — the choice is persisted and automatically passed to any run launched from the UI.

---

## Authentication

The Streamlit UI requires login. Three roles are available:

| Role | Pages |
|---|---|
| `admin` | All pages |
| `curator` | Tableau de bord · Lancer un run · Publications · Statistiques · Chercheurs · OA Monitor · Nettoyage · Aide |
| `reporting` | Tableau de bord · Publications · Statistiques · Chercheurs · OA Monitor · Aide |

The CLI pipeline is **not** protected by authentication — credentials are only required for the web UI.

### Setup

```bash
# Create the first users (passwords are prompted interactively)
python -m ui.auth add admin    admin
python -m ui.auth add curator  curator
python -m ui.auth add reporter reporting

# Other management commands
python -m ui.auth list
python -m ui.auth passwd <username>
python -m ui.auth remove <username>
```

Credentials are stored as [bcrypt](https://pypi.org/project/bcrypt/) hashes in `.streamlit/auth.yaml` (gitignored). Copy `.streamlit/auth.yaml.example` as a reference for the file structure.

---

## Scheduled runs

The **⏰ Programmation** page (admin only) lets you define pipeline runs that fire automatically on a cron schedule, without manual intervention.

### How it works

`run_ui.sh` starts `scheduler.py` as a background daemon alongside Streamlit. The daemon reads `data/schedules.json` every 15 seconds and registers or updates APScheduler jobs accordingly — any change made in the UI takes effect without a restart.

When a scheduled job fires, the scheduler:
1. Acquires the per-environment run lock (`data/run_active_{env}.json`). If a run is already active, the execution is silently skipped.
2. Spawns `data_pipeline/main.py` as an independent OS subprocess.
3. Updates `last_run_at`, `last_run_id`, and `last_run_status` in `schedules.json` on completion.

Because jobs run as separate OS processes, **stopping or restarting the UI does not interrupt a run already in progress** — it continues to completion and releases the lock itself.

### Creating a schedule

1. Open **⏰ Programmation** in the sidebar (admin role required).
2. Check the scheduler status indicator at the top — it should show 🟢. If it shows ⚪, start the UI via `./run_ui.sh`.
3. Click **➕ Nouveau schedule** and fill in:

| Field | Description |
|---|---|
| **Nom** | Label shown in the schedule list |
| **Environnement** | `dev`, `test`, or `prod` |
| **Sources** | One or more sources to harvest (default: `scopus`, `crossref`, `openalex`) |
| **Fenêtre glissante** | Days back from today (default: 20) |
| **Fréquence** | Preset (daily, weekly, …) or "Personnalisé…" for a manual cron expression |
| **Expression cron** | Standard 5-field cron — pre-filled from the preset, always editable |
| **Dry-run** | Skip DSpace ingestion |
| **Désactiver l'envoi d'e-mail** | Suppress the Excel report email |

4. Click **Créer le schedule**. The next execution time is shown immediately.

### Managing schedules

Each schedule card shows the next scheduled execution and the last run result (✅ completed / ⏳ running / ❌ failed / 🛑 killed).

- **Toggle "Actif"** — enable or disable without deleting the schedule. Takes effect within 15 seconds.
- **▶ Now** — fire the run immediately, using the schedule's configuration.
- **🗑** — permanently delete the schedule.

The last 50 lines of `logs/scheduler.log` are accessible at the bottom of the page.

---

## CLI reference

### Time window

| Argument | Default | Description |
|---|---|---|
| `--window-days N` | `15` | Sliding window: *today − N days* → *today* |
| `--start-date YYYY-MM-DD` | — | Fixed start date (requires `--end-date`) |
| `--end-date YYYY-MM-DD` | — | Fixed end date (requires `--start-date`) |

### Sources

```bash
# All sources (default)
python3 data_pipeline/main.py

# Specific subset
python3 data_pipeline/main.py --sources scopus,wos,openalex
```

Available: `wos`, `scopus`, `crossref`, `openalex`, `openalex+crossref`, `datacite`, `zenodo`, `epo`

### Query overrides

Override the default institution-wide query for one or more sources:

```bash
python3 data_pipeline/main.py \
  --query-wos    "EPFL OR Lausanne" \
  --query-scopus "AFFIL(EPFL)"
```

Query overrides are also available in the UI via the **Requêtes (optionnel)** expander on the run page.

#### Crossref — flexible query format

The Crossref query field accepts three formats:

**Plain string** — uses the generic `query` parameter:
```bash
--query-crossref "EPFL machine learning"
```

**JSON object** — spread directly as API parameters (supports any [Crossref query index](https://api.crossref.org/swagger-ui/index.html) or [filter](https://www.crossref.org/documentation/retrieve-metadata/rest-api/rest-api-filters/)):
```bash
--query-crossref '{"query.affiliation": "EPFL SV", "filter": "type:journal-article"}'
--query-crossref '{"filter": "orcid:0000-0002-1825-0097"}'
```

**JSON array** — each element runs as a separate API call; results are merged and deduplicated by DOI:
```bash
--query-crossref '[{"query.affiliation": "EPFL"}, {"filter": "ror-id:02s376052"}]'
```

> **Reserved filters:** `from-created-date` and `until-created-date` are always injected by the harvester (from the configured time window) and cannot be overridden. Any other [Crossref filter](https://www.crossref.org/documentation/retrieve-metadata/rest-api/rest-api-filters/) can be used freely.

### Author ID-based harvesting

When any of these flags are provided, the pipeline switches to **ID-based harvesting** for the relevant sources instead of institution-wide queries.

```bash
# Scopus Author IDs
python3 data_pipeline/main.py --scopus-ids "7004212771,57201854951"

# Web of Science ResearcherIDs
python3 data_pipeline/main.py --wos-ids "A-1234-2010,R-5678-2017"

# ORCID iDs (used by Crossref and OpenAlex)
python3 data_pipeline/main.py --orcid-ids "0000-0002-1825-0097"

# OpenAlex Author IDs
python3 data_pipeline/main.py --openalex-ids "A12345678"

# All combined
python3 data_pipeline/main.py \
  --scopus-ids "7004212771" \
  --orcid-ids  "0000-0002-1825-0097" \
  --sources scopus,openalex,crossref
```

Each flag also accepts a **file path** (one ID per line):

```bash
python3 data_pipeline/main.py --scopus-ids ./ids/scopus_ids.txt
```

> **Researcher Monitor internals:** `--run-type researcher_import`, `--forced-sciper`, `--exclude-dois`, `--exclude-openalex-ids`, and `--exclude-titles` are used internally by the [Researcher Monitor](#researcher-monitor) UI to launch a targeted, pre-filtered import for a single researcher — not intended for direct manual use.

### Run modes

| Flag | Effect |
|---|---|
| `--dry-run` | Skip DSpace load and email — safe for inspection |
| `--no-email` | Generate the report but do not send it |
| `-v` / `-vv` | Increase log verbosity |
| `--env {dev,test,prod}` | Select the target environment (see [Environments](#environments-dev--test--prod)) |
| `--output-dir PATH` | Override the output directory (default: `data/`) |

### Common recipes

```bash
# Inspect last 7 days without touching anything
python3 data_pipeline/main.py --dry-run --no-email --window-days 7 -vv

# Backfill a specific month on prod
python3 data_pipeline/main.py --env prod \
  --start-date 2025-01-01 --end-date 2025-01-31

# Harvest one author across all relevant sources
python3 data_pipeline/main.py \
  --scopus-ids "7004212771" \
  --orcid-ids  "0000-0002-1825-0097" \
  --wos-ids    "A-1234-2010" \
  --sources scopus,wos,openalex,crossref --dry-run
```

---

## Researcher Monitor

A companion subsystem (`researcher_monitor/` package + 🧑‍🔬 **Chercheurs** UI page) that tracks individual researchers' publications and flags gaps against Infoscience — independent of the institution-wide harvest window used by the main pipeline.

```bash
# Sync the researcher registry from the EPFL People API (discover new + offboard departed)
python -m researcher_monitor.main --action sync

# Re-enrich existing registry entries by SCIPER (no discovery, no offboarding)
python -m researcher_monitor.main --action refresh --sciper 349140

# Harvest publications from OpenAlex / ORCID for active researchers
python -m researcher_monitor.main --action harvest --sciper 349140,120091 --start-year 2020

# Compare harvested publications against Infoscience and flag gaps
python -m researcher_monitor.main --action analyze --sciper 349140

# Run sync + harvest + analyze in sequence
python -m researcher_monitor.main --action all
```

| Module | Responsibility |
|---|---|
| `registry.py` | Syncs the researcher registry from the EPFL People API (accreditation-based scope), merges per-SCIPER, batch-fetches ORCID IDs |
| `person_harvester.py` | Harvests OpenAlex + ORCID publications per researcher, with DOI-based and title+year-fallback deduplication |
| `gap_analyzer.py` | Compares harvested publications against Infoscience (via the CRIS `RELATION.Person.researchoutputs` relation), classifying each as matched / missing / ambiguous using the same type-aware rules as the main deduplicator |
| `import_trigger.py` | Builds and launches a targeted `data_pipeline/main.py --run-type researcher_import` run for a single researcher, excluding already-matched or pre-rejected publications |

Results (registry, harvested publications, gap analysis) are persisted in the same per-environment DuckDB (`data/pipeline_{env}.duckdb`) as the main pipeline, in dedicated tables, and browsed through the **Chercheurs** UI page — which also exposes manual sync/harvest/analyze actions with live log streaming and a one-click targeted import for a given researcher's missing publications.

---

## OA Monitor

A companion subsystem (`oa_monitor/` package + 🔎 **OA Monitor** UI page) that monitors institution-wide Open Access compliance against the Swiss National OA Monitor (NOAM) typology, and tracks Read & Publish (R&P) / APC agreements against what has actually landed in Infoscience.

```bash
# Harvest Infoscience items for a year range into the OA Monitor DB
python oa_monitor/run_oa_enricher.py --step harvest --year-from 2022 --year-to 2024

# Enrich with OpenAlex + Unpaywall OA metadata and classify (Gold/Green/Hybrid/Diamond/Closed)
python oa_monitor/run_oa_enricher.py --step enrich --year-from 2022 --year-to 2024

# Export the NOAM-format CSVs + Excel report
python oa_monitor/run_oa_enricher.py --step export-noam --year-from 2022 --year-to 2024

# Consolidate Read & Publish / APC tracking workbooks (data/apc/{publisher}/{contract}/*.xlsx)
python oa_monitor/run_oa_enricher.py --step consolidate-rap

# Join R&P tracking against enriched Infoscience data to find gaps
python oa_monitor/run_oa_enricher.py --step rap-gaps

# Run everything in one pass
python oa_monitor/run_oa_enricher.py --step all --year-from 2022 --year-to 2024
```

| Step | Description |
|---|---|
| `harvest` | Queries Infoscience (DSpace-CRIS/Solr) for items in the year range, via the COAR `types_authority` index rather than free-text `dc.type` (language-independent) |
| `enrich` | Reconciles each item against OpenAlex and Unpaywall, resolves version/licence, and classifies OA status per the NOAM typology (Diamond → Gold → Hybrid → Green → Closed), including DOAJ membership as an independent Gold signal |
| `export-noam` | Writes NOAM-format CSVs and an Excel summary to `data/noam/` |
| `consolidate-rap` | Parses every publisher's R&P/APC tracking workbook under `data/apc/{publisher}/{contract}/*.xlsx` into a single consolidated table |
| `rap-gaps` | Joins consolidated R&P tracking against enriched Infoscience data by DOI to flag articles that are missing, not yet open, or need manual review |
| `inspect` | Prints a summary of what is currently available in the OA Monitor DB |
| `purge` / `purge-rap` | Clear harvested/enriched data for a year range (or R&P tables) without touching the source workbooks in `data/apc/` |

Data is persisted separately from the main pipeline, in `data/oa_work/oa_monitor_{env}.duckdb` — this avoids write-lock contention with the main pipeline's UI and database. The **OA Monitor** UI page exposes all of the above as a guided form (▶ Lancer), a compliance dashboard (📊 Baromètre) with OA/type/year breakdowns, and a filterable browser over enriched data and R&P gaps (📂 Résultats).

---

## Environments (dev / test / prod)

The pipeline supports three fully isolated environments. Each has its own credentials file, DuckDB database, and run-lock — a production run never interferes with development.

| Environment | Credentials | Database | Run lock |
|---|---|---|---|
| `dev` | `.env.dev` | `data/pipeline_dev.duckdb` | `data/run_active_dev.json` |
| `test` | `.env.test` | `data/pipeline_test.duckdb` | `data/run_active_test.json` |
| `prod` | `.env.prod` | `data/pipeline_prod.duckdb` | `data/run_active_prod.json` |

The active environment defaults to `dev`. All `.env.*` files are git-ignored.

### Setup

```bash
cp .sample.env .env.dev   # fill in dev credentials
cp .sample.env .env.test  # fill in test credentials
cp .sample.env .env.prod  # fill in prod credentials
```

### Switching environments

**CLI** — applies to that invocation only, not persisted:
```bash
python3 data_pipeline/main.py --env prod --dry-run
```

**Streamlit UI** — use the selector at the top of the sidebar; the choice is persisted to `data/active_env` and passed automatically to any run launched from the UI. A coloured badge (green / orange / red) and a warning banner indicate the active environment.

**Shell variable** — highest priority, useful for cron or CI/CD:
```bash
APP_ENV=prod python3 data_pipeline/main.py
APP_ENV=test streamlit run app.py
```

Priority order: `APP_ENV` > `--env` > persisted `data/active_env` > `dev`

**Fallback:** if `.env.{env}` does not exist, the pipeline falls back to the generic `.env` at the project root.

---

## Environment variables

Copy `.sample.env` to the appropriate `.env.*` file(s) and fill in the values.

### Required

| Variable | Description |
|---|---|
| `DS_API_ENDPOINT` | DSpace REST API base URL — `https://<domain>/server/api` |
| `DS_API_TOKEN` | DSpace REST API static token |

### Authentication (alternative)

| Variable | Description |
|---|---|
| `DS_ACCESS_TOKEN` | DSpace session cookie token — alternative auth set after login |

### EPFL People API

| Variable | Description |
|---|---|
| `API_EPFL_USER` | [EPFL People API](https://api.epfl.ch/) username (author reconciliation) |
| `API_EPFL_PWD` | EPFL People API password |

### Harvesting sources

| Variable | Source | Description |
|---|---|---|
| `SCOPUS_API_KEY` | [Scopus](https://dev.elsevier.com/) | Elsevier API key |
| `SCOPUS_INST_TOKEN` | Scopus | Elsevier institutional token |
| `ELS_API_KEY` | [Unpaywall](https://unpaywall.org/) | Elsevier key for full-text PDF retrieval |
| `WOS_TOKEN` | [Web of Science](https://developer.clarivate.com/) | Clarivate API token |
| `EPO_OPS_KEY` | [EPO OPS](https://developers.epo.org/) | Open Patent Services consumer key |
| `EPO_OPS_SECRET` | EPO OPS | Open Patent Services consumer secret |
| `OPENALEX_API_KEY` | [OpenAlex](https://docs.openalex.org/) | Authenticated access (higher rate limits) |
| `OPENALEX_DATA_VERSION` | OpenAlex | API data version (default: `2`) |
| `ZENODO_API_KEY` | [Zenodo](https://developers.zenodo.org/) | Authenticated rate limit |
| `ORCID_API_TOKEN` | [ORCID](https://info.orcid.org/documentation/api-tutorials/) | Bearer token for author reconciliation |
| `ORCID_REPOSITORY_SOURCE_NAME` | ORCID | Source name recorded when writing works back to a researcher's ORCID record |

> [DataCite](https://support.datacite.org/docs/api) requires no API key — its REST API is public.

### Researcher registry (Researcher Monitor)

| Variable | Default | Description |
|---|---|---|
| `EPFL_ACCRED_CLASS_IDS` | `5,6,10` | Comma-separated EPFL accreditation class IDs in scope for the researcher registry sync |
| `EPFL_ACCRED_POSITION_ID` | `65,70,123,146,181,184,182,186,187,189,1456,256,208` | Comma-separated EPFL accreditation position IDs in scope |
| `EPFL_ACCRED_STATUS_ID` | `1` | `1` = internal staff, `2` = hosted researchers. Leave unset to include all statuses |

### Polite pool / HTTP

| Variable | Description |
|---|---|
| `CONTACT_API_EMAIL` | Email sent as `mailto` in requests to [Crossref](https://www.crossref.org/documentation/retrieve-metadata/rest-api/), [Unpaywall](https://unpaywall.org/), [OpenAlex](https://docs.openalex.org/) — strongly recommended |
| `USER_AGENT` | HTTP `User-Agent` header (defaults to a sensible EPFL string if unset) |

### Email report (optional)

| Variable | Description |
|---|---|
| `RECIPIENT_EMAIL` | Report recipient address |
| `SENDER_EMAIL` | Report sender address |
| `SMTP_SERVER` | SMTP hostname |

---

## Architecture

The pipeline follows a strict linear sequence: **Harvest → Deduplicate → Enrich → Load → Report → Persist**.

```
data_pipeline/
├── main.py                     Entry point — CLI args, orchestration, DuckDB persistence
├── harvester.py                One Harvester subclass per source (WoS, Scopus, Crossref, OpenAlex, DataCite, Zenodo, EPO)
├── deduplicator.py             Cross-source dedup + DSpace-aware dedup
├── enricher.py                 EPFL author reconciliation, OA/full-text enrichment
├── loader.py                   Builds DSpace-CRIS payloads, calls DSpaceClientWrapper
├── reporting.py                Excel report generation + SMTP delivery
├── infoscience_status_sync.py  Re-checks imported item status (workspace/workflow/published/rejected) post-import
└── PDFUpdater.py               Legacy PDF upload + file-metadata patch helper (not currently wired into main.py)

oa_monitor/          Open Access compliance monitoring (NOAM typology) + R&P/APC tracking — see [OA Monitor](#oa-monitor)
├── oa_harvester.py      Infoscience (DSpace-CRIS/Solr) harvester, COAR authority-scoped
├── oa_enricher.py       OpenAlex + Unpaywall reconciliation and NOAM export
├── oa_classifier.py     Pure OA classification rules (Diamond/Gold/Hybrid/Green/Closed)
├── type_mapping.py      COAR authority code ↔ NOAM resource type ↔ human-readable label
├── rap_consolidator.py  Parses publisher R&P/APC tracking workbooks (data/apc/)
├── rap_gap_analysis.py  Joins R&P tracking against enriched Infoscience data
└── run_oa_enricher.py   CLI entry point (--step harvest|enrich|export-noam|consolidate-rap|rap-gaps|…)

researcher_monitor/  Per-researcher publication tracking — see [Researcher Monitor](#researcher-monitor)
├── registry.py          Syncs the researcher registry from the EPFL People API
├── person_harvester.py  OpenAlex + ORCID harvesting per researcher, with dedup
├── gap_analyzer.py       Compares harvested publications against Infoscience
├── import_trigger.py    Launches a targeted data_pipeline/main.py import for one researcher
└── main.py               CLI entry point (--action sync|refresh|harvest|analyze|all)

clients/             One module per external API (scopus, wos, crossref, openalex, datacite,
                     zenodo, epo_ops, unpaywall, orcid, api_epfl, doi, dspace_client_wrapper)
config/
├── __init__.py      YAML loader — exposes source_order, default_queries, unit_types, …
├── pipeline.yaml    Default harvest queries, source priority order, unit filters, Scopus AF-IDs
└── mappings/
    ├── collections.yaml      Infoscience collection names → UUIDs + DSpace Submission Forms section names
    ├── doctypes.yaml         Source doc-types → collection + dc.type (active + commented-out)
    ├── licenses.yaml         OA licence identifiers → DSpace display values
    ├── versions.yaml         OA version identifiers → COAR URIs
    └── types_authority.yaml  dc.type values → COAR authority identifiers
mappings.py          Loads the above YAML files; exposes classify_record_type, get_version_mapping, …
env_loader.py        Environment selection and .env.* loading
db/pipeline_db.py    DuckDB persistence layer (run history, publications, authors, researcher registry)
ui/
├── constants.py     Design tokens (PRIMARY, SECONDARY, C_GREEN …), SOURCES list, lookup tables
├── helpers.py       Shared helpers — icons, badges, metric cards, fmt_dur/fmt_dt, get_db
├── pub_helpers.py   Publication business logic — is_weak, oa_text, lic_text, source_api_url
├── run_state.py     File-based mutex (one pipeline run at a time, per environment)
├── auth.py          Streamlit authentication + role-based ACL (admin / curator / reporting)
├── styles.css       External stylesheet (CSS custom properties injected from app.py)
├── components/
│   ├── pub_table.py        Publications table rendering + per-row dialogs
│   ├── run_table.py        Runs table — filterable, paginated, review tracking
│   ├── researcher_table.py Researcher registry cards + gap-analysis detail view
│   └── researcher_jobs.py  Job-lock + launch helpers for Researcher Monitor actions
└── pages/
    ├── dashboard.py          KPI tiles, trend charts, recent runs
    ├── run_launcher.py       Pipeline launch form + live log streaming
    ├── scheduling.py         Scheduled run CRUD
    ├── publications.py       Filterable paginated publications table
    ├── statistics.py         Per-run and global analytical charts
    ├── researcher_monitor.py Researcher Monitor UI (Chercheurs)
    ├── oa_monitor.py         OA Monitor UI (harvest/enrich form, barometer, data browser)
    ├── cleanup.py            Bulk workspace/workflow item removal (Nettoyage)
    ├── configuration.py      Environment variables status, DuckDB info
    └── help.py               Curator guide (Aide)
app.py               Streamlit entry point — setup, sidebar, page router
```

**Key design points:**

- `env_loader.py` is the single source of truth for environment selection. It sets `APP_ENV` in `os.environ` on load so every downstream module (`PipelineDB`, `run_state`) picks up the correct environment without re-reading the state file.
- `PipelineDB` opens a new [DuckDB](https://duckdb.org/) connection for every operation and closes it immediately — no persistent connections, which avoids write-lock conflicts. All DDL uses `CREATE/ALTER … IF NOT EXISTS` so schema migrations are idempotent.
- The run-lock file (`data/run_active_{env}.json`) is created atomically with `open(..., 'x')` so two simultaneous UI submissions cannot both start a run.
- The UI is authenticated via bcrypt-hashed credentials in `.streamlit/auth.yaml`. The CLI is authentication-free.
- All data-driven configuration (queries, mappings, collection UUIDs) lives in `config/pipeline.yaml` and `config/mappings/*.yaml`. To add a new document type, update `doctypes.yaml`; to update a collection UUID after a DSpace migration, update `collections.yaml` — no Python changes required.
- Source priority for deduplication merging is defined in `config/pipeline.yaml → source_order`.
- The stylesheet lives in `ui/styles.css` (pure CSS); `app.py` injects colour tokens as CSS custom properties (`var(--primary)`, `var(--secondary)`, etc.) from `ui/constants.py` via a small inline `<style>` block — no Python templating in the stylesheet itself.
- Each UI page is a standalone module under `ui/pages/` exposing a single `render(db, ...)` function. `app.py` is a thin router that calls the relevant `render()` after sidebar and authentication setup. Larger pages (Chercheurs, OA Monitor) extract reusable rendering/business logic into `ui/components/`.
- The run launcher and scheduling pages check for `DS_API_ENDPOINT` and `DS_API_TOKEN` in the active `.env.{env}` file before allowing any run to start — a misconfigured environment blocks launch at the UI level, not at pipeline runtime.
- `oa_monitor/` and `researcher_monitor/` are self-contained packages with their own CLI entry points, independent of `data_pipeline/main.py`'s harvest window and dedup pipeline. `oa_monitor` persists to its own DuckDB file (`data/oa_work/oa_monitor_{env}.duckdb`) to avoid write-lock contention; `researcher_monitor` shares `data/pipeline_{env}.duckdb` but writes to dedicated tables.

---

## Output structure

Each run produces a timestamped subfolder under `data/`:

```
data/
└── 2025-10-08_03-15/
    ├── Raw_WosItems.csv
    ├── Raw_ScopusItems.csv
    ├── Raw_CrossrefItems.csv
    ├── Raw_OpenalexItems.csv
    ├── Raw_ZenodoItems.csv
    ├── Raw_EpoItems.csv
    ├── DeduplicatedItems.csv
    ├── UnloadedItems.csv          # clear duplicates found in Infoscience (discarded)
    ├── Items.csv
    ├── AuthorsAndAffiliations.csv
    ├── EpflAuthors.csv
    ├── ItemsWithOAMetadata.csv
    ├── ImportedItems.csv
    ├── RejectedItems.csv
    └── Report_2025-10-08_03-15.xlsx
```

Run history, per-source statistics, publications, EPFL authors, and unit links are also written to DuckDB (`data/pipeline_{env}.duckdb`) and are browsable through the Streamlit UI.

**OA Monitor and Researcher Monitor** use separate storage under `data/`:
- `data/oa_work/oa_monitor_{env}.duckdb` — harvested/enriched OA data and R&P gap analysis (year-partitioned)
- `data/noam/` — exported NOAM CSVs and Excel reports
- `data/apc/{publisher}/{contract}/*.xlsx` — source Read & Publish/APC tracking workbooks (consolidated, never modified)
- Researcher registry, harvested publications, and gap analysis are written to dedicated tables in the main `data/pipeline_{env}.duckdb`

---

## Incremental logic

The pipeline is intentionally stateless. Re-running it is always safe because:

1. **Sliding window** — only publications within the configured date range are harvested.
2. **DSpace-aware dedup** — the deduplicator queries Infoscience for existing items before loading, so already-imported records are never duplicated. Deduplication is **type-aware**:
   - A dataset is only deduplicated against other datasets (scoped to the *Datasets and Code* collection); a title match in another collection is flagged but not discarded.
   - A preprint whose published version already exists in Infoscience is forwarded to the DSpace workspace rather than silently dropped, so it can be reviewed and linked.
   - Clear duplicates (same DOI, same type, same title + year within the same collection) are discarded without flagging.
3. **Stable `row_id`** — each record gets a deterministic hash of its key fields, ensuring consistent matching across runs.

Running daily with a 15-day window (the default) catches late-indexed publications while the overlap with previous windows is handled entirely by deduplication.

---

## License

This project is licensed under the **MIT License** — see [LICENSE](LICENSE) for the full text.

The bundled [DSpace REST Python Client](https://github.com/the-library-code/dspace-rest-python) (`dspace/dspace_rest_client/`) is licensed under the **BSD 3-Clause License** (© The Library Code GmbH). MIT and BSD-3-Clause are compatible permissive licences; the BSD-3-Clause copyright notice is preserved in the LICENSE file and in the bundled source.

---

## Citation

If you use this software in your research or institutional work, please cite it using the metadata in [CITATION.cff](CITATION.cff) or the reference below:

```bibtex
@software{infoscience_import_pipeline,
  author    = {Sicot, Julien and Borel, Alain and Geoffroy, Géraldine},
  title     = {Infoscience Import Pipeline},
  year      = {2026},
  publisher = {EPFL Library},
  url       = {https://github.com/epfllibrary/infoscience-imports},
  license   = {MIT}
}
```

# Infoscience Import Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-007480.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)

Automated harvesting, deduplication, enrichment, and loading of publication data from multiple external sources into **[Infoscience](https://infoscience.epfl.ch) / [DSpace-CRIS](https://wiki.lyrasis.org/display/DSDOC7x)**.

**Sources:** [Scopus](https://dev.elsevier.com/) · [Web of Science](https://developer.clarivate.com/) · [Crossref](https://www.crossref.org/documentation/retrieve-metadata/rest-api/) · [OpenAlex](https://docs.openalex.org/) · [Zenodo](https://developers.zenodo.org/) · [EPO OPS](https://developers.epo.org/)

---

## Table of contents

1. [What it does](#what-it-does)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Quick start](#quick-start)
5. [Streamlit supervision UI](#streamlit-supervision-ui)
6. [Authentication](#authentication)
7. [CLI reference](#cli-reference)
8. [Environments](#environments-dev--test--prod)
9. [Environment variables](#environment-variables)
10. [Architecture](#architecture)
11. [Output structure](#output-structure)
12. [Incremental logic](#incremental-logic)
13. [License](#license)
14. [Citation](#citation)

---

## What it does

The pipeline runs a linear sequence of stages on every execution:

| Stage | Description |
|---|---|
| **Harvest** | Queries each enabled source API within the configured time window |
| **Deduplicate** | Cross-source dedup (title + year or DOI), then dedup against existing Infoscience items |
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
./run_ui.sh          # default port 8501
./run_ui.sh 8502     # custom port
# or directly:
streamlit run app.py
```

### Pages

| Page | Role | Description |
|---|---|---|
| 🏠 Tableau de bord | all | KPIs, 30-day trend chart, recent runs, per-source breakdown, charts by type / OA status / year / unit / journal / PDF proportion |
| 🚀 Lancer un run | admin | Form to configure and launch the pipeline; live log streaming with stop button |
| 📋 Publications | all | Paginated, sortable, filterable datatable; OA / licence / PDF badges; EPFL author + unit aggregation; weak-status flag; Excel report download |
| 📊 Statistiques | all | Per-run funnel by source, publication type breakdown, EPFL author and unit tabs |
| ⚙️ Configuration | admin | Environment variable status, DuckDB info, `.env` template |

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

The environment selector in the sidebar switches between `dev`, `test`, and `prod` — the choice is persisted and automatically passed to any run launched from the UI.

---

## Authentication

The Streamlit UI requires login. Two roles are available:

| Role | Pages |
|---|---|
| `admin` | All pages |
| `reporting` | Tableau de bord · Publications · Statistiques |

The CLI pipeline is **not** protected by authentication — credentials are only required for the web UI.

### Setup

```bash
# Create the first users (passwords are prompted interactively)
python -m ui.auth add admin    admin
python -m ui.auth add reporter reporting

# Other management commands
python -m ui.auth list
python -m ui.auth passwd <username>
python -m ui.auth remove <username>
```

Credentials are stored as [bcrypt](https://pypi.org/project/bcrypt/) hashes in `.streamlit/auth.yaml` (gitignored). Copy `.streamlit/auth.yaml.example` as a reference for the file structure.

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

Available: `wos`, `scopus`, `crossref`, `openalex`, `zenodo`, `epo`

### Query overrides

Override the default institution-wide query for one or more sources:

```bash
python3 data_pipeline/main.py \
  --query-wos    "EPFL OR Lausanne" \
  --query-scopus "AFFIL(EPFL)" \
  --query-crossref "Ecole Polytechnique Federale de Lausanne"
```

Query overrides are also available in the UI via the **Requêtes (optionnel)** expander on the run page.

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
├── main.py          Entry point — CLI args, orchestration, DuckDB persistence
├── harvester.py     One Harvester subclass per source (WoS, Scopus, Crossref, …)
├── deduplicator.py  Cross-source dedup + DSpace-aware dedup
├── enricher.py      EPFL author reconciliation, OA/full-text enrichment
├── loader.py        Builds DSpace-CRIS payloads, calls DSpaceClientWrapper
└── reporting.py     Excel report generation + SMTP delivery

clients/             One module per external API
config.py            Default harvest queries, source priority order, unit filters
mappings.py          Source doc-types → Infoscience collections + dc.type values
env_loader.py        Environment selection and .env.* loading
db/pipeline_db.py    DuckDB persistence layer (run history, publications, authors)
ui/
├── run_state.py     File-based mutex (one pipeline run at a time, per environment)
├── auth.py          Streamlit authentication + role-based ACL
└── styles.css       External stylesheet (colour tokens injected from app.py)
app.py               Streamlit supervision UI
```

**Key design points:**

- `env_loader.py` is the single source of truth for environment selection. It sets `APP_ENV` in `os.environ` on load so every downstream module (`PipelineDB`, `run_state`) picks up the correct environment without re-reading the state file.
- `PipelineDB` opens a new [DuckDB](https://duckdb.org/) connection for every operation and closes it immediately — no persistent connections, which avoids write-lock conflicts. All DDL uses `CREATE/ALTER … IF NOT EXISTS` so schema migrations are idempotent.
- The run-lock file (`data/run_active_{env}.json`) is created atomically with `open(..., 'x')` so two simultaneous UI submissions cannot both start a run.
- The UI is authenticated via bcrypt-hashed credentials in `.streamlit/auth.yaml`. The CLI is authentication-free.
- Source priority for deduplication merging is defined in `config.py → source_order`.
- The stylesheet lives in `ui/styles.css` (pure CSS); `app.py` only injects colour tokens as CSS custom properties (`var(--canard)`, etc.) via a small inline `<style>` block.

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
    ├── UnloadedItems.csv          # duplicates found in Infoscience
    ├── Items.csv
    ├── AuthorsAndAffiliations.csv
    ├── EpflAuthors.csv
    ├── ItemsWithOAMetadata.csv
    ├── ImportedItems.csv
    ├── RejectedItems.csv
    └── Report_2025-10-08_03-15.xlsx
```

Run history, per-source statistics, publications, EPFL authors, and unit links are also written to DuckDB (`data/pipeline_{env}.duckdb`) and are browsable through the Streamlit UI.

---

## Incremental logic

The pipeline is intentionally stateless. Re-running it is always safe because:

1. **Sliding window** — only publications within the configured date range are harvested.
2. **DSpace-aware dedup** — the deduplicator queries Infoscience for existing items before loading, so already-imported records are never duplicated.
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
  author    = {Sicot, Julien},
  title     = {Infoscience Import Pipeline},
  year      = {2026},
  publisher = {EPFL Library},
  url       = {https://github.com/epfllibrary/infoscience-imports},
  license   = {MIT}
}
```

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Install dependencies** (Python 3.11, virtualenv at `.venv`):
```bash
pip install -r requirements.txt
```

**Run the pipeline** (from project root):
```bash
python3 data_pipeline/main.py                              # all sources, 15-day window
python3 data_pipeline/main.py --dry-run --no-email         # local test, no DSpace load
python3 data_pipeline/main.py --sources scopus,openalex -vv
python3 data_pipeline/main.py --start-date 2025-01-01 --end-date 2025-01-31
```

**Run the Streamlit supervision UI**:
```bash
./run_ui.sh           # default port 8501
./run_ui.sh 8502      # custom port
# or directly:
streamlit run app.py
```

**Lint**:
```bash
flake8 .
pylint data_pipeline/ clients/ db/ ui/
```

## Architecture

The pipeline follows a linear sequence: **Harvest → Deduplicate → Enrich → Load → Report/Persist**.

### Entry points

- `data_pipeline/main.py` — CLI entry point. Parses args, builds queries, calls `run_pipeline()`.
- `app.py` — Streamlit UI that launches the pipeline as a subprocess and displays run history from DuckDB.

### Core pipeline stages (`data_pipeline/`)

| Module | Class | Role |
|---|---|---|
| `harvester.py` | `WosHarvester`, `ScopusHarvester`, `CrossrefHarvester`, `OpenAlexCrossrefHarvester`, `ZenodoHarvester`, `EPOHarvester` | Each extends `Harvester` ABC; `fetch_and_parse_publications()` returns a normalized DataFrame |
| `deduplicator.py` | `DataFrameProcessor` | Cross-source dedup (title+year or DOI), then DSpace-aware dedup via `deduplicate_infoscience()` |
| `enricher.py` | `AuthorProcessor`, `PublicationProcessor` | EPFL author reconciliation (EPFL People API, ORCID), OA/full-text enrichment (Unpaywall, OpenAlex) |
| `loader.py` | `Loader` | Builds DSpace-CRIS item payloads and calls `DSpaceClientWrapper` to ingest |
| `reporting.py` | `GenerateReports` | Excel report generation and SMTP email delivery |

### External API clients (`clients/`)

Each client wraps one external API: `wos_client_v2.py`, `scopus_client.py`, `crossref_client.py`, `openalex_client.py`, `zenodo_client.py`, `unpaywall_client.py`, `orcid_client.py`, `api_epfl_client.py`, `epo_ops_client.py`, `dspace_client_wrapper.py`.

`DSpaceClientWrapper` wraps the bundled `dspace/dspace_rest_client/` library and is used both by the loader (writes) and the deduplicator (reads existing items).

### Persistence (`db/`)

`PipelineDB` (DuckDB, `data/pipeline.duckdb`) stores run history, per-source stats, imported/rejected publications, EPFL authors, and unit links. All connections are short-lived to avoid DuckDB write-lock conflicts. The Streamlit UI reads this DB to render dashboards.

### Configuration (`config.py`)

- `default_queries` — default harvest queries per source (EPFL institution IDs for Scopus, WoS OG filter, etc.)
- `source_order` — priority order for deduplication merging
- `unit_types` / `excluded_unit_types` — filters for EPFL organisational units from api.epfl.ch
- `scopus_epfl_afids` — list of Scopus AF-IDs covering EPFL

### Mappings (`mappings.py`)

Translates source-specific document types (`source_wos`, `source_scopus`, etc.) to Infoscience collection names and `dc.type` values for DSpace ingestion. Also maps OA licenses and version strings.

### UI state (`ui/run_state.py`)

File-based mutex (`data/run_active.json`) ensuring only one pipeline process runs at a time. Uses POSIX exclusive file creation (`open(..., 'x')`) for atomicity. The Streamlit app checks this to show live status and offer a kill button.

## Required environment variables (`.env` at project root)

| Variable | Required for |
|---|---|
| `DS_API_ENDPOINT` | DSpace REST API base URL (hard required — loading fails without it) |
| `DS_API_TOKEN` | DSpace REST API static token (used by `dspace_rest_client`) |
| `DS_ACCESS_TOKEN` | DSpace session cookie token (alternative auth, set after login) |
| `API_EPFL_USER` / `API_EPFL_PWD` | EPFL People API credentials (author reconciliation) |
| `ELS_API_KEY` | Elsevier API key (PDF retrieval via Unpaywall) |
| `SCOPUS_API_KEY` / `SCOPUS_INST_TOKEN` | Scopus harvesting |
| `WOS_TOKEN` | Web of Science harvesting |
| `EPO_OPS_KEY` / `EPO_OPS_SECRET` | EPO Open Patent Services harvesting |
| `CONTACT_API_EMAIL` | Crossref / Unpaywall / OpenAlex polite pool (strongly recommended) |
| `OPENALEX_API_KEY` | OpenAlex authenticated API access |
| `ZENODO_API_KEY` | Zenodo authenticated rate limit |
| `ORCID_API_TOKEN` | ORCID author reconciliation |
| `USER_AGENT` | HTTP User-Agent header (defaults to a sensible EPFL string if unset) |
| `RECIPIENT_EMAIL` / `SENDER_EMAIL` / `SMTP_SERVER` | Email report delivery |

## Output structure

Each run writes to a timestamped directory `data/YYYY-MM-DD_HH-MM/` containing raw CSVs per source, deduplicated/loaded/rejected CSVs, and an Excel report. The pipeline is stateless — incrementality comes from the sliding time window and deduplication against existing DSpace items.

## ID-based harvesting

When `--scopus-ids`, `--wos-ids`, `--orcid-ids`, or `--openalex-ids` are provided, `build_id_queries()` in `main.py` generates per-source query strings that target specific author identifiers instead of institution-wide queries. These override the defaults from `config.py` for the affected sources.

## Language

All code UI, comments, docstrings, commit messages, and PR descriptions must be written in **English**.

## GIT Workflow

### 1. Always start from main (REQUIRED)

git checkout main
git pull origin main

### 2. Create feature branch (REQUIRED - use descriptive names)

git checkout -b feature/descriptive-name

### 3. Work and commit regularly (REQUIRED)

#### Make logical commits with conventional format

git add .
git commit -m "feat(scope): descriptive message"

### 4. Before creating PR (REQUIRED)

npm run build && npm run typecheck && npm run lint

### 5. Push and create PR (REQUIRED)

git push origin feature/descriptive-name
gh pr create --title "Brief description" --body "Detailed description"

### NEVER

- Work on main branch
- Ask permission for commands in .claude/settings.local.json
- Skip the git workflow
- Make commits without following conventional format

# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

---

## Quick start

```bash
pip install -r requirements.txt          # Python 3.11, virtualenv at .venv

# Run the pipeline (CLI)
python3 data_pipeline/main.py                             # all sources, 15-day window
python3 data_pipeline/main.py --dry-run --no-email        # local test, no DSpace write
python3 data_pipeline/main.py --sources scopus,openalex --window-days 20 -vv
python3 data_pipeline/main.py --start-date 2025-01-01 --end-date 2025-01-31

# Run the supervision UI + background scheduler
./run_ui.sh             # port 8501 (default)
./run_ui.sh 8502        # custom port

# Lint
flake8 .
pylint data_pipeline/ clients/ db/ ui/
```

---

## Repository layout

```
infoscience-imports/
├── data_pipeline/          # Core pipeline stages
│   ├── main.py             # CLI entry point
│   ├── harvester.py        # Source harvesters (Scopus, WoS, CrossRef, OpenAlex, Zenodo, EPO)
│   ├── deduplicator.py     # Cross-source + DSpace dedup
│   ├── enricher.py         # Author reconciliation + OA/PDF enrichment
│   ├── loader.py           # DSpace-CRIS ingestion
│   └── reporting.py        # Excel report + email delivery
├── clients/                # One file per external API
├── db/
│   └── pipeline_db.py      # DuckDB persistence layer (PipelineDB)
├── ui/
│   ├── auth.py             # bcrypt auth, role-based ACL, session persistence
│   ├── run_state.py        # POSIX file-based run lock (one run at a time)
│   └── styles.css          # Streamlit external stylesheet
├── app.py                  # Streamlit multi-page UI
├── scheduler.py            # APScheduler background daemon
├── env_loader.py           # Multi-environment support (dev/test/prod)
├── config.py               # Default queries, source order, unit filters
├── mappings.py             # Document type / collection / OA license mappings
├── run_ui.sh               # Starts Streamlit + scheduler together
├── run_pipeline.sh         # Cron wrapper (all main sources)
├── run_epo.sh              # Cron wrapper (EPO only, 365-day window)
├── run_zenodo.sh           # Cron wrapper (Zenodo only, 365-day window)
├── .streamlit/
│   ├── config.toml         # Theme, headless mode, WebSocket keepalive
│   └── auth.yaml.example   # Credentials file template
└── data/
    ├── pipeline_{env}.duckdb   # Per-environment DuckDB database
    ├── schedules.json          # Scheduled run configuration (UI-managed)
    ├── sessions.json           # Active UI sessions (auto-managed)
    └── run_active_{env}.json   # Run lock file (auto-managed)
```

---

## Pipeline architecture

Linear sequence: **Harvest → Deduplicate → Enrich → Load → Report → Persist**

### 1. Harvest (`data_pipeline/harvester.py`)

Each harvester extends the `Harvester` ABC and implements `fetch_and_parse_publications()` returning a normalized DataFrame with a common schema (`row_id`, `title`, `doi`, `pubyear`, `source`, `authors`, …).

| Class | Source |
|---|---|
| `ScopusHarvester` | Elsevier Scopus API |
| `WosHarvester` | Clarivate Web of Science API v2 |
| `CrossrefHarvester` | Crossref REST API |
| `OpenAlexCrossrefHarvester` | OpenAlex (Crossref schema) |
| `ZenodoHarvester` | Zenodo REST API |
| `EPOHarvester` | EPO Open Patent Services |

### 2. Deduplicate (`data_pipeline/deduplicator.py`)

`DataFrameProcessor` performs two passes:
1. Cross-source dedup by DOI then by normalised title+year (`generate_unique_ids`)
2. DSpace-aware dedup: queries existing Infoscience items via `DSpaceClientWrapper` (`deduplicate_infoscience`)

### 3. Enrich (`data_pipeline/enricher.py`)

- **`AuthorProcessor`**: detects EPFL-affiliated authors, normalises names, reconciles with EPFL People API and DSpace authority to obtain SCIPER, unit, and academic status. Outputs `df_epfl_authors` (one row per author × publication).
- **`PublicationProcessor`**: enriches each publication with OA status, license, and PDF availability via Unpaywall. Sets `upw_valid_pdf` to a filename string or `None` (never `False` — the loader treats it as a path).

### 4. Load (`data_pipeline/loader.py`)

`Loader` builds DSpace-CRIS item payloads from `df_oa_metadata` + `df_epfl_authors`, calls `DSpaceClientWrapper.create_workspaceitem()`, and returns `df_loaded`.

### 5. Report & Persist

`GenerateReports` writes an Excel report (sheets: `Detected EPFL Authors`, `Matched EPFL Authors`, …) and sends it by email. `PipelineDB` persists the entire run to DuckDB.

---

## CLI reference (`data_pipeline/main.py`)

| Flag | Default | Description |
|---|---|---|
| `--sources` | all | Comma-separated list: `scopus,crossref,openalex,wos,epo,zenodo` |
| `--window-days N` | 15 | Sliding time window (days back from today) |
| `--start-date` / `--end-date` | — | Fixed date range (overrides `--window-days`) |
| `--env` | `dev` | Active environment: `dev`, `test`, `prod` |
| `--run-id` | timestamp | Explicit run ID (used by UI to correlate subprocess with DB record) |
| `--dry-run` | off | Skip DSpace ingestion, write to DB as usual |
| `--no-email` | off | Suppress email report delivery |
| `-v` / `-vv` | off | Verbose / debug logging |
| `--scopus-ids` | — | Comma-separated Scopus Author IDs (overrides institution query) |
| `--wos-ids` | — | Comma-separated WoS ResearcherIDs |
| `--orcid-ids` | — | Comma-separated ORCID iDs |
| `--openalex-ids` | — | Comma-separated OpenAlex author IDs |
| `--query-{source}` | — | Override default query for a specific source |

---

## Multi-environment support (`env_loader.py`)

Three isolated environments share the same codebase but use separate credentials, databases, and run locks:

| Environment | `.env` file | DB file | Run lock |
|---|---|---|---|
| `dev` | `.env.dev` | `data/pipeline_dev.duckdb` | `data/run_active_dev.json` |
| `test` | `.env.test` | `data/pipeline_test.duckdb` | `data/run_active_test.json` |
| `prod` | `.env.prod` | `data/pipeline_prod.duckdb` | `data/run_active_prod.json` |

`env_loader.load_env(env)` sets `os.environ["APP_ENV"]` and calls `load_dotenv()` on the matching file. The active environment is persisted in `data/active_env` so the UI survives page reloads. A `.env` fallback is used when the environment-specific file doesn't exist.

---

## Supervision UI (`app.py`)

Launched by `./run_ui.sh`. Pages and required role:

| Page | Role | Description |
|---|---|---|
| 🏠 Tableau de bord | all | KPI metrics, 30-day trend, sources breakdown, OA/PDF/year/unit/journal charts |
| 🚀 Lancer un run | admin | Configure and launch a pipeline run with live log streaming |
| ⏰ Programmation | admin | CRUD for scheduled runs (cron), enable/disable, run-now, scheduler status |
| 📋 Publications | all | Paginated, filterable table with OA/PDF/licence/EPFL author/unit columns |
| 📊 Statistiques | all | Per-run charts (funnel, OA, year, PDF, journals) + top EPFL authors |
| ⚙️ Configuration | admin | Environment variables status, DuckDB info, `.env` template |

### Key UI design decisions

- **Widgets outside `st.form()`**: time window picker and cron preset selector live outside their respective forms so selection triggers an immediate rerun (form widgets don't rerun on change).
- **Pagination reset**: a `_filter_sig` hash stored in `session_state` detects any filter change and resets `pub_page` to 1 before the `number_input` renders — avoids the "set value + widget default" Streamlit conflict.
- **Enrichment fetch strategy**: when a specific run is selected, the full per-run author/unit fetch is used (efficient single query). When "Tous les runs" is selected, a per-page fetch on `(run_id, row_id)` pairs is used to avoid loading all runs.
- **CSS**: colour tokens are injected as CSS custom properties in an f-string block, then `ui/styles.css` is loaded as a static file — no Python templating in the stylesheet.

---

## Authentication (`ui/auth.py`)

Credentials stored in `.streamlit/auth.yaml` (bcrypt hashed, never committed). Manage users via CLI:

```bash
python -m ui.auth add <username> admin    # create admin user
python -m ui.auth add <username> reporting
python -m ui.auth list
python -m ui.auth passwd <username>
python -m ui.auth remove <username>
```

**Session persistence**: on login, a 256-bit random token is created, stored server-side in `data/sessions.json` (TTL: 8 hours), and written to the URL via `st.query_params["_s"]`. On browser refresh the token is read from the URL query param and the session is restored without re-authentication.

**Roles**:
- `admin` — all pages including run launcher, scheduling, and configuration
- `reporting` — dashboard, publications, statistics (read-only)

---

## Background scheduler (`scheduler.py`)

Started automatically by `run_ui.sh` alongside Streamlit. Reads `data/schedules.json`:

- Polls the file every 15 seconds — UI changes take effect without restart.
- Each job re-reads its schedule at fire time (picks up edits to window, sources, etc.).
- Acquires `run_active_{env}.json` before spawning `main.py`; skips silently if another run is active.
- Loads `.env.{env}` via `dotenv_values()` (thread-safe, no `os.environ` mutation) before spawning the subprocess — required for API keys not present in the shell environment.
- Updates `last_run_at`, `last_run_status` in `schedules.json` after each execution.
- Logs to `logs/scheduler.log`.

`schedules.json` schema per entry: `id`, `name`, `enabled`, `sources`, `window_days`, `cron`, `env`, `dry_run`, `no_email`, `created_by`, `created_at`, `last_run_at`, `last_run_id`, `last_run_status`.

---

## Persistence (`db/pipeline_db.py`)

`PipelineDB` wraps DuckDB. All connections are short-lived (open → execute → close) to avoid the single-writer lock. Schema is idempotent (`CREATE … IF NOT EXISTS`, `ALTER … ADD COLUMN IF NOT EXISTS`) — run `__init__` on any existing DB is always safe.

**Tables**: `runs`, `source_stats`, `publications`, `epfl_authors`, `units`, `pub_authors`, `pub_units`, `pub_detected_authors`, `run_logs`.

**Key column notes**:
- `publications.pub_year`: populated from `df.pubyear` (not `df.year` — the metadata DataFrame always uses `pubyear`).
- `publications.upw_valid_pdf`: `BOOLEAN`; `True` = PDF retrieved, `NULL` = no PDF or non-open licence. Never `False` — the loader treats this as a filename path.
- `pub_detected_authors`: stores only EPFL-affiliated authors without a SCIPER match (Detected − Matched). Used to populate the "Auteurs EPFL" column for rejected publications.

---

## External API clients (`clients/`)

| File | API |
|---|---|
| `scopus_client.py` | Elsevier Scopus Search |
| `wos_client_v2.py` | Clarivate WoS Expanded API |
| `crossref_client.py` | Crossref REST |
| `openalex_client.py` | OpenAlex |
| `zenodo_client.py` | Zenodo REST |
| `unpaywall_client.py` | Unpaywall OA metadata + PDF |
| `orcid_client.py` | ORCID Public API |
| `api_epfl_client.py` | EPFL People / Accreditation API |
| `epo_ops_client.py` | EPO Open Patent Services |
| `dspace_client_wrapper.py` | DSpace-CRIS REST (reads + writes) |

`DSpaceClientWrapper` wraps the bundled `dspace/dspace_rest_client/` library. Used by the loader (create/update items) and the deduplicator (check existing items).

---

## Environment variables

Copy `.sample.env` to `.env.dev` (or `.env.test`, `.env.prod`) and fill in values.

| Variable | Required | Purpose |
|---|---|---|
| `DS_API_ENDPOINT` | ✅ | DSpace REST API base URL |
| `DS_API_TOKEN` | ✅ | DSpace static auth token |
| `DS_ACCESS_TOKEN` | — | DSpace session cookie (alternative auth) |
| `API_EPFL_USER` / `API_EPFL_PWD` | — | EPFL People API credentials |
| `SCOPUS_API_KEY` / `SCOPUS_INST_TOKEN` | — | Scopus harvesting |
| `WOS_TOKEN` | — | Web of Science harvesting |
| `EPO_OPS_KEY` / `EPO_OPS_SECRET` | — | EPO harvesting |
| `OPENALEX_API_KEY` | — | OpenAlex authenticated access |
| `ZENODO_API_KEY` | — | Zenodo rate-limited access |
| `ORCID_API_TOKEN` | — | ORCID reconciliation |
| `ELS_API_KEY` | — | Elsevier PDF retrieval (Unpaywall) |
| `CONTACT_API_EMAIL` | — | Polite pool for Crossref/Unpaywall/OpenAlex |
| `USER_AGENT` | — | HTTP User-Agent header |
| `RECIPIENT_EMAIL` / `SENDER_EMAIL` / `SMTP_SERVER` | — | Email report delivery |

---

## Output structure

```
data/
├── pipeline_{env}.duckdb        # persistent DB
├── schedules.json               # scheduled run config
├── sessions.json                # UI session store
├── active_env                   # persisted active environment name
├── run_active_{env}.json        # run lock (present during active run)
└── {run_id}/                    # per-run CSV exports
    ├── Items.csv
    ├── AuthorsAndAffiliations.csv
    ├── EpflAuthors.csv
    ├── ImportedItems.csv
    ├── RejectedItems.csv
    ├── UnloadedItems.csv
    └── *Report*.xlsx

logs/
├── run_{run_id}.log             # per-run pipeline log
├── scheduler.log                # scheduler daemon log
└── cron.out                     # cron script output
```

---

## Language

All code, UI labels, comments, docstrings, commit messages, and PR descriptions must be written in **English**.

---

## Git workflow

```bash
# Always branch from main
git checkout main && git pull origin main
git checkout -b feature/descriptive-name

# Commit with conventional format
git add <files>
git commit -m "feat(scope): imperative description"
# Types: feat, fix, refactor, docs, test, chore, perf, style, ci

# Push and open PR
git push origin feature/descriptive-name
gh pr create --title "Brief description" --body "..."
```

**Never** work directly on `main`. **Never** skip the conventional commit format.

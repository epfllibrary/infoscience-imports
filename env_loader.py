"""Environment loader — selects and loads the appropriate .env.* file.

Environments
------------
dev   → .env.dev    local development (default)
test  → .env.test   test / staging instance
prod  → .env.prod   production instance

Selection priority (highest to lowest)
---------------------------------------
1. Explicit ``env_name`` argument passed to :func:`load_env`
2. ``APP_ENV`` shell variable (useful for cron / CI/CD)
3. Persisted choice in ``data/active_env`` (written by :func:`set_active_env`)
4. Hard default: ``"dev"``

Isolation
---------
Each environment owns its own files so prod and dev data never mix:
  data/pipeline_dev.duckdb   /  data/run_active_dev.json
  data/pipeline_test.duckdb  /  data/run_active_test.json
  data/pipeline_prod.duckdb  /  data/run_active_prod.json

Typical usage
-------------
# In app.py / main.py entry point:
    import env_loader
    active_env = env_loader.load_env()          # picks up persisted choice

# In CLI (after argparse):
    active_env = env_loader.load_env(args.env)  # explicit override

# Switch env from UI:
    env_loader.set_active_env("prod")
    env_loader.load_env("prod")
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent

ENVIRONMENTS: tuple[str, ...] = ("dev", "test", "prod")
DEFAULT_ENV: str = "dev"

_ACTIVE_ENV_FILE = ROOT / "data" / "active_env"


def get_active_env() -> str:
    """Return the name of the currently active environment."""
    from_var = os.environ.get("APP_ENV", "").strip().lower()
    if from_var in ENVIRONMENTS:
        return from_var
    if _ACTIVE_ENV_FILE.exists():
        val = _ACTIVE_ENV_FILE.read_text(encoding="utf-8").strip().lower()
        if val in ENVIRONMENTS:
            return val
    return DEFAULT_ENV


def set_active_env(env_name: str) -> None:
    """Persist the selected environment so future processes pick it up."""
    if env_name not in ENVIRONMENTS:
        raise ValueError(
            f"Unknown environment {env_name!r}. "
            f"Valid choices: {', '.join(ENVIRONMENTS)}"
        )
    _ACTIVE_ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
    _ACTIVE_ENV_FILE.write_text(env_name, encoding="utf-8")


def load_env(env_name: str | None = None, *, override: bool = True) -> str:
    """Load ``.env.{env_name}`` into the process environment.

    Falls back to ``.env`` if the env-specific file does not exist.
    Returns the resolved environment name.

    Critically, sets ``APP_ENV`` in ``os.environ`` so that every subsequent
    call to ``get_active_env()`` within this process (e.g. from PipelineDB or
    run_state) returns the correct environment without needing to re-read the
    ``data/active_env`` file.
    """
    if env_name is None or env_name not in ENVIRONMENTS:
        env_name = get_active_env()

    # Pin the active env for this process so db_path() / run_lock_path()
    # return the right paths even when called before any file is written.
    os.environ["APP_ENV"] = env_name

    env_file = ROOT / f".env.{env_name}"
    if env_file.exists():
        load_dotenv(env_file, override=override)
    else:
        # Graceful fallback: generic .env (backward compat)
        fallback = ROOT / ".env"
        if fallback.exists():
            load_dotenv(fallback, override=override)

    return env_name


def db_path(env_name: str | None = None) -> Path:
    """Return the DuckDB file path for the given (or active) environment."""
    env = env_name or get_active_env()
    return ROOT / "data" / f"pipeline_{env}.duckdb"


def run_lock_path(env_name: str | None = None) -> Path:
    """Return the run-lock JSON path for the given (or active) environment."""
    env = env_name or get_active_env()
    return ROOT / "data" / f"run_active_{env}.json"

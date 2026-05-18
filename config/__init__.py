"""Pipeline configuration — all data-driven values loaded from config/pipeline.yaml.

Exposes the same names as the old config.py so all existing imports work unchanged:
  from config import logs_dir, source_order, default_queries, ...
"""

from datetime import date
from pathlib import Path
import os
import yaml

_CONFIG_DIR  = Path(__file__).resolve().parent   # .../config/
_PROJECT_ROOT = _CONFIG_DIR.parent               # project root

# ── Runtime paths (pure Python — not in YAML) ────────────────────────────────

CURRENT_DATE = str(date.today())

base_dir = str(_PROJECT_ROOT)
logs_dir  = os.path.join(base_dir, "logs")
os.makedirs(logs_dir, exist_ok=True)

# License conditions for Unpaywall (small, logic-adjacent — kept in Python)
LICENSE_CONDITIONS = {
    "allowed_licenses": ["cc-by", "public-domain"],
    "allowed_oa_statuses": ["gold", "hybrid", "green"],
}

# ── YAML loader ───────────────────────────────────────────────────────────────

def _load_yaml(relative_path: str) -> dict:
    path = _CONFIG_DIR / relative_path
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_pipeline = _load_yaml("pipeline.yaml")

# ── Public names (same as old config.py) ─────────────────────────────────────

default_queries:    dict  = _pipeline["default_queries"]
source_order:       list  = _pipeline["source_order"]
unit_types:         list  = _pipeline["unit_types"]
excluded_unit_types: list = _pipeline["excluded_unit_types"]
scopus_epfl_afids:  list  = _pipeline["scopus_epfl_afids"]

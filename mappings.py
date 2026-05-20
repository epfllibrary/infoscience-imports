"""Mappings of source platform doctypes to Infoscience collections.

Data-driven dicts are loaded from config/mappings/*.yaml so they can be updated
without touching Python. Pure-Python names (MAPPING_UNITS_*, classify_record_type,
get_version_mapping) remain here.
"""

from pathlib import Path
import yaml

_MAPPINGS_DIR = Path(__file__).resolve().parent / "config" / "mappings"


def _load(filename: str) -> dict:
    with open(_MAPPINGS_DIR / filename, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# Data-driven dicts — loaded from YAML
# ---------------------------------------------------------------------------

doctypes_mapping_dict:   dict = _load("doctypes.yaml")
collections_mapping:     dict = _load("collections.yaml")
licenses_mapping:        dict = _load("licenses.yaml")
versions_mapping:        dict = _load("versions.yaml")
types_authority_mapping: dict = _load("types_authority.yaml")


# ---------------------------------------------------------------------------
# Unit label mappings (kept in Python — small, stable, logic-adjacent)
# ---------------------------------------------------------------------------

MAPPING_UNITS_EN = {
    "ANTENNE": "Antenna",
    "CENTRE": "Center",
    "CHAIRE": "Chair",
    "COLLEGE": "College",
    "DPT": "Department",
    "ECOLE": "Doctoral program",
    "EHE": "Host entities",
    "ENTREPRISES": "Corporate Company",
    "FACULTE": "School",
    "FONDATION": "Foundation",
    "GROUPE": "Group",
    "INSTITUT": "Institute",
    "LABO": "Laboratory",
    "PARTICIPATION": "Participation",
    "PROGRAMME": "Program",
    "SECTION": "Section",
    "SERVICE-CENTRAL": "Central service",
    "SERVICE-GENERAL": "General service",
    "NONE": "Other",
    "SPC": "Center",
    "PLATEFORME": "Facility",
}

MAPPING_UNITS_FR = {
    "ANTENNE": "Antenne",
    "CENTRE": "Centre",
    "CHAIRE": "Chaire",
    "COLLEGE": "Collège",
    "DPT": "Département",
    "ECOLE": "Ecole doctorale",
    "EHE": "Entités hôtes de l'EPFL",
    "ENTREPRISES": "Entreprises sur site",
    "FACULTE": "Faculté",
    "FONDATION": "Fondation",
    "GROUPE": "Groupe",
    "INSTITUT": "Institut",
    "LABO": "Laboratoire",
    "PARTICIPATION": "Participation",
    "PROGRAMME": "Programme",
    "SECTION": "Section",
    "SERVICE-CENTRAL": "Service central",
    "SERVICE-GENERAL": "Service général",
    "NONE": "Autre",
    "SPC": "Centre",
    "PLATEFORME": "Plateforme",
}

MAPPING_UNITS_CODES = {
    "antenne": "ANTENNE",
    "centre": "CENTRE",
    "chaire": "CHAIRE",
    "collège": "COLLEGE",
    "direction": "DIRECTION",
    "dpt": "DPT",
    "ecole": "ECOLE",
    "entités hôtes de l'epfl": "EHE",
    "entreprises": "ENTREPRISES",
    "faculté": "FACULTE",
    "fondation": "FONDATION",
    "groupe": "GROUPE",
    "institut": "INSTITUT",
    "laboratoire": "LABO",
    "participation": "PARTICIPATION",
    "programme": "PROGRAMME",
    "section": "SECTION",
    "service central": "SERVICE-CENTRAL",
    "service général": "SERVICE-GENERAL",
    "divers": "NONE",
    "swiss plasma center": "CENTRE",
    "visibilité annuaire": "NONE",
    "visibilité organigramme": "NONE",
    "plateforme": "PLATEFORME",
}


# ---------------------------------------------------------------------------
# Deduplication type classification
# ---------------------------------------------------------------------------

PREPRINT_COLLECTION = "Preprints and Working Papers"
DATASET_COLLECTION  = "Datasets and Code"

PREPRINT_COLLECTION_UUID = collections_mapping[PREPRINT_COLLECTION]["id"]
DATASET_COLLECTION_UUID  = collections_mapping[DATASET_COLLECTION]["id"]


def classify_record_type(row) -> str:
    """Return 'preprint', 'dataset', or 'published' for a harvested/dedup row.

    Checks ``ifs3_collection`` first (always populated after harvest mapping),
    then falls back to the ``dc.type`` / ``dc_type`` column.
    """
    collection = str(row.get("ifs3_collection") or "")
    if collection == PREPRINT_COLLECTION:
        return "preprint"
    if collection == DATASET_COLLECTION:
        return "dataset"
    dc = str(row.get("dc.type") or row.get("dc_type") or "")
    if dc == "text::preprint":
        return "preprint"
    if dc.startswith("dataset") or dc.startswith("software"):
        return "dataset"
    return "published"


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

def get_version_mapping(version_value) -> dict:
    """Safe lookup in versions_mapping.

    Normalises Python None, empty string and the legacy "None" string to the
    "NA" fallback key so callers don't need to handle these cases individually.
    """
    _FALLBACK = "NA"
    if version_value is None or str(version_value).strip() in ("", "None", "nan"):
        key = _FALLBACK
    else:
        key = str(version_value).strip()
    return versions_mapping.get(key, versions_mapping[_FALLBACK])

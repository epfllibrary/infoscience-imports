"""Tests for pure helper functions in ui/pages/cleanup.py."""

from datetime import date

import pytest

from ui.pages.cleanup import _meta_val, _build_date_query, _parse_item_response, _parse_search_hit


# ── Fixtures ──────────────────────────────────────────────────────────────────

_WORKFLOWITEM_RESPONSE = {
    "id": 7045,
    "type": "workflowitem",
    "_embedded": {
        "item": {
            "uuid": "e4460a7a-fe39-4ff2-9ba2-878621b6ff1a",
            "metadata": {
                "dc.title": [{"value": "General Solution Theory for SPDEs"}],
                "dc.date.created": [{"value": "2026-06-17"}],
                "dc.type": [{"value": "text::conference output::conference paper"}],
                "dc.identifier.doi": [{"value": "10.1007/978-3-031-70660-8_4"}],
            },
        },
        "submitter": {
            "email": "julien.sicot@epfl.ch",
            "metadata": {
                "eperson.firstname": [{"value": "Julien"}],
                "eperson.lastname": [{"value": "Sicot"}],
            },
        },
    },
}

_WORKSPACEITEM_RESPONSE = {
    "id": 240351,
    "type": "workspaceitem",
    "_embedded": {
        "item": {
            "uuid": "8a8c818e-dbd4-4ddd-8ca8-7c0eac4b8c36",
            "metadata": {
                "dc.title": [{"value": "Études sur le paysage agricole"}],
                "dc.date.created": [{"value": "2024-07-18"}],
            },
        },
        "submitter": {
            "email": "alessandra.bianchi@epfl.ch",
            "metadata": {
                "eperson.firstname": [{"value": "Alessandra"}],
                "eperson.lastname": [{"value": "Bianchi"}],
            },
        },
    },
}

_ITEM_NO_OPTIONAL_FIELDS = {
    "id": 9999,
    "type": "workspaceitem",
    "_embedded": {
        "item": {
            "uuid": "aaaaaaaa-0000-0000-0000-000000000001",
            "metadata": {
                "dc.title": [{"value": "Minimal Item"}],
            },
        },
        "submitter": {
            "email": "test@epfl.ch",
            "metadata": {},
        },
    },
}


# ── _meta_val ─────────────────────────────────────────────────────────────────

class TestMetaVal:
    def test_returns_first_value(self):
        md = {"dc.title": [{"value": "Hello World"}, {"value": "Other"}]}
        assert _meta_val(md, "dc.title") == "Hello World"

    def test_missing_field_returns_empty_string(self):
        assert _meta_val({}, "dc.title") == ""

    def test_empty_list_returns_empty_string(self):
        assert _meta_val({"dc.title": []}, "dc.title") == ""

    def test_none_value_returns_empty_string(self):
        assert _meta_val({"dc.title": [{"value": None}]}, "dc.title") == ""

    def test_non_list_returns_empty_string(self):
        assert _meta_val({"dc.title": "flat string"}, "dc.title") == ""


# ── _build_date_query ─────────────────────────────────────────────────────────

class TestBuildDateQuery:
    def test_single_day(self):
        q = _build_date_query(date(2026, 6, 17), date(2026, 6, 17))
        assert q == "dc.date.created:[2026-06-17 TO 2026-06-17]"

    def test_date_range(self):
        q = _build_date_query(date(2026, 1, 1), date(2026, 12, 31))
        assert q == "dc.date.created:[2026-01-01 TO 2026-12-31]"

    def test_zero_padding(self):
        q = _build_date_query(date(2025, 3, 5), date(2025, 3, 5))
        assert "2025-03-05" in q


# ── _parse_item_response ──────────────────────────────────────────────────────

class TestParseItemResponse:
    def test_workflowitem_id_and_type(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["id"] == 7045
        assert row["item_type"] == "workflowitem"

    def test_workflowitem_uuid_from_embedded_item(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["uuid"] == "e4460a7a-fe39-4ff2-9ba2-878621b6ff1a"

    def test_workflowitem_title(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["title"] == "General Solution Theory for SPDEs"

    def test_workflowitem_date_created(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["date_created"] == "2026-06-17"

    def test_workflowitem_doi(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["doi"] == "10.1007/978-3-031-70660-8_4"

    def test_workflowitem_dc_type(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["dc_type"] == "text::conference output::conference paper"

    def test_submitter_fullname_from_eperson_metadata(self):
        row = _parse_item_response(_WORKFLOWITEM_RESPONSE)
        assert row["submitter"] == "Julien Sicot"

    def test_submitter_falls_back_to_email_when_no_name(self):
        row = _parse_item_response(_ITEM_NO_OPTIONAL_FIELDS)
        assert row["submitter"] == "test@epfl.ch"

    def test_workspaceitem_id_and_type(self):
        row = _parse_item_response(_WORKSPACEITEM_RESPONSE)
        assert row["id"] == 240351
        assert row["item_type"] == "workspaceitem"

    def test_workspaceitem_submitter_name(self):
        row = _parse_item_response(_WORKSPACEITEM_RESPONSE)
        assert row["submitter"] == "Alessandra Bianchi"

    def test_missing_doi_returns_empty_string(self):
        row = _parse_item_response(_ITEM_NO_OPTIONAL_FIELDS)
        assert row["doi"] == ""

    def test_missing_date_returns_empty_string(self):
        row = _parse_item_response(_ITEM_NO_OPTIONAL_FIELDS)
        assert row["date_created"] == ""

    def test_missing_dc_type_returns_empty_string(self):
        row = _parse_item_response(_ITEM_NO_OPTIONAL_FIELDS)
        assert row["dc_type"] == ""


# ── _parse_search_hit ─────────────────────────────────────────────────────────

_SEARCH_HIT_WORKFLOW = {
    "_embedded": {
        "indexableObject": {
            "id": 7045,
            "type": "workflowitem",
            "_embedded": {
                "item": {
                    "uuid": "e4460a7a-fe39-4ff2-9ba2-878621b6ff1a",
                    "submitterName": "Julien Sicot",
                    "metadata": {
                        "dc.title": [{"value": "General Solution Theory for SPDEs"}],
                        "dc.date.created": [{"value": "2026-06-17"}],
                        "dc.type": [{"value": "text::conference output::conference paper"}],
                        "dc.identifier.doi": [{"value": "10.1007/978-3-031-70660-8_4"}],
                    },
                },
                "submitter": {
                    "email": "julien.sicot@epfl.ch",
                    "metadata": {
                        "eperson.firstname": [{"value": "Julien"}],
                        "eperson.lastname": [{"value": "Sicot"}],
                    },
                },
            },
        }
    }
}

_SEARCH_HIT_WORKSPACE_NO_SUBMITTERNAME = {
    "_embedded": {
        "indexableObject": {
            "id": 240351,
            "type": "workspaceitem",
            "_embedded": {
                "item": {
                    "uuid": "8a8c818e-dbd4-4ddd-8ca8-7c0eac4b8c36",
                    "submitterName": "",
                    "metadata": {
                        "dc.title": [{"value": "Études sur le paysage agricole"}],
                        "dc.date.created": [{"value": "2024-07-18"}],
                    },
                },
                "submitter": {
                    "email": "alessandra.bianchi@epfl.ch",
                    "metadata": {
                        "eperson.firstname": [{"value": "Alessandra"}],
                        "eperson.lastname": [{"value": "Bianchi"}],
                    },
                },
            },
        }
    }
}

_SEARCH_HIT_NO_SUBMITTER_AT_ALL = {
    "_embedded": {
        "indexableObject": {
            "id": 9001,
            "type": "workspaceitem",
            "_embedded": {
                "item": {
                    "uuid": "aaaaaaaa-0000-0000-0000-000000000002",
                    "submitterName": "",
                    "metadata": {"dc.title": [{"value": "Minimal"}]},
                },
                "submitter": {"email": "anon@epfl.ch", "metadata": {}},
            },
        }
    }
}


class TestParseSearchHit:
    def test_workflow_id_and_type(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["id"] == 7045
        assert row["item_type"] == "workflowitem"

    def test_workflow_uuid(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["uuid"] == "e4460a7a-fe39-4ff2-9ba2-878621b6ff1a"

    def test_workflow_title(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["title"] == "General Solution Theory for SPDEs"

    def test_workflow_date_created(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["date_created"] == "2026-06-17"

    def test_workflow_doi(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["doi"] == "10.1007/978-3-031-70660-8_4"

    def test_workflow_dc_type(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["dc_type"] == "text::conference output::conference paper"

    def test_submitter_name_from_item_submittername(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKFLOW)
        assert row["submitter"] == "Julien Sicot"

    def test_submitter_falls_back_to_eperson_metadata(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKSPACE_NO_SUBMITTERNAME)
        assert row["submitter"] == "Alessandra Bianchi"

    def test_submitter_falls_back_to_email_when_no_name(self):
        row = _parse_search_hit(_SEARCH_HIT_NO_SUBMITTER_AT_ALL)
        assert row["submitter"] == "anon@epfl.ch"

    def test_workspace_id_and_type(self):
        row = _parse_search_hit(_SEARCH_HIT_WORKSPACE_NO_SUBMITTERNAME)
        assert row["id"] == 240351
        assert row["item_type"] == "workspaceitem"

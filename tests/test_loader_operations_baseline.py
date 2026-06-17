"""Baseline tests for Loader patch and remove operation builders.

Tests _construct_remove_operations and _construct_patch_operations in isolation
(no DSpace API calls). They capture the current output structure so any
refactoring phase can be verified to produce identical operation lists.

The module-level DSpaceClientWrapper() instantiation in loader.py is intercepted
via sys.modules mocking before the import.
"""

import sys
import unittest
import os
from unittest.mock import MagicMock

# --- Mock DSpaceClientWrapper BEFORE importing loader to prevent network call ---
_mock_dspace_mod = MagicMock()
_mock_dspace_mod.DSpaceClientWrapper = MagicMock(return_value=MagicMock())
sys.modules['clients.dspace_client_wrapper'] = _mock_dspace_mod

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline.loader import Loader
from tests.fixtures.sample_rows import (
    ARTICLE_ROW, CONFERENCE_ROW, BOOK_CHAPTER_ROW, PREPRINT_ROW, DATASET_ROW,
    REPORT_ROW, EMPTY_AUTHORS_DF, EMPTY_EPFL_AUTHORS_DF, UNITS_SINGLE,
)
import mappings


def _make_loader():
    # main branch Loader.__init__ takes (df_metadata, df_epfl_authors, df_authors)
    return Loader(
        df_metadata=None,
        df_epfl_authors=EMPTY_EPFL_AUTHORS_DF,
        df_authors=EMPTY_AUTHORS_DF,
    )


EMPTY_WORKSPACE = {"sections": {}}


class TestRemoveOperations(unittest.TestCase):
    """_construct_remove_operations must always reset dc.subject and conditionally remove others."""

    def setUp(self):
        self.loader = _make_loader()

    def _get_remove_ops(self, collection_name):
        section = mappings.collections_mapping[collection_name]["section"]
        base = f"/sections/{section}details"
        return self.loader._construct_remove_operations(EMPTY_WORKSPACE, base, form_section=section)

    def test_article_always_has_subject_reset(self):
        ops = self._get_remove_ops("Journal articles")
        subject_ops = [o for o in ops if "dc.subject" in o["path"]]
        self.assertEqual(len(subject_ops), 1)
        self.assertEqual(subject_ops[0]["op"], "add")
        self.assertEqual(subject_ops[0]["value"], [])

    def test_conference_always_has_subject_reset(self):
        ops = self._get_remove_ops("Conferences, Workshops, Symposiums, and Seminars")
        subject_ops = [o for o in ops if "dc.subject" in o["path"]]
        self.assertEqual(len(subject_ops), 1)
        self.assertEqual(subject_ops[0]["value"], [])

    def test_dataset_always_has_subject_reset(self):
        ops = self._get_remove_ops("Datasets and Code")
        subject_ops = [o for o in ops if "dc.subject" in o["path"]]
        self.assertEqual(len(subject_ops), 1)
        self.assertEqual(subject_ops[0]["value"], [])

    def test_empty_workspace_no_spurious_removes(self):
        ops = self._get_remove_ops("Journal articles")
        remove_ops = [o for o in ops if o["op"] == "remove"]
        self.assertEqual(remove_ops, [], "Empty workspace should produce zero remove ops")

    def test_workspace_with_title_triggers_remove(self):
        section = mappings.collections_mapping["Journal articles"]["section"]
        base = f"/sections/{section}details"
        workspace = {"sections": {section + "details": {"dc.title": [{"value": "Old Title"}]}}}
        ops = self.loader._construct_remove_operations(workspace, base, form_section=section)
        remove_paths = [o["path"] for o in ops if o["op"] == "remove"]
        self.assertIn(f"{base}/dc.title", remove_paths)

    def test_workspace_with_journal_triggers_journal_removes(self):
        section = mappings.collections_mapping["Journal articles"]["section"]
        base = f"/sections/{section}details"
        workspace = {
            "sections": {
                "journalcontainer_details": {
                    "dc.relation.journal": [{"value": "Nature"}],
                    "dc.relation.issn": [{"value": "0028-0836"}],
                    "oaire.citation.volume": [{"value": "42"}],
                }
            }
        }
        ops = self.loader._construct_remove_operations(workspace, base, form_section=section)
        remove_paths = [o["path"] for o in ops if o["op"] == "remove"]
        self.assertIn("/sections/journalcontainer_details/dc.relation.journal", remove_paths)
        self.assertIn("/sections/journalcontainer_details/dc.relation.issn", remove_paths)
        self.assertIn("/sections/journalcontainer_details/oaire.citation.volume", remove_paths)


class TestPatchOperationsArticle(unittest.TestCase):
    """_construct_patch_operations for article_ form_section."""

    def setUp(self):
        self.loader = _make_loader()
        self.section = mappings.collections_mapping["Journal articles"]["section"]
        self.base = f"/sections/{self.section}details"

    def _get_ops(self, row=None):
        r = row if row is not None else ARTICLE_ROW
        return self.loader._construct_patch_operations(
            r, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )

    def test_returns_list(self):
        self.assertIsInstance(self._get_ops(), list)

    def test_title_op_present(self):
        ops = self._get_ops()
        title_ops = [o for o in ops if "dc.title" in o["path"]]
        self.assertTrue(len(title_ops) >= 1, "No dc.title operation found")

    def test_dc_subject_not_in_patch_ops(self):
        """dc.subject must NOT appear in _construct_patch_operations.

        Keywords are sent via the dedicated final patch in _patch_additional_metadata.
        This avoids writing dc.subject twice (once here, once in the final call).
        """
        ops = self._get_ops()
        subject_ops = [o for o in ops if "dc.subject" in o["path"]]
        self.assertEqual(
            subject_ops, [],
            "_construct_patch_operations must not produce dc.subject ops",
        )

    def test_no_keywords_does_not_add_subject_op(self):
        """When keywords is None, _construct_patch_operations produces no dc.subject op."""
        row = ARTICLE_ROW.copy()
        row["keywords"] = None
        ops = self._get_ops(row)
        subject_ops = [o for o in ops if "dc.subject" in o["path"]]
        self.assertEqual(subject_ops, [])

    def test_journal_fields_present(self):
        ops = self._get_ops()
        paths = [o["path"] for o in ops]
        self.assertTrue(any("dc.relation.journal" in p for p in paths))
        self.assertTrue(any("dc.relation.issn" in p for p in paths))

    def test_funding_ops_present(self):
        ops = self._get_ops()
        funding_paths = [o["path"] for o in ops if "grants" in o["path"]]
        self.assertTrue(len(funding_paths) > 0)

    def test_license_granted_always_present(self):
        ops = self._get_ops()
        granted = [o for o in ops if o["path"] == "/sections/license/granted"]
        self.assertEqual(len(granted), 1)
        self.assertEqual(granted[0]["value"], "true")

    def test_sponsorship_uses_unit_acronym(self):
        ops = self._get_ops()
        spons = [o for o in ops if "dc.description.sponsorship" in o["path"]]
        self.assertTrue(len(spons) >= 1)
        acronyms = [v["value"] for v in spons[0]["value"] if v]
        self.assertIn("LSMS", acronyms)

    def test_no_editors_for_article(self):
        ops = self._get_ops()
        editor_paths = [o["path"] for o in ops if "scientificeditor" in o["path"]]
        self.assertEqual(editor_paths, [])


class TestPatchOperationsConference(unittest.TestCase):

    def setUp(self):
        self.loader = _make_loader()
        self.section = mappings.collections_mapping[
            "Conferences, Workshops, Symposiums, and Seminars"
        ]["section"]
        self.base = f"/sections/{self.section}details"

    def test_conference_info_ops_present(self):
        ops = self.loader._construct_patch_operations(
            CONFERENCE_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        conf_paths = [o["path"] for o in ops if "conference_event" in o["path"]]
        self.assertTrue(len(conf_paths) > 0, "No conference_event operations found")

    def test_conference_name_value(self):
        ops = self.loader._construct_patch_operations(
            CONFERENCE_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        name_ops = [o for o in ops if "dc.relation.conference" in o["path"]]
        self.assertTrue(len(name_ops) > 0)
        names = [v["value"] for v in name_ops[0]["value"] if v]
        self.assertIn("IROS 2024", names)


class TestPatchOperationsBookChapter(unittest.TestCase):

    def setUp(self):
        self.loader = _make_loader()
        self.section = mappings.collections_mapping["Books and Book parts"]["section"]
        self.base = f"/sections/{self.section}details"

    def test_editors_ops_present(self):
        ops = self.loader._construct_patch_operations(
            BOOK_CHAPTER_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        editor_paths = [o["path"] for o in ops if "scientificeditor" in o["path"]]
        self.assertTrue(len(editor_paths) > 0)

    def test_book_title_present(self):
        ops = self.loader._construct_patch_operations(
            BOOK_CHAPTER_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        ispartof = [o for o in ops if "dc.relation.ispartof" in o["path"]]
        self.assertTrue(len(ispartof) > 0)

    def test_isbn_in_bookcontainer_for_chapter(self):
        ops = self.loader._construct_patch_operations(
            BOOK_CHAPTER_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        isbn_ops = [o for o in ops if "dc.relation.isbn" in o["path"]]
        self.assertTrue(len(isbn_ops) > 0)
        self.assertIn("bookcontainer_details", isbn_ops[0]["path"])

    def test_book_chapter_no_journalcontainer_ops(self):
        ops = self.loader._construct_patch_operations(
            BOOK_CHAPTER_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        journal_ops = [o for o in ops if "journalcontainer_details" in o["path"]]
        self.assertEqual(journal_ops, [],
                         f"book_ form must not reference journalcontainer_details: "
                         f"{[o['path'] for o in journal_ops]}")

    def test_book_monograph_no_journalcontainer_ops(self):
        row = BOOK_CHAPTER_ROW.copy()
        row["dc.type"] = "text::book/monograph"
        row["dc.type_authority"] = "book-coar-types:c_2f33"
        ops = self.loader._construct_patch_operations(
            row, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        journal_ops = [o for o in ops if "journalcontainer_details" in o["path"]]
        self.assertEqual(journal_ops, [],
                         f"book_ form must not reference journalcontainer_details: "
                         f"{[o['path'] for o in journal_ops]}")


class TestPatchOperationsDataset(unittest.TestCase):

    def setUp(self):
        self.loader = _make_loader()
        self.section = mappings.collections_mapping["Datasets and Code"]["section"]
        self.base = f"/sections/{self.section}details"

    def _get_ops(self):
        return self.loader._construct_patch_operations(
            DATASET_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )

    def test_language_op_present(self):
        ops = self._get_ops()
        lang_ops = [o for o in ops if "dc.language.iso" in o["path"]]
        self.assertTrue(len(lang_ops) > 0)
        self.assertEqual(lang_ops[0]["value"][0]["value"], "en")

    def test_version_op_present(self):
        ops = self._get_ops()
        ver_ops = [o for o in ops if "dc.description.version" in o["path"]]
        self.assertTrue(len(ver_ops) > 0)
        self.assertEqual(ver_ops[0]["value"][0]["value"], "1.0")

    def test_access_conditions_present(self):
        ops = self._get_ops()
        ac_ops = [o for o in ops if "accessConditions" in o["path"]]
        self.assertTrue(len(ac_ops) > 0)

    def test_additional_url_present(self):
        ops = self._get_ops()
        url_ops = [o for o in ops if "epfl.url" in o["path"]]
        self.assertTrue(len(url_ops) > 0)

    def test_no_editors_for_dataset(self):
        ops = self._get_ops()
        editor_paths = [o["path"] for o in ops if "scientificeditor" in o["path"]]
        self.assertEqual(editor_paths, [])

    def test_related_event_not_conference_event(self):
        ops = self._get_ops()
        paths = [o["path"] for o in ops]
        self.assertFalse(any("conference_event" in p for p in paths),
                         "Dataset must use related_event, not conference_event")

    def test_related_works_present(self):
        ops = self._get_ops()
        rw_ops = [o for o in ops if "related_works" in o["path"]]
        self.assertTrue(len(rw_ops) > 0)

    def test_zenodo_license_condition_present(self):
        ops = self._get_ops()
        lic_ops = [o for o in ops if "ctb.oaireXXlicenseCondition" in o["path"]]
        self.assertTrue(len(lic_ops) > 0)


class TestPatchOperationsPreprint(unittest.TestCase):

    def setUp(self):
        self.loader = _make_loader()
        self.section = mappings.collections_mapping["Preprints and Working Papers"]["section"]
        self.base = f"/sections/{self.section}details"

    def test_publisher_in_preprint_details(self):
        ops = self.loader._construct_patch_operations(
            PREPRINT_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        pub_ops = [o for o in ops if "dc.publisher" in o["path"]]
        self.assertTrue(len(pub_ops) > 0)
        self.assertTrue(any("preprint_details" in o["path"] for o in pub_ops))

    def test_no_peer_reviewed_value_for_preprint(self):
        ops = self.loader._construct_patch_operations(
            PREPRINT_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        reviewed_ops = [o for o in ops if "epfl.peerreviewed" in o["path"]]
        for op in reviewed_ops:
            val = op.get("value")
            if isinstance(val, list):
                actual = val[0].get("value") if val and isinstance(val[0], dict) else None
            elif isinstance(val, dict):
                actual = val.get("value")
            else:
                actual = val
            self.assertIsNone(actual, f"Preprint should not have peerreviewed value: {actual}")

    def test_dc_type_uses_preprint_type_section(self):
        """dc.type for preprints must go to preprint_type, not preprint_details."""
        ops = self.loader._construct_patch_operations(
            PREPRINT_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        type_ops = [o for o in ops if "dc.type" in o["path"]]
        self.assertTrue(len(type_ops) >= 1, "No dc.type operation found")
        for op in type_ops:
            self.assertIn("preprint_type", op["path"],
                          f"dc.type path must use preprint_type section, got: {op['path']}")
            self.assertNotIn("preprint_details", op["path"])

    def test_no_journalcontainer_fields_for_preprint_with_journal_data(self):
        """Preprints must not write to journalcontainer_details even when journal data is non-null."""
        arxiv_row = PREPRINT_ROW.copy()
        arxiv_row["journalTitle"] = "arXiv"
        arxiv_row["journalISSN"] = "2331-8422"
        arxiv_row["journalVolume"] = "abs/2504"
        ops = self.loader._construct_patch_operations(
            arxiv_row, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )
        journal_container_ops = [o for o in ops if "journalcontainer_details" in o["path"]]
        self.assertEqual(
            journal_container_ops, [],
            f"Preprint must produce no journalcontainer_details ops, got: {journal_container_ops}",
        )

    def test_remove_uses_preprint_type_section_for_dc_type(self):
        """_construct_remove_operations must target preprint_type for dc.type removal."""
        workspace = {
            "sections": {
                "preprint_type": {"dc.type": [{"value": "old-type"}]},
            }
        }
        ops = self.loader._construct_remove_operations(
            workspace, self.base, form_section=self.section
        )
        type_removes = [o for o in ops if "dc.type" in o["path"] and o["op"] == "remove"]
        self.assertTrue(len(type_removes) >= 1, "No dc.type remove op found")
        self.assertIn("preprint_type", type_removes[0]["path"])
        self.assertNotIn("preprint_details", type_removes[0]["path"])


class TestFilterPublicationsByValidAffiliations(unittest.TestCase):
    """_filter_publications_by_valid_affiliations must exclude empty-string final_mainunit."""

    def _make_loader_with(self, epfl_authors_df, metadata_df):
        return Loader(
            df_metadata=metadata_df,
            df_epfl_authors=epfl_authors_df,
            df_authors=EMPTY_AUTHORS_DF,
        )

    def test_excludes_null_final_mainunit(self):
        import pandas as pd
        epfl = pd.DataFrame([
            {"row_id": 1, "author": "Doe J.", "sciper_id": "111", "dspace_uuid": None,
             "organizations": "EPFL", "final_mainunit": None},
        ])
        meta = pd.DataFrame([{"row_id": 1, "title": "A paper"}])
        loader = self._make_loader_with(epfl, meta)
        result = loader._filter_publications_by_valid_affiliations()
        self.assertTrue(result.empty)

    def test_excludes_empty_string_final_mainunit(self):
        import pandas as pd
        epfl = pd.DataFrame([
            {"row_id": 1, "author": "Doe J.", "sciper_id": "111", "dspace_uuid": None,
             "organizations": "EPFL", "final_mainunit": ""},
        ])
        meta = pd.DataFrame([{"row_id": 1, "title": "A paper"}])
        loader = self._make_loader_with(epfl, meta)
        result = loader._filter_publications_by_valid_affiliations()
        self.assertTrue(result.empty, "Empty-string final_mainunit must be excluded like None")

    def test_excludes_whitespace_only_final_mainunit(self):
        import pandas as pd
        epfl = pd.DataFrame([
            {"row_id": 1, "author": "Doe J.", "sciper_id": "111", "dspace_uuid": None,
             "organizations": "EPFL", "final_mainunit": "   "},
        ])
        meta = pd.DataFrame([{"row_id": 1, "title": "A paper"}])
        loader = self._make_loader_with(epfl, meta)
        result = loader._filter_publications_by_valid_affiliations()
        self.assertTrue(result.empty, "Whitespace-only final_mainunit must be excluded")

    def test_includes_publication_with_valid_unit(self):
        import pandas as pd
        epfl = pd.DataFrame([
            {"row_id": 1, "author": "Doe J.", "sciper_id": "111", "dspace_uuid": None,
             "organizations": "EPFL", "final_mainunit": "LSMS"},
        ])
        meta = pd.DataFrame([{"row_id": 1, "title": "A paper"}])
        loader = self._make_loader_with(epfl, meta)
        result = loader._filter_publications_by_valid_affiliations()
        self.assertEqual(len(result), 1)

    def test_mixed_authors_one_valid_includes_publication(self):
        """If one author has valid unit, the publication must be included."""
        import pandas as pd
        epfl = pd.DataFrame([
            {"row_id": 1, "author": "Doe J.", "sciper_id": "111", "dspace_uuid": None,
             "organizations": "EPFL", "final_mainunit": None},
            {"row_id": 1, "author": "Smith A.", "sciper_id": "222", "dspace_uuid": None,
             "organizations": "EPFL", "final_mainunit": "LASUR"},
        ])
        meta = pd.DataFrame([{"row_id": 1, "title": "A paper"}])
        loader = self._make_loader_with(epfl, meta)
        result = loader._filter_publications_by_valid_affiliations()
        self.assertEqual(len(result), 1)



class TestPatchOperationsReport(unittest.TestCase):
    """_construct_patch_operations for report_ form_section.

    Bug 1: dc.type placed at /sections/report_type/dc.type (non-existent) → 422.
    Bug 2: dc.publisher placed at /sections/bookcontainer_details/dc.publisher
           (wrong section for reports) → 422.
    Fix: both must use /sections/report_details/....
    """

    def setUp(self):
        self.loader = _make_loader()
        self.section = mappings.collections_mapping["Reports, Documentation, and Standards"]["section"]
        self.base = f"/sections/{self.section}details"

    def _get_ops(self):
        return self.loader._construct_patch_operations(
            REPORT_ROW, UNITS_SINGLE, self.base, self.section, EMPTY_WORKSPACE
        )

    def test_returns_list(self):
        self.assertIsInstance(self._get_ops(), list)

    def test_dc_type_in_report_details_not_report_type(self):
        ops = self._get_ops()
        type_paths = [o["path"] for o in ops if "/dc.type" in o["path"]]
        self.assertTrue(type_paths, "No dc.type operation found for report")
        for path in type_paths:
            self.assertIn("report_details", path,
                          f"dc.type must use report_details, got: {path}")
            self.assertNotIn("report_type", path,
                             f"report_type section does not exist in DSpace: {path}")

    def test_publisher_in_report_details_not_bookcontainer(self):
        ops = self._get_ops()
        pub_ops = [o for o in ops if "dc.publisher" in o["path"]]
        self.assertTrue(pub_ops, "No dc.publisher operation found for report with publisher set")
        for op in pub_ops:
            self.assertIn("report_details", op["path"],
                          f"dc.publisher must use report_details, got: {op['path']}")
            self.assertNotIn("bookcontainer_details", op["path"],
                             f"bookcontainer_details is wrong for report: {op['path']}")

    def test_title_in_report_details(self):
        ops = self._get_ops()
        title_ops = [o for o in ops if "dc.title" in o["path"]]
        self.assertTrue(title_ops)
        self.assertIn("report_details", title_ops[0]["path"])

    def test_no_peer_reviewed_for_report(self):
        ops = self._get_ops()
        reviewed_ops = [o for o in ops if "epfl.peerreviewed" in o["path"]]
        for op in reviewed_ops:
            val = op.get("value")
            if isinstance(val, dict):
                actual = val.get("value")
            elif isinstance(val, list) and val:
                actual = val[0].get("value") if isinstance(val[0], dict) else None
            else:
                actual = val
            self.assertIsNone(actual, "Report should not have a peerreviewed value")

    def test_remove_dc_type_uses_report_details(self):
        workspace = {"sections": {"report_type": {"dc.type": [{"value": "old"}]}}}
        ops = self.loader._construct_remove_operations(workspace, self.base, form_section=self.section)
        type_remove_paths = [o["path"] for o in ops if "/dc.type" in o["path"] and o["op"] == "remove"]
        for path in type_remove_paths:
            self.assertNotIn("report_type", path,
                             f"remove must target report_details, not report_type: {path}")


class TestLanguageFieldForNonDataset(unittest.TestCase):
    """dc.language.iso must be written for non-dataset types when language is available."""

    def setUp(self):
        self.loader = _make_loader()

    def _ops_with_language(self, row_fixture, collection_name):
        section = mappings.collections_mapping[collection_name]["section"]
        row = row_fixture.copy()
        row["language"] = "en"
        base = f"/sections/{section}details"
        return self.loader._construct_patch_operations(
            row, UNITS_SINGLE, base, section, EMPTY_WORKSPACE
        )

    def test_article_language_op_written(self):
        ops = self._ops_with_language(ARTICLE_ROW, "Journal articles")
        lang_ops = [o for o in ops if "dc.language.iso" in o["path"]]
        self.assertTrue(len(lang_ops) > 0, "dc.language.iso must be written for articles")
        self.assertIn("article_details", lang_ops[0]["path"])
        self.assertEqual(lang_ops[0]["value"][0]["value"], "en")

    def test_preprint_language_op_written(self):
        ops = self._ops_with_language(PREPRINT_ROW, "Preprints and Working Papers")
        lang_ops = [o for o in ops if "dc.language.iso" in o["path"]]
        self.assertTrue(len(lang_ops) > 0, "dc.language.iso must be written for preprints")
        self.assertIn("preprint_details", lang_ops[0]["path"])

    def test_report_language_op_written(self):
        ops = self._ops_with_language(REPORT_ROW, "Reports, Documentation, and Standards")
        lang_ops = [o for o in ops if "dc.language.iso" in o["path"]]
        self.assertTrue(len(lang_ops) > 0, "dc.language.iso must be written for reports")
        self.assertIn("report_details", lang_ops[0]["path"])

    def test_none_language_produces_no_language_op(self):
        section = mappings.collections_mapping["Journal articles"]["section"]
        row = ARTICLE_ROW.copy()
        row["language"] = None
        base = f"/sections/{section}details"
        ops = self.loader._construct_patch_operations(
            row, UNITS_SINGLE, base, section, EMPTY_WORKSPACE
        )
        lang_ops = [o for o in ops if "dc.language.iso" in o["path"]]
        self.assertEqual(lang_ops, [], "None language must not produce dc.language.iso op")

    def test_dataset_language_not_duplicated(self):
        """Dataset language comes from parse_language only; the fields-list path must not add a duplicate."""
        section = mappings.collections_mapping["Datasets and Code"]["section"]
        base = f"/sections/{section}details"
        ops = self.loader._construct_patch_operations(
            DATASET_ROW, UNITS_SINGLE, base, section, EMPTY_WORKSPACE
        )
        lang_ops = [o for o in ops if "dc.language.iso" in o["path"]]
        self.assertEqual(len(lang_ops), 1, "Dataset must produce exactly one dc.language.iso op")


class TestCorrespondingAuthorPatch(unittest.TestCase):
    """_process_and_replace_authors must emit epfl.author.corresponding for non-report types."""

    def _loader_with(self, author_rows):
        import pandas as pd
        df_authors = pd.DataFrame(author_rows)
        return Loader(
            df_metadata=None,
            df_epfl_authors=EMPTY_EPFL_AUTHORS_DF,
            df_authors=df_authors,
        )

    def _base_author(self, row_id=1, is_corresponding=False):
        return {
            "row_id": row_id, "author": "Doe J.", "orcid_id": None, "epfl_orcid": None,
            "role": "author", "organizations": "EPFL", "source": "openalex",
            "internal_author_id": None, "openalex_is_corresponding": is_corresponding,
        }

    def test_corresponding_true_written_for_article(self):
        loader = self._loader_with([self._base_author(is_corresponding=True)])
        section = "article_"
        ops = loader._process_and_replace_authors(
            {"sections": {}}, row_id=1,
            base=f"/sections/{section}details", form_section=section,
        )
        corr_ops = [o for o in ops if "epfl.author.corresponding" in o["path"]]
        self.assertTrue(len(corr_ops) > 0, "epfl.author.corresponding must be written")
        values = corr_ops[0]["value"]
        self.assertTrue(any(v.get("value") == "true" for v in values if v))

    def test_corresponding_false_uses_placeholder(self):
        loader = self._loader_with([self._base_author(is_corresponding=False)])
        section = "article_"
        ops = loader._process_and_replace_authors(
            {"sections": {}}, row_id=1,
            base=f"/sections/{section}details", form_section=section,
        )
        corr_ops = [o for o in ops if "epfl.author.corresponding" in o["path"]]
        self.assertTrue(len(corr_ops) > 0)
        values = corr_ops[0]["value"]
        self.assertTrue(
            all(v.get("value") == "#PLACEHOLDER_PARENT_METADATA_VALUE#" for v in values if v)
        )

    def test_multiple_authors_correct_positions(self):
        loader = self._loader_with([
            self._base_author(is_corresponding=False),
            {**self._base_author(), "author": "Smith A.", "openalex_is_corresponding": True},
        ])
        section = "article_"
        ops = loader._process_and_replace_authors(
            {"sections": {}}, row_id=1,
            base=f"/sections/{section}details", form_section=section,
        )
        corr_ops = [o for o in ops if "epfl.author.corresponding" in o["path"]]
        self.assertEqual(len(corr_ops), 1)
        values = [v.get("value") for v in corr_ops[0]["value"] if v]
        self.assertEqual(values[0], "#PLACEHOLDER_PARENT_METADATA_VALUE#")
        self.assertEqual(values[1], "true")

    def test_no_corresponding_op_for_report(self):
        """report_details child form has no epfl.author.corresponding field."""
        loader = self._loader_with([self._base_author(is_corresponding=True)])
        section = "report_"
        ops = loader._process_and_replace_authors(
            {"sections": {}}, row_id=1,
            base=f"/sections/{section}details", form_section=section,
        )
        corr_ops = [o for o in ops if "epfl.author.corresponding" in o["path"]]
        self.assertEqual(corr_ops, [], "report_ form has no epfl.author.corresponding")

    def test_preprint_gets_corresponding(self):
        loader = self._loader_with([self._base_author(is_corresponding=True)])
        section = "preprint_"
        ops = loader._process_and_replace_authors(
            {"sections": {}}, row_id=1,
            base=f"/sections/{section}details", form_section=section,
        )
        corr_ops = [o for o in ops if "epfl.author.corresponding" in o["path"]]
        self.assertTrue(len(corr_ops) > 0, "preprint_ must support epfl.author.corresponding")


if __name__ == "__main__":
    unittest.main()

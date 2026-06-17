"""Tests for CrossrefClient._extract_ifs3_record_info — no network calls.

_extract_abstract may fall back to OpenAlexClient when no abstract is in the record.
The journal-article fixture includes an abstract so that path is never reached.
The book-chapter fixture has no abstract; OpenAlexClient.fetch_record_by_unique_id
is patched to return None so no HTTP request is made.
"""

import sys
import os
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.crossref_client import Client


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CROSSREF_ARTICLE = {
    "DOI": "10.1038/s41586-024-07592-z",
    "type": "journal-article",
    "title": ["Quantum Computing Advances at EPFL"],
    "container-title": ["Nature"],
    "ISSN": ["0028-0836"],
    "volume": "631",
    "issue": "7",
    "page": "100-110",
    # _extract_pubyear reads from 'issued', not 'published-print'
    "issued": {"date-parts": [[2024, 7, 15]]},
    "published-print": {"date-parts": [[2024, 7, 15]]},
    "publisher": "Springer Nature",
    "author": [
        {
            "given": "Jane",
            "family": "Doe",
            "ORCID": "https://orcid.org/0000-0001-2345-6789",
            "affiliation": [{"name": "EPFL, Lausanne, Switzerland"}],
        },
        {
            "given": "Hans",
            "family": "Müller",
            "affiliation": [{"name": "ETH Zürich"}],
        },
    ],
    "abstract": "<jats:p>Abstract text.</jats:p>",
    "subject": ["Physics", "Computer Science"],
    "funder": [{"name": "SNF", "award": ["200021-123456"]}],
    "alternative-id": [],
    "ISBN": [],
}

CROSSREF_BOOK_CHAPTER = {
    "DOI": "10.1007/978-3-030-12345-6_5",
    "type": "book-chapter",
    "title": ["A Chapter on Neural Networks"],
    "container-title": ["Advances in Machine Learning"],
    "ISBN": ["978-3-030-12345-6"],
    "page": "50-75",
    # _extract_pubyear reads from 'issued', not 'published-print'
    "issued": {"date-parts": [[2023, 9, 1]]},
    "published-print": {"date-parts": [[2023, 9, 1]]},
    "publisher": "Springer",
    "author": [{"given": "Alice", "family": "Smith", "affiliation": []}],
    "alternative-id": [],
    "ISSN": [],
}


def _make_client():
    """Instantiate CrossrefClient without calling __init__ (no HTTP session setup)."""
    return Client.__new__(Client)


# ---------------------------------------------------------------------------
# Journal article tests
# ---------------------------------------------------------------------------

class TestCrossrefClientArticleIfs3(unittest.TestCase):
    """Tests for _extract_ifs3_record_info with a journal-article fixture."""

    def setUp(self):
        self.client = _make_client()
        self.record = self.client._extract_ifs3_record_info(CROSSREF_ARTICLE)

    def test_source_is_crossref(self):
        self.assertEqual(self.record["source"], "crossref")

    def test_internal_id_is_doi(self):
        self.assertEqual(self.record["internal_id"], "10.1038/s41586-024-07592-z")

    def test_doi_is_non_empty(self):
        self.assertTrue(self.record["doi"])

    def test_title_extracted(self):
        self.assertEqual(self.record["title"], "Quantum Computing Advances at EPFL")

    def test_pubyear_is_2024(self):
        # _extract_pubyear returns an int from issued.date-parts[0][0]
        self.assertEqual(self.record["pubyear"], 2024)

    def test_journal_title_is_nature(self):
        self.assertEqual(self.record["journalTitle"], "Nature")

    def test_journal_issn_non_empty(self):
        self.assertTrue(self.record["journalISSN"])

    def test_journal_volume_is_631(self):
        self.assertEqual(self.record["journalVolume"], "631")

    def test_starting_page_is_100(self):
        self.assertEqual(self.record["startingPage"], "100")

    def test_ending_page_is_110(self):
        self.assertEqual(self.record["endingPage"], "110")

    def test_ifs3_collection_is_journal_articles(self):
        self.assertEqual(self.record["ifs3_collection"], "Journal articles")

    def test_dc_type_starts_with_text_journal(self):
        self.assertTrue(self.record["dc.type"].startswith("text::journal"))

    def test_authors_list_has_two_items(self):
        self.assertEqual(len(self.record["authors"]), 2)

    def test_first_author_name_contains_doe(self):
        self.assertIn("Doe", self.record["authors"][0]["author"])

    def test_first_author_orcid_extracted(self):
        # _extract_author_orcid strips the 'https://orcid.org/' prefix
        orcid = self.record["authors"][0]["orcid_id"]
        self.assertIn("0000-0001-2345-6789", orcid)


# ---------------------------------------------------------------------------
# Book chapter tests
# ---------------------------------------------------------------------------

class TestCrossrefClientBookChapterIfs3(unittest.TestCase):
    """Tests for _extract_ifs3_record_info with a book-chapter fixture."""

    def setUp(self):
        self.client = _make_client()
        # Patch the OpenAlex fallback inside _extract_abstract so no HTTP call is made
        patcher = patch(
            "clients.crossref_client.OpenAlexClient.fetch_record_by_unique_id",
            return_value=None,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.record = self.client._extract_ifs3_record_info(CROSSREF_BOOK_CHAPTER)

    def test_ifs3_collection_is_books_and_book_parts(self):
        self.assertEqual(self.record["ifs3_collection"], "Books and Book parts")

    def test_book_title_is_container_title(self):
        self.assertEqual(self.record["bookTitle"], "Advances in Machine Learning")

    def test_book_isbn_non_empty(self):
        self.assertTrue(self.record["bookISBN"])

    def test_source_is_crossref(self):
        self.assertEqual(self.record["source"], "crossref")

    def test_dc_type_starts_with_text_book(self):
        self.assertTrue(self.record["dc.type"].startswith("text::book"))

    def test_title_extracted(self):
        self.assertEqual(self.record["title"], "A Chapter on Neural Networks")

    def test_pubyear_is_2023(self):
        self.assertEqual(self.record["pubyear"], 2023)

    def test_authors_list_has_one_item(self):
        self.assertEqual(len(self.record["authors"]), 1)

    def test_author_name_contains_smith(self):
        self.assertIn("Smith", self.record["authors"][0]["author"])


CROSSREF_BOOK_MONOGRAPH_IN_SERIES = {
    "DOI": "10.1007/978-3-031-69586-5",
    "type": "book",
    "title": ["Stochastic Calculus in Infinite Dimensions and SPDEs"],
    "container-title": ["SpringerBriefs in Mathematics"],
    "ISBN": ["9783031695858", "9783031695865"],
    "ISSN": ["2191-8198", "2191-8201"],
    "issued": {"date-parts": [[2024, 1, 1]]},
    "publisher": "Springer International Publishing",
    "author": [{"given": "Daniel", "family": "Goodair", "affiliation": []}],
    "alternative-id": [],
}

CROSSREF_BOOK_CHAPTER_WITH_SERIES = {
    "DOI": "10.1007/978-3-031-69586-5_2",
    "type": "book-chapter",
    "title": ["Stochastic Calculus in Infinite Dimensions"],
    # Crossref returns series first, then the parent book title
    "container-title": ["SpringerBriefs in Mathematics", "Stochastic Calculus in Infinite Dimensions and SPDEs"],
    "ISBN": ["9783031695858", "9783031695865"],
    "ISSN": ["2191-8198", "2191-8201"],
    "page": "7-56",
    "issued": {"date-parts": [[2024, 1, 1]]},
    "publisher": "Springer International Publishing",
    "author": [{"given": "Daniel", "family": "Goodair", "affiliation": []}],
    "alternative-id": [],
}


class TestCrossrefClientBookMonographInSeriesIfs3(unittest.TestCase):
    """Standalone book with a single container-title (the series): series fields must be set."""

    def setUp(self):
        self.client = _make_client()
        patcher = patch(
            "clients.crossref_client.OpenAlexClient.fetch_record_by_unique_id",
            return_value=None,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.record = self.client._extract_ifs3_record_info(CROSSREF_BOOK_MONOGRAPH_IN_SERIES)

    def test_series_title_is_springerbriefs(self):
        # For a standalone book, single container-title is the series, not a parent book
        self.assertEqual(self.record["seriesTitle"], "SpringerBriefs in Mathematics")

    def test_book_title_is_empty(self):
        # A standalone book has no parent book title
        self.assertEqual(self.record["bookTitle"], "")

    def test_series_issn_non_empty(self):
        self.assertTrue(self.record["seriesISSN"])

    def test_ifs3_collection_is_books_and_book_parts(self):
        self.assertEqual(self.record["ifs3_collection"], "Books and Book parts")


class TestCrossrefClientBookChapterWithSeriesIfs3(unittest.TestCase):
    """Book chapter with two container-titles: first=series, second=book title."""

    def setUp(self):
        self.client = _make_client()
        patcher = patch(
            "clients.crossref_client.OpenAlexClient.fetch_record_by_unique_id",
            return_value=None,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.record = self.client._extract_ifs3_record_info(CROSSREF_BOOK_CHAPTER_WITH_SERIES)

    def test_series_title_is_springerbriefs(self):
        self.assertEqual(self.record["seriesTitle"], "SpringerBriefs in Mathematics")

    def test_book_title_is_parent_book(self):
        self.assertEqual(self.record["bookTitle"], "Stochastic Calculus in Infinite Dimensions and SPDEs")

    def test_series_issn_non_empty(self):
        self.assertTrue(self.record["seriesISSN"])


if __name__ == "__main__":
    unittest.main()

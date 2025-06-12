"""Harvester processor for external sources."""

import abc
import os
import re
import time
from collections import defaultdict
import pandas as pd
from data_pipeline.enricher import AuthorProcessor
from clients.wos_client_v2 import WosClient
from clients.scopus_client import ScopusClient
from clients.zenodo_client import ZenodoClient
from clients.openalex_client import OpenAlexClient
from clients.crossref_client import CrossrefClient
from clients.datacite_client import DataCiteClient
from utils import manage_logger
from config import logs_dir


class Harvester(abc.ABC):

    """
    Abstract base class for harvesters.
    """

    def __init__(
        self, source_name: str, start_date: str, end_date: str, query: str, format: str
    ):
        """
        Initialize the harvester.

        :param source_name: Name of the source (e.g. WOS, Scopus)
        :param publication_date_range: Tuple of (start_date, end_date) for the publication date range
        :param format: output format form metadata
        """
        self.source_name = source_name
        self.start_date = start_date
        self.end_date = end_date
        self.query = query
        self.format = format
        # Create a logger
        log_file_path = os.path.join(logs_dir, "logging.log")
        self.logger = manage_logger(log_file_path)

    @abc.abstractmethod
    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Fetch publications from the source.

        :return: Response from the source
        """
        pass

    def harvest(self) -> pd.DataFrame:
        """
        Harvest publications from the source.

        :return: List of publications
        """
        self.logger.info("Harvesting publications from %s...", self.source_name)
        publications = self.fetch_and_parse_publications()
        self.logger.info(
            "- Found %d %s's publications to be processed for Infoscience",
            len(publications),
            self.source_name,
        )
        return publications


class WosHarvester(Harvester):
    """
    WOS Harvester.
    """

    def __init__(
        self, start_date: str, end_date: str, query: str, format: str = "ifs3"
    ):
        super().__init__("WOS", start_date, end_date, query, format)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing the harvested publications.

        According to the "ifs3" default param, the DataFrame includes the following columns:
        - `source`: The source database of the publication's metadata (value "wos")
        - `internal_id`: The internal ID of the publication in the source KB (WOS:xxxxx).
        - `title`: The title of the publication.
        - `doi`: The Digital Object Identifier of the publication.
        - `doctype`: The type of the publication (e.g., Article, Book Chapter, etc.).
        - `pubyear`: The year of publication.
        - `ifs3_collection`: The IFS3 doctype of the publication.
        - `ifs3_collection_id`: The IFS3 collection ID of the publication.
        - `authors`: A list of authors, each represented as a dictionary containing `author`, `orcid_id`, `internal_author_id`, `organizations`, and `suborganization`.
        """

        createdTimeSpan = f"{self.start_date}+{self.end_date}"
        total = WosClient.count_results(
            usrQuery=self.query, createdTimeSpan=createdTimeSpan
        )
        self.logger.info("- Total publications found in WOS: %s", total)

        if total == 0:
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()

        total = int(total) 
        count = 20
        recs = []

        if total == 1:
            self.logger.info("Only one publication found. Fetching the single record.")
            recs = WosClient.fetch_records(
                format=self.format,
                usrQuery=self.query,
                count=1,
                firstRecord=1,
                createdTimeSpan=createdTimeSpan,
            )
        else:
            for i in range(1, total + 1, count):
                self.logger.info(
                    "Harvesting publications %d to %d on a total of %d publications",
                    i, min(i + count - 1, total), total
                )
                h_recs = WosClient.fetch_records(
                    format=self.format,
                    usrQuery=self.query,
                    count=count,
                    firstRecord=i,
                    createdTimeSpan=createdTimeSpan,
                )
                recs.extend(h_recs)
        df = (
            pd.DataFrame(recs)
            .query('ifs3_collection != "unknown"')
            .reset_index(drop=True)
        )

        # author_processor = AuthorProcessor(df)

        # df = df[
        #     df["affiliation_controlled"].isna()
        #     | df["affiliation_controlled"].astype(str).str.strip().eq("")
        #     | df["affiliation_controlled"]
        #     .astype(str)
        #     .apply(lambda x: author_processor.process_scopus(x, check_all=True))
        # ]

        return df


class ScopusHarvester(Harvester):
    """
    Scopus Harvester.
    """

    def __init__(
        self, start_date: str, end_date: str, query: str, format: str = "ifs3"
    ):
        super().__init__("Scopus", start_date, end_date, query, format)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing the harvested publications from Scopus.

        According to the "ifs3" default param, the DataFrame includes the following columns:
        - `source`: The source database of the publication's metadata (value "scopus")
        - `internal_id`: The internal ID of the publication in the source KB (SCOPUS_ID:xxxxx).
        - `title`: The title of the publication.
        - `doi`: The Digital Object Identifier of the publication.
        - `doctype`: The type of the publication (e.g., Article, Book Chapter, etc.).
        - `pubyear`: The year of publication.
        - `ifs3_collection`: The IFS3 doctype of the publication.
        - `ifs3_collection_id`: The IFS3 collection ID of the publication.
        - `authors`: A list of authors, each represented as a dictionary containing `author`, `orcid_id`, `internal_author_id`, `organizations`, and `suborganization`.
        """
        # updated_query = f'({self.query}) AND (ORIG-LOAD-DATE AFT {self.start_date.strftime("%Y-%m-%d").replace("-","")}) AND (ORIG-LOAD-DATE BEF {self.end_date.strftime("%Y-%m-%d").replace("-","")})'
        updated_query = f'({self.query}) AND (ORIG-LOAD-DATE AFT {self.start_date.replace("-","")}) AND (ORIG-LOAD-DATE BEF {self.end_date.replace("-","")})'
        total = ScopusClient.count_results(query=updated_query)
        self.logger.info("- Total publications found in Scopus: %s", total)

        if total == "0":  # scopus API returns 0 as string
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()

        total = int(total)  # Convert total to integer for calculations
        count = 50
        recs = []

        # Special case: Handle single result
        if total == 1:
            self.logger.info("Only one publication found. Fetching the single record.")
            recs = ScopusClient.fetch_records(
                format=self.format, query=updated_query, count=1, start=0
            )
        else:
            for i in range(0, total, count):
                self.logger.info(
                    "Harvest publications %d to %d on a total of %d publications",
                    i + 1, min(i + count, total), total
                )
                h_recs = ScopusClient.fetch_records(
                    format=self.format, query=updated_query, count=count, start=i
                )
                recs.extend(h_recs)

        # Keep only valid ifs3 doctypes
        df = (
            pd.DataFrame(recs)
            .query('ifs3_collection != "unknown"')  # Filter out unknown_doctype
            .reset_index(drop=True)
        )

        return df


class ZenodoHarvester(Harvester):
    """
    Zenodo Harvester.
    """

    policy_threshold = pd.to_datetime("2023-03-01")
    # older_recid = "7712815"

    def __init__(
        self, start_date: str, end_date: str, query: str, format: str = "ifs3"
    ):
        super().__init__("Zenodo", start_date, end_date, query, format)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing objects harvested from Zenodo.

        Using the "ifs3" default format, the DataFrame includes the following:
        - `source`: source database of the object's metadata (value "zenodo")
        - `internal_id`: internal ID of the object in the source DB (xxxxx).
        - `title`: The title of the object.
        - `doi`: Digital Object Identifier of the object.
        - `doctype`: type of the object (e.g., Dataset, Article, etc.).
        - `pubyear`: year of publication.
        - `ifs3_collection`: IFS3 collection of the object.
        - `ifs3_collection_id`: IFS3 collection ID of the object.
        - `authors`: list of creators, each represented as a dict containing:
            - `author`
            - `orcid_id`
            - `internal_author_id` (empty for Zenoodo)
            - `organizations`
            - `suborganization`
        """

        columns = (
            "source",
            "internal_id",
            "title",
            "doi",
            "doctype",
            "pubyear",
            "ifs3_collection",
            "ifs3_collection_id",
            "authors",
            "first_creation",
        )
        empty_data = {}
        for c in columns:
            empty_data[c] = []

        updated_query = " AND ".join(
            [self.query, f"created:[{self.start_date} TO {self.end_date}]"]
        )

        total = ZenodoClient.count_results(q=updated_query)
        self.logger.info("- Number of objects found in Zenodo: %s", total)
        if total == 0:
            self.logger.warning("No object found. Returning an empty DataFrame.")
            return pd.DataFrame()
        size = 50
        recs = []
        for i in range(0, 1 + int(total) // size):
            self.logger.info(
                "Harvest objects %d to %d out of %d", i * size + 1, min((i + 1) * size, total), total
            )
            h_recs = ZenodoClient.fetch_records(
                format=self.format, q=updated_query, size=size, page=i + 1
            )
            if h_recs is not None:
                recs.extend(h_recs)
            time.sleep(30)
        # Keep only valid ifs3 doctypes, filter out unknown_doctype
        # print(recs)
        df = (
            pd.DataFrame(recs)
            .query('ifs3_collection != "unknown"')
            .query(f'first_creation > "{self.policy_threshold}"')
            .reset_index(drop=True)
        )

        return df


class OpenAlexHarvester(Harvester):
    """
    OpenAlex Harvester.
    """

    def __init__(
        self, start_date: str, end_date: str, query: str, format: str = "ifs3"
    ):
        super().__init__("OpenAlex", start_date, end_date, query, format)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing the harvested publications from OpenAlex.

        The DataFrame includes columns based on the specified format, such as:
        - `source`: The source database of the publication's metadata (value "openalex")
        - `internal_id`: The internal ID of the publication in the source KB (OpenAlex ID).
        - `title`: The title of the publication.
        - `doi`: The Digital Object Identifier of the publication.
        - `doctype`: The type of the publication (e.g., Article, Book Chapter, etc.).
        - `pubyear`: The year of publication.
        - `ifs3_collection`: The IFS3 collection of the publication.
        - `ifs3_collection_id`: The IFS3 collection ID of the publication.
        - `authors`: A list of authors, each represented as a dictionary.
        """

        # Formulate the filter for the query
        filters = (
            f"from_publication_date:{self.start_date},"
            f"to_publication_date:{self.end_date},"
            f"{self.query}"
        )

        # Count total publications to manage progress logging
        total = OpenAlexClient.count_results(filter=filters)
        self.logger.info("- Total publications found in OpenAlex: %s", total)

        if total == 0:
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()

        count = 50  # Number of items per request
        recs = []
        cursor = "*"
        page = 1

        while True:
            self.logger.info("Harvesting page %d (cursor: %s)", page, cursor)

            try:
                h_recs = OpenAlexClient.fetch_records(
                    format=self.format, filter=filters, per_page=count, cursor=cursor
                )

                if h_recs:
                    recs.extend(h_recs)
                else:
                    self.logger.warning("No records found for page %d.", page)
                    break

                # Get the next cursor from the last API response
                last_response_meta = OpenAlexClient.last_response.get("meta", {})
                cursor = last_response_meta.get("next_cursor", None)

                if not cursor:
                    self.logger.info("No next cursor found. Pagination complete.")
                    break

                page += 1

            except Exception as e:
                self.logger.error("Error fetching records for page %d: %s", page, e)
                break

        # Build the DataFrame if records were collected
        if recs:
            df = (
                pd.DataFrame(recs)
                .query('ifs3_collection != "unknown"')  # Filter out unmapped types
                .reset_index(drop=True)
            )
            df = pd.DataFrame(recs).reset_index(drop=True)
        else:
            self.logger.debug("No valid records fetched. Returning an empty DataFrame.")
            return pd.DataFrame()

        return df


class CrossrefHarvester(Harvester):
    """
    Crossref Harvester.
    """

    def __init__(
        self,
        start_date: str,
        end_date: str,
        query: str,
        format: str = "ifs3",
        field_queries: dict = None,
    ):
        """
        Initialize the Crossref harvester.

        :param start_date: Start date for the publication date range (YYYY-MM-DD)
        :param end_date: End date for the publication date range (YYYY-MM-DD)
        :param query: A generic query string to search across all fields
        :param format: Output format for metadata (default "ifs3")
        :param field_queries: Optional dictionary with additional targeted query parameters.
            Example:
            {
                "query.author": "Smith",
                "query.title": "machine learning",
                "query.affiliation": "Harvard"
            }
        """
        super().__init__("Crossref", start_date, end_date, query, format)
        self.field_queries = field_queries or {}

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing the harvested publications from Crossref.

        The DataFrame includes columns based on the specified format, such as:
        - `source`: The source of the publication's metadata (value "crossref")
        - `internal_id`: The internal ID of the publication in the source KB (DOI)
        - `title`: The title of the publication.
        - `doi`: The Digital Object Identifier of the publication.
        - `doctype`: The type of the publication (e.g., Article, Book Chapter, etc.).
        - `pubyear`: The year of publication.
        - `ifs3_collection`: The IFS3 collection of the publication.
        - `ifs3_collection_id`: The IFS3 collection ID of the publication.
        - `authors`: A list of authors as dictionaries.
        """
        # Build the parameter dictionary for targeted queries.
        params = {}
        if self.query:
            params["query"] = self.query
        params.update(self.field_queries)
        params["filter"] = (
            f"from-created-date:{self.start_date},until-created-date:{self.end_date}"
        )

        total = CrossrefClient.count_results(**params)
        self.logger.info("- Total publications found in Crossref: %s", total)

        if total == 0:
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()

        total = int(total)
        count = 50
        recs = []

        for offset in range(0, total, count):
            self.logger.info(
                "Harvesting records %d to %d of %d",
                offset + 1, min(offset + count, total), total
            )
            try:
                h_recs = CrossrefClient.fetch_records(
                    format=self.format,
                    rows=count,
                    offset=offset,
                    **params,
                )
                if h_recs:
                    recs.extend(h_recs)
                else:
                    self.logger.warning("No records found at offset %d.", offset)
            except Exception as e:
                self.logger.error("Error fetching records at offset %d: %s", offset, e)

        if recs:
            df = (
                pd.DataFrame(recs)
                .query('ifs3_collection != "unknown"')
                .reset_index(drop=True)
            )
        else:
            self.logger.debug("No valid records fetched. Returning an empty DataFrame.")
            return pd.DataFrame()

        return df


class OpenAlexCrossrefHarvester(Harvester):
    """
    Harvests DOIs via OpenAlex, and fetches rich metadata from Crossref.
    """

    def __init__(
        self, start_date: str, end_date: str, query: str, format: str = "ifs3"
    ):
        super().__init__("openalex+crossref", start_date, end_date, query, format)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        1. Fetch full OpenAlex records (with authors + affiliations).
        2. Enrich each DOI with Crossref metadata (title, type, etc.).
        3. Merge and return a normalized DataFrame.
        """
        self.logger.info("Fetching records from OpenAlex...")

        filters = (
            f"from_publication_date:{self.start_date},"
            f"to_publication_date:{self.end_date},"
            f"{self.query}"
        )

        try:
            openalex_records = OpenAlexClient.fetch_records(
                format="openalex", filter=filters
            )
        except Exception as e:
            self.logger.error("Failed to fetch records from OpenAlex: %s", e)
            return pd.DataFrame()

        if not openalex_records:
            self.logger.warning("No records found in OpenAlex. Returning empty DataFrame.")
            return pd.DataFrame()

        self.logger.info("- %d records retrieved from OpenAlex.", len(openalex_records))

        results = []

        for idx, oa_rec in enumerate(openalex_records, 1):
            doi = OpenAlexClient.openalex_extract_doi(oa_rec)
            if not doi:
                continue

            self.logger.info(
                "[%d/%d] Fetching Crossref metadata for DOI: %s", idx, len(openalex_records), doi
            )
            try:
                record = CrossrefClient.fetch_record_by_unique_id(
                    doi=doi, format=self.format
                )
                if record:
                    record["source"] = "openalex+crossref"
                    record["authors"] = OpenAlexClient.extract_ifs3_authors(oa_rec)
                    results.append(record)
            except Exception as e:
                self.logger.warning("Failed to enrich DOI %s: %s", doi, e)
                continue

        if not results:
            self.logger.warning(
                "No valid enriched records returned. Returning empty DataFrame."
            )
            return pd.DataFrame()

        return (
            pd.DataFrame(results)
            .query('ifs3_collection != "unknown"')
            .reset_index(drop=True)
        )


class DataCiteHarvester(Harvester):
    """
    Harvests records from datacite.
    """
    def __init__(
        self,
        start_date: str,
        end_date: str,
        query: str = None,
        format: str = "ifs3",
        filters: dict = None,
    ):
        super().__init__("DataCite", start_date, end_date, query, format)
        self.filters = filters or {}

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        # Construct DataCite API filters with date range
        api_filters = {
            "published": f"{self.start_date},{self.end_date}",
            "state": "findable",
        }
        api_filters.update(self.filters)

        self.logger.info("Querying DataCite with filters: %s", api_filters)

        total = DataCiteClient.count_results(
            query=self.query,
            filters=api_filters,
        )
        self.logger.info("- Total publications found in DataCite : %s", total)

        if not total:
            return pd.DataFrame()

        # Use classic page-number-based pagination to fetch all records
        recs = DataCiteClient.fetch_records(
            format=self.format,
            query=self.query,
            filters=api_filters,
            page_size=100,  # Maximize efficiency
        )

        if not recs:
            self.logger.warning("No records returned after fetch.")
            return pd.DataFrame()

        df = (
            pd.DataFrame(recs)
            .query('ifs3_collection != "unknown"')
            .reset_index(drop=True)
        )
        df_deduplicate_versions = self._deduplicate_versions(df)
        return df_deduplicate_versions

    def _deduplicate_versions(self, df: pd.DataFrame) -> pd.DataFrame:
        """        
        Deduplicate a DataFrame of publications by:
        A) Preliminary: for each connected group of DOIs (via HasVersion or IsVersionOf),
            keep only the row with the most recent 'registered' date (tie-breaker: highest numeric suffix).
        B) Then apply existing rules:
            1) Remove rows whose HasVersion lists any DOI present in internal_id.
            2) For each parent DOI in IsVersionOf, keep only the row with the highest suffix.
        """
        # Normalize
        df = df.copy()
        df["HasVersion"] = df["HasVersion"].fillna("").astype(str)
        df["IsVersionOf"] = df["IsVersionOf"].fillna("").astype(str)
        df["internal_id"] = df["internal_id"].astype(str)

        # Split by client
        mask_zen = df.get("client", "") == "cern.zenodo"
        df_zen = df[mask_zen]
        df_others = df[~mask_zen]

        # Zenodo: rule2 then rule1
        denom_zen = self._apply_isversionof(df_zen)
        final_zen = self._apply_hasversion(denom_zen)

        # Others: registered filter, rule2, then rule1
        filtered = self._filter_by_registered(df_others)
        denom_oth = self._apply_isversionof(filtered)
        final_oth = self._apply_hasversion(denom_oth)

        # Combine
        result = pd.concat([final_zen, final_oth], ignore_index=True)
        return result.reset_index(drop=True)

    def _parse_versions(self, cell: str) -> list[str]:
        parts = [p.strip() for p in cell.split("||") if p and p.strip()]
        return [re.sub(r"^https?://(?:dx\.)?doi\.org/", "", part) for part in parts]

    def _extract_suffix(self, doi: str) -> int:
        match = re.search(r"(\d+)$", doi)
        return int(match.group(1)) if match else -1

    def _apply_hasversion(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop rows whose HasVersion lists any remaining internal_id
        remaining = set(df["internal_id"])
        mask = df["HasVersion"].apply(
            lambda cell: any(doi in remaining for doi in self._parse_versions(cell))
        )
        return df[~mask].reset_index(drop=True)

    def _apply_isversionof(self, df: pd.DataFrame) -> pd.DataFrame:
        # Keep highest suffix per parent DOI
        df = df.copy()
        df["_suffix"] = df["internal_id"].map(self._extract_suffix)
        parent_to_idxs: dict[str, list[int]] = defaultdict(list)
        no_parent: list[int] = []
        for idx, cell in df["IsVersionOf"].items():
            parents = self._parse_versions(cell)
            if not parents:
                no_parent.append(idx)
            else:
                for p in parents:
                    parent_to_idxs[p].append(idx)
        keep = set(no_parent)
        for idxs in parent_to_idxs.values():
            best = max(idxs, key=lambda i: df.at[i, "_suffix"])
            keep.add(best)
        return df.loc[sorted(keep)].drop(columns=["_suffix"])

    def _filter_by_registered(self, df: pd.DataFrame) -> pd.DataFrame:
        # For non-Zenodo: keep most recent registered per versions group
        df = df.copy()
        df["registered_dt"] = pd.to_datetime(df.get("registered", ""), errors="coerce")
        primary = set(df["internal_id"])
        # Build graph
        neigh: dict[str, set[str]] = defaultdict(set)
        for _, row in df.iterrows():
            me = row["internal_id"]
            for doi in self._parse_versions(row["HasVersion"]) + self._parse_versions(
                row["IsVersionOf"]
            ):
                if doi in primary:
                    neigh[me].add(doi)
                    neigh[doi].add(me)
        # Explore components
        visited, groups = set(), []
        for doi in primary:
            if doi in visited:
                continue
            stack, comp = [doi], {doi}
            visited.add(doi)
            while stack:
                cur = stack.pop()
                for nbr in neigh[cur]:
                    if nbr not in visited:
                        visited.add(nbr)
                        comp.add(nbr)
                        stack.append(nbr)
            groups.append(comp)
        # Select per group
        keep = set()
        for comp in groups:
            if len(comp) == 1:
                keep |= comp
            else:
                sub = df[df["internal_id"].isin(comp)]
                max_date = sub["registered_dt"].max()
                cand = sub[sub["registered_dt"] == max_date]
                if len(cand) > 1:
                    cand = cand.copy()
                    cand["_suffix"] = cand["internal_id"].map(self._extract_suffix)
                    max_suf = cand["_suffix"].max()
                    doi_keep = cand[cand["_suffix"] == max_suf]["internal_id"].iloc[0]
                else:
                    doi_keep = cand["internal_id"].iloc[0]
                keep.add(doi_keep)
        return df[df["internal_id"].isin(keep)].drop(columns=["registered_dt"]).copy()

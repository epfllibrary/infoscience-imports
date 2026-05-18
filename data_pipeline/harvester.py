"""Harvester processor for external sources."""

import abc
import os
import re
import time
from collections import defaultdict
import pandas as pd
import json
from data_pipeline.enricher import AuthorProcessor
from clients.wos_client_v2 import WosClient
from clients.scopus_client import ScopusClient
from clients.zenodo_client import ZenodoClient
from clients.openalex_client import OpenAlexClient
from clients.crossref_client import CrossrefClient
from clients.datacite_client import DataCiteClient
from clients.epo_ops_client import EPOClient
from utils import get_pipeline_logger


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
        self.logger = get_pipeline_logger(self.__class__.__name__.lower())

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
        self.logger.info("[%s] Starting harvest", self.source_name)
        publications = self.fetch_and_parse_publications()
        self.logger.info("[%s] %d publication(s) ready for processing", self.source_name, len(publications))
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
        self.logger.debug("[WOS] Query: %s", self.query)
        createdTimeSpan = f"{self.start_date}+{self.end_date}"
        total = WosClient.count_results(
            usrQuery=self.query, createdTimeSpan=createdTimeSpan
        )
        self.logger.info("[WOS] %s result(s) found", total)

        if total == 0:
            return pd.DataFrame()

        total = int(total)
        count = 20
        recs = []

        if total == 1:
            self.logger.debug("[WOS] Single record — fetching directly")
            recs = WosClient.fetch_records(
                format=self.format,
                usrQuery=self.query,
                count=1,
                firstRecord=1,
                createdTimeSpan=createdTimeSpan,
            )
        else:
            for i in range(1, total + 1, count):
                self.logger.debug(
                    "[WOS] Fetching records %d–%d / %d",
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
        updated_query = f'({self.query}) AND (ORIG-LOAD-DATE AFT {self.start_date.replace("-","")}) AND (ORIG-LOAD-DATE BEF {self.end_date.replace("-","")})'
        self.logger.debug("[Scopus] Query: %s", updated_query)
        total = ScopusClient.count_results(query=updated_query)
        self.logger.info("[Scopus] %s result(s) found", total)

        if total == "0":
            return pd.DataFrame()

        total = int(total)
        count = 50
        recs = []

        if total == 1:
            self.logger.debug("[Scopus] Single record — fetching directly")
            recs = ScopusClient.fetch_records(
                format=self.format, query=updated_query, count=1, start=0
            )
        else:
            for i in range(0, total, count):
                self.logger.debug(
                    "[Scopus] Fetching records %d–%d / %d",
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
            - `internal_author_id` (empty for Zenodo)
            - `organizations`
            - `suborganization`
        """
        self.logger.debug("[Zenodo] Query: %s", self.query)
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
        empty_data = {c: [] for c in columns}

        updated_query = " AND ".join(
            [self.query, f"created:[{self.start_date} TO {self.end_date}]"]
        )

        total = int(ZenodoClient.count_results(q=updated_query))
        self.logger.info("[Zenodo] %d result(s) found", total)

        if total == 0:
            return pd.DataFrame(columns=columns)

        size = 25
        recs: list[dict] = []

        num_pages = (total + size - 1) // size

        for page in range(1, num_pages + 1):
            start_idx = (page - 1) * size + 1
            end_idx = min(page * size, total)
            self.logger.debug("[Zenodo] Fetching records %d–%d / %d", start_idx, end_idx, total)

            h_recs = ZenodoClient.fetch_records(
                format=self.format,
                q=updated_query,
                size=size,
                page=page,
            )
            if h_recs:
                recs.extend(h_recs)

            time.sleep(30)

        # Keep only valid ifs3 doctypes, filter out unknown_doctype
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
        self.logger.info("Fetching records from OpenAlex with query: %s", self.query)
        # Formulate the filter for the query
        filters = (
            f"from_publication_date:{self.start_date},"
            f"to_publication_date:{self.end_date},"
            f"{self.query}"
        )

        # Count total publications to manage progress logging
        total = OpenAlexClient.count_results(filter=filters)
        self.logger.info("[OpenAlex] %s result(s) found", total)

        try:
            openalex_records = OpenAlexClient.fetch_records(
                format=self.format, filter=filters
            )
        except Exception as e:
            self.logger.error("[OpenAlex] Failed to fetch records: %s", e)
            return pd.DataFrame()

        if not openalex_records:
            self.logger.info("[OpenAlex] No records returned")
            return pd.DataFrame()

        df = pd.DataFrame(openalex_records)

        if "ifs3_collection" in df.columns:
            df = df.query('ifs3_collection != "unknown"')

        df = df.copy()
        df["source"] = "openalex"

        return df.reset_index(drop=True)

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
        self.logger.info("Fetching records from Crossref with query: %s", self.query)
        # Build the parameter dictionary for targeted queries.
        params = {}
        if isinstance(self.query, dict):
            params.update(self.query)  # utiliser tel quel
        elif isinstance(self.query, str):
            try:
                parsed_query = json.loads(self.query)
                if isinstance(parsed_query, dict):
                    params.update(parsed_query)
                else:
                    self.logger.warning("Parsed self.query is not a dictionary.")
                    params["query"] = self.query
            except json.JSONDecodeError:
                self.logger.warning("Failed to parse self.query as JSON string.")
                params["query"] = self.query
        elif self.query:
            params["query"] = self.query

        params.update(self.field_queries)
        params["filter"] = (
            f"from-created-date:{self.start_date},until-created-date:{self.end_date}"
        )
        total = CrossrefClient.count_results(**params)
        self.logger.info("[Crossref] %s result(s) found", total)

        if total == 0:
            return pd.DataFrame()

        total = int(total)
        count = 50
        recs = []

        for offset in range(0, total, count):
            self.logger.debug(
                "[Crossref] Fetching records %d–%d / %d",
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
                    self.logger.warning("[Crossref] No records at offset %d", offset)
            except Exception as e:
                self.logger.error("[Crossref] Error at offset %d: %s", offset, e)

        if recs:
            df = (
                pd.DataFrame(recs)
                .query('ifs3_collection != "unknown"')
                .reset_index(drop=True)
            )
        else:
            self.logger.debug("No valid records fetched. Returning an empty DataFrame.")
            return pd.DataFrame()

        epfl_pattern = re.compile(
            r"(?:EPFL|[Pp]olytechnique\s+[Ff].d.rale\s+de\s+Lausanne"
            r"|[Ss]wiss\s+[Ff]ederal\s+[Ii]nstitute\s+of\s+[Tt]echnology\s+in\s+[Ll]ausanne)"
        )

        def _has_epfl_affiliation(authors):
            if not isinstance(authors, list):
                return False
            return any(
                epfl_pattern.search(str(a.get("organizations", "")))
                for a in authors
                if isinstance(a, dict)
            )

        before = len(df)
        df = df[df["authors"].apply(_has_epfl_affiliation)].reset_index(drop=True)
        filtered = before - len(df)
        if filtered:
            self.logger.info(
                "Filtered out %d record(s) with no EPFL affiliation (%d remaining).",
                filtered, len(df),
            )

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
        self.logger.debug("[OpenAlex+Crossref] Query: %s", self.query)

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
            self.logger.error("[OpenAlex+Crossref] Failed to fetch OpenAlex records: %s", e)
            return pd.DataFrame()

        if not openalex_records:
            self.logger.info("[OpenAlex+Crossref] No records returned")
            return pd.DataFrame()

        self.logger.info("[OpenAlex+Crossref] %d OpenAlex record(s) — enriching with Crossref", len(openalex_records))

        results = []

        for idx, oa_rec in enumerate(openalex_records, 1):
            doi = OpenAlexClient.openalex_extract_doi(oa_rec)
            if not doi:
                continue

            self.logger.debug("[OpenAlex+Crossref] [%d/%d] Enriching DOI: %s", idx, len(openalex_records), doi)
            try:
                record = CrossrefClient.fetch_record_by_unique_id(
                    doi=doi, format=self.format
                )
                if record:
                    record["source"] = "openalex+crossref"
                    record["authors"] = OpenAlexClient.extract_ifs3_authors(oa_rec)
                    results.append(record)
            except Exception as e:
                self.logger.warning("[OpenAlex+Crossref] Failed to enrich DOI %s: %s", doi, e)
                continue

        if not results:
            self.logger.warning("[OpenAlex+Crossref] No enriched records after Crossref lookup")
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

        self.logger.debug("[DataCite] Filters: %s", api_filters)

        total = DataCiteClient.count_results(
            query=self.query,
            filters=api_filters,
        )
        self.logger.info("[DataCite] %s result(s) found", total)

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

class EPOHarvester(Harvester):
    """
    EPO OPS (Espacenet) Harvester.
    """

    def __init__(
        self,
        start_date: str,
        end_date: str,
        query: str,
        format: str = "ifs3",
        constituents: list[str] | None = None,
        per_page: int = 25,
        max_records: int | None = None,
        group_by_family: bool = True,
    ):
        super().__init__("EPO", start_date, end_date, query, format)
        self.constituents = constituents  # ex: ["biblio"] (optionnel)
        self.per_page = per_page
        self.max_records = max_records
        self.group_by_family = group_by_family

        # client instance (OPS uses env credentials)
        self.client = EPOClient()

    @staticmethod
    def _yyyymmdd(date_str: str) -> str:
        # expected input "YYYY-MM-DD" (as used elsewhere in your pipeline)
        return (date_str or "").replace("-", "").strip()


    def _build_cql(self) -> str:
        """
        Build an EPO OPS CQL query using:
        pd within "YYYYMMDD,YYYYMMDD"
        """
        q = (self.query or "").strip()
        if not q:
            raise ValueError("EPOHarvester.query is empty; provide a CQL fragment.")

        sd = self._yyyymmdd(self.start_date)
        ed = self._yyyymmdd(self.end_date)

        parts = [f"({q})"]

        if len(sd) == 8 and sd.isdigit() and len(ed) == 8 and ed.isdigit():
            parts.append(f'pd within "{sd},{ed}"')
        else:
            # Defensive: don't silently harvest everything if dates are malformed
            raise ValueError(
                f"Invalid date range for OPS 'pd within': start_date={self.start_date} end_date={self.end_date}"
            )

        return " AND ".join(parts)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing patent publications harvested from EPO OPS.

        Output columns depend on your EPOClient format, but you should end up with at least:
        - source, internal_id, title, doctype, pubyear
        and any extra patent-specific fields (family_id, applicants, inventors, issueDate, etc.).
        """
        cql = self._build_cql()
        self.logger.debug("[EPO] CQL: %s", cql)

        try:
            total = self.client.count_results(cql=cql)
            self.logger.info("[EPO] %s result(s) found", total)
        except Exception as e:
            self.logger.warning("[EPO] Could not count results (continuing): %s", e)
            total = None

        try:
            recs = self.client.fetch_records(
                cql=cql,
                format=self.format,
                per_page=self.per_page,
                max_records=self.max_records,
                constituents=self.constituents,
                group_by_family=self.group_by_family,
            )
        except Exception as e:
            self.logger.error("Failed to fetch records from EPO OPS: %s", e)
            return pd.DataFrame()

        if not recs:
            self.logger.warning(
                "No records returned by EPO OPS. Returning empty DataFrame."
            )
            return pd.DataFrame()

        df = pd.DataFrame(recs).reset_index(drop=True)

        # Harmonisation minimale attendue downstream
        if "source" not in df.columns:
            df["source"] = "epo"

        # Ta sortie EPO a déjà doctype="Patent" et pubyear, donc rien à filtrer ici.
        # Si tu veux protéger le pipeline contre des records incomplets:
        needed = ["internal_id", "title", "doctype"]
        missing = [c for c in needed if c not in df.columns]
        if missing:
            self.logger.warning("EPO dataframe missing expected columns: %s", missing)

        return df

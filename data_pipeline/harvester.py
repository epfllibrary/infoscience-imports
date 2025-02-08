"""Harvester processor for external sources."""

import abc
import os

import pandas as pd
from data_pipeline.enricher import AuthorProcessor
from clients.wos_client_v2 import WosClient
from clients.scopus_client import ScopusClient
from clients.zenodo_client import ZenodoClient
from clients.openalex_client import OpenAlexClient

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
        log_file_path = os.path.join(logs_dir, "harvesting.log")
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
        self.logger.info(f"Harvesting publications from {self.source_name}...")
        publications = self.fetch_and_parse_publications()
        self.logger.info(
            f"- Nombre de publications {self.source_name} compatibles Infoscience {len(publications)}"
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
        self.logger.info(f"- Nombre de publications trouvées dans le WOS: {total}")

        if total == 0:
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()

        total = int(total)  # Assurez-vous que `total` est un entier
        count = 50
        recs = []

        # Cas spécial : un seul résultat
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
                    f"Harvesting publications {i} to {min(i + count - 1, total)} on a total of {total} publications"
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
            .query('ifs3_collection != "unknown"')  # Filtrer les ifs3 inconnus
            .reset_index(drop=True)
        )

        author_processor = AuthorProcessor(df)

        df = df[
            df["affiliation_controlled"].isna() |  
            df["affiliation_controlled"].astype(str).str.strip().eq("") | 
            df["affiliation_controlled"].astype(str).apply(lambda x: author_processor.process_scopus(x, check_all=True))
        ]

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
        self.logger.info(f"- Nombre de publications trouvées dans Scopus: {total}")
        
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
                    f"Harvest publications {i + 1} to {min(i + count, total)} on a total of {total} publications"
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

    def __init__(
        self, start_date: str, end_date: str, query: str, format: str = "ifs3"
    ):
        super().__init__("Zenodo", start_date, end_date, query, format)

    def fetch_and_parse_publications(self) -> pd.DataFrame:
        """
        Returns a pandas DataFrame containing objeczs harvested from Zenodo.

        Using the "ifs3" default format, the DataFrame includes the following:
        - `source`: source database of the objecz's metadata (value "zenodo")
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
        )
        empty_data = {}
        for c in columns:
            empty_data[c] = []

        updated_query = " AND ".join(
            [self.query, f"created:[{self.start_date} TO {self.end_date}]"]
        )

        total = ZenodoClient.count_results(q=updated_query)
        self.logger.info(f"- Nombre d'objets trouvées dans Zenodo: {total}")
        if total == 0:  # Zenodo API returns 0 as string
            self.logger.warning("No object found. Returning an empty DataFrame.")
            return pd.DataFrame()
        size = 50
        recs = []
        # print(total)
        # print(range(0, 1 + int(total) // size))
        for i in range(0, 1 + int(total) // size):
            self.logger.info(
                f"Harvest objects {i*size*(int(total)//size)+1} to {i*size*(int(total)//size)} out of a total of {total} objects"
            )
            h_recs = ZenodoClient.fetch_records(
                format=self.format, q=updated_query, size=size, page=i + 1
            )
            if h_recs is not None:
                recs.extend(h_recs)
        # Keep only valid ifs3 doctypes, filter out unknown_doctype
        # print(recs)
        df = (
            pd.DataFrame(recs)
            .query('ifs3_collection != "unknown"')
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

        # Count total publications to manage pagination
        total = OpenAlexClient.count_results(filter=filters)
        self.logger.info(f"- Nombre de publications trouvées dans OpenAlex: {total}")

        if total == 0:
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()

        count = 50  # Set a per_page count for pagination
        recs = []

        # Fetch records in pages of `count` items each
        for page in range(1, (total // count) + 2):  # Adjusted to +2 to handle last page
            self.logger.info(f"Harvesting page {page} out of {total // count + 1}")

            try:
                h_recs = OpenAlexClient.fetch_records(
                    format=self.format, filter=filters, per_page=count, page=page
                )
                if h_recs:
                    recs.extend(h_recs)
                else:
                    self.logger.warning(f"No records found for page {page}.")
            except Exception as e:
                self.logger.error(f"Error fetching records for page {page}: {e}")

        # Check if valid records were fetched
        if recs:
            df = (
                pd.DataFrame(recs)
                .query('ifs3_collection != "unknown"')  # Filter out unknown_doctype
                .reset_index(drop=True)
            )
        else:
            self.logger.debug("No valid records fetched. Returning an empty DataFrame.")
            return pd.DataFrame()

        return df

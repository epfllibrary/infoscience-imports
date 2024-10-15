import abc

import pandas as pd
from clients.wos_client_v2 import WosClient
from clients.scopus_client import ScopusClient
from clients.zenodo_client import ZenodoClient
from utils import manage_logger


class Harvester(abc.ABC):

    """
    Abstract base class for harvesters.
    """

    def __init__(self, source_name: str, start_date: str, end_date: str, query: str, format: str):
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
        self.logger = manage_logger("./logs/harvesting.log")  

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
            f"- Nombre de publications {self.source_name} avec un doctype compatible Infoscience {len(publications)}"
        )
        return publications


class WosHarvester(Harvester):
    """
    WOS Harvester.
    """

    def __init__(self, start_date: str, end_date: str, query: str, format: str = "ifs3"):
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
        - `ifs3_doctype`: The IFS3 doctype of the publication.
        - `ifs3_collection_id`: The IFS3 collection ID of the publication.
        - `authors`: A list of authors, each represented as a dictionary containing `author`, `orcid_id`, `internal_author_id`, `organizations`, and `suborganization`.
        """

        createdTimeSpan = f"{self.start_date}+{self.end_date}"
        total = WosClient.count_results(
            usrQuery=self.query, createdTimeSpan=createdTimeSpan
        )
        self.logger.info(f"- Nombre de publications trouvées dans le WOS: {total}")
        if total == 0:
            return pd.DataFrame()
        recs = []
        count = 50
        for i in range(1, int(total), int(count)):
            self.logger.info(
                f"Harvest publications {str(i)} to {str(int(i) + int(count))} on a total of {str(total)} publications"
            )
            h_recs = WosClient.fetch_records(
              format=self.format,
              usrQuery=self.query,
              count=count,
              firstRecord=i,
              createdTimeSpan=createdTimeSpan,
            )
            recs.extend(h_recs)
        # keep only valid ifs3 doctypes
        df = (pd.DataFrame(recs)
                          .query('ifs3_doctype != "unknown_doctype"')  # Filter out unknown_doctype
                          .reset_index(drop=True))
        return df

class ScopusHarvester(Harvester):
    """
    Scopus Harvester.
    """

    def __init__(self, start_date: str, end_date: str, query: str, format: str = "ifs3"):
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
        - `ifs3_doctype`: The IFS3 doctype of the publication.
        - `ifs3_collection_id`: The IFS3 collection ID of the publication.
        - `authors`: A list of authors, each represented as a dictionary containing `author`, `orcid_id`, `internal_author_id`, `organizations`, and `suborganization`.
        """
        # updated_query = f'({self.query}) AND (ORIG-LOAD-DATE AFT {self.start_date.strftime("%Y-%m-%d").replace("-","")}) AND (ORIG-LOAD-DATE BEF {self.end_date.strftime("%Y-%m-%d").replace("-","")})'
        updated_query = f'({self.query}) AND (ORIG-LOAD-DATE AFT {self.start_date.replace("-","")}) AND (ORIG-LOAD-DATE BEF {self.end_date.replace("-","")})'
        total = ScopusClient.count_results(
                query= updated_query
        )
        self.logger.info(f"- Nombre de publications trouvées dans Scopus: {total}")
        if total == "0": #scopus API returns 0 as string
            self.logger.debug("No publications found. Returning an empty DataFrame.")
            return pd.DataFrame()
        count = 50
        recs = []
        for i in range(1, int(total), int(count)):
            self.logger.info(
                f"Harvest publications {str(i)} to {str(int(i) + int(count))} on a total of {str(total)} publications"
            )
            h_recs = ScopusClient.fetch_records(
              format=self.format,
              query=updated_query,
              count=count,
              start=i
            )
            recs.extend(h_recs)
        # Keep only valid ifs3 doctypes
        df = (pd.DataFrame(recs)
                          .query('ifs3_doctype != "unknown_doctype"')  # Filter out unknown_doctype
                          .reset_index(drop=True))
        return df


class ZenodoHarvester(Harvester):
    """
    Zenodo Harvester.
    """

    def __init__(self, start_date: str, end_date: str,
                 query: str, format: str = "ifs3"):
        super().__init__("Scopus", start_date, end_date, query, format)

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
        - `ifs3_doctype`: IFS3 doctype of the object.
        - `ifs3_collection_id`: IFS3 collection ID of the object.
        - `authors`: list of creators, each represented as a dict containing:
           - `author`
           - `orcid_id`
           - `internal_author_id` (empty for Zenoodo)
           - `organizations`
           - `suborganization`
        """

        updated_query = ' AND '.join([self.query,
                                  f'created:[{self.start_date} TO {self.end_date}]'])

        total = ZenodoClient.count_results(q=updated_query)
        self.logger.info(f"- Nombre d'objets trouvées dans Zenodo: {total}")
        if total == "0":  # Zenodo API returns 0 as string
            self.logger.warning("No object found. Returning an empty DataFrame.")
            return pd.DataFrame()
        size = 50
        recs = []
        print(total)
        for i in range(0, 1+int(total)//size):
            self.logger.info(
                f"Harvest objects {i*size*(int(total)//size)+1} to {i*size*(int(total)//size)} out of a total of {total} objects"
            )
            h_recs = ZenodoClient.fetch_records(
              format=self.format,
              q=updated_query,
              size=size,
              page=i+1)
            # print(i, h_recs)
            recs.extend(h_recs)
        # Keep only valid ifs3 doctypes, filter out unknown_doctype
        # print(recs)
        df = (pd.DataFrame(recs)
                .query('ifs3_doctype != "unknown_doctype"')
                .reset_index(drop=True))
        return df

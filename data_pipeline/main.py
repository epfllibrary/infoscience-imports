from .harvester import WosHarvester, ScopusHarvester
from .deduplicator import DataFrameProcessor
from .enricher import AuthorProcessor, PublicationProcessor
from .loader import Loader
from config import default_queries
import os
import logging
# Configure logging to display messages in the notebook
logging.basicConfig(level=logging.INFO, format='%(message)s')
        
def main(start_date="2022-01-01", end_date="2024-01-01", queries=None):

     # Merge provided queries with default queries
    if queries:
        default_queries.update(queries)

    wos_harvester = WosHarvester(start_date, end_date, default_queries["wos"])
    scopus_harvester = ScopusHarvester(start_date, end_date, default_queries["scopus"])

    wos_publications = wos_harvester.harvest()
    scopus_publications = scopus_harvester.harvest()

    # Merge 
    deduplicator = DataFrameProcessor(wos_publications, scopus_publications)
    # Deduplicate the publications : first deduplicate operation between the sources
    deduplicated_sources_df = deduplicator.deduplicate_dataframes()
    # and second operation : filter by removing founded duplicates in Infoscience
    df_final,df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)
    # Generate main dataframes
    df_metadata, df_authors = deduplicator.generate_main_dataframes(df_final)
    # Generate EPFL authors enriched dataframe
    author_processor = AuthorProcessor(df_authors)
    df_epfl_authors = (author_nameprocessor
                        .process()
                        .filter_epfl_authors()
                        .clean_authors()
                        .nameparse_authors()
                        .api_epfl_reconciliation()
                        .generate_dspace_uuid(return_df=True)
                    )
    # Generate publications dataframe enriched with OA attributes
    publication_processor = PublicationProcessor(df_metadata)
    df_oa_metadata = (publication_nameprocessor
                        .process(return_df=True)
                    )
    return df_oa_metadata, df_authors, df_epfl_authors, df_unloaded
    # Create publications in Dspace
    #Loader.create_complete_publication(df_metadata)
    # Create or update person entities in Dspace
    #Loader.manage_person(df_epfl_authors)

if __name__ == "__main__":
    main()
from .harvester import WosHarvester, ScopusHarvester
from .deduplicator import DataFrameProcessor
import os
import logging
# Configure logging to display messages in the notebook
logging.basicConfig(level=logging.INFO, format='%(message)s')

def main(start_date="2022-01-01", end_date="2024-01-01", queries=None):
    
      # Set default queries if none provided
    default_queries = {
        "wos": "OG=(Ecole Polytechnique Federale de Lausanne)",
        "scopus": "AF-ID(60028186) OR AF-ID(60210159) OR AF-ID(60070536) OR AF-ID(60204330) OR AF-ID(60070531) OR AF-ID(60070534) OR AF-ID(60070538) OR AF-ID(60014951) OR AF-ID(60070529) OR AF-ID(60070532) OR AF-ID(60070535) OR AF-ID(60122563) OR AF-ID(60210160) OR AF-ID(60204331)",
        "openalex": "OPENALEX_QUERY_HERE",  # Placeholder for OpenAlex query
        "zenodo": "ZENODO_QUERY_HERE"      # Placeholder for Zenodo query
    }
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
    # returns the separated metadata and authors dataframe + the dataframe of unloaded duplicate publications
    return df_metadata, df_authors, df_unloaded

if __name__ == "__main__":
    main()
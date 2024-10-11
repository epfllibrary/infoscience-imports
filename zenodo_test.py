"""quick Zenodo harvester test"""

import os

from data_pipeline.harvester import ZenodoHarvester
from data_pipeline.deduplicator import DataFrameProcessor
from data_pipeline.enricher import AuthorProcessor, PublicationProcessor
from data_pipeline.loader import Loader

start_date = "2024-01-01"
end_date = "2024-10-31"
query = 'parent.communities.entries.id:"3c1383da-d7ab-4167-8f12-4d8aa0cc637f"'

h = ZenodoHarvester(start_date, end_date, query)
df = h.harvest()

deduplicator = DataFrameProcessor(df)
deduplicated_sources_df = deduplicator.deduplicate_dataframes()
# df_final, df_unloaded = deduplicator.deduplicate_infoscience(deduplicated_sources_df)
df_metadata, df_authors = deduplicator.generate_main_dataframes(df)
# Generate EPFL authors enriched dataframe
author_processor = AuthorProcessor(df_authors)
df_epfl_authors = (
    author_processor.process()
    .filter_epfl_authors()
    .clean_authors()
    .nameparse_authors()
    #.api_epfl_reconciliation()
    #.generate_dspace_uuid(return_df=True)
)



df_metadata.to_csv(os.path.join('.', "ResearchOutput.csv"),
		 		   index=False, encoding="utf-8")

df_authors.to_csv(os.path.join('.', "AddressesAndNames.csv"),
		 		   index=False, encoding="utf-8")

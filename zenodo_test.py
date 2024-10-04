from data_pipeline.harvester import ZenodoHarvester

start_date = "2024-01-01"
end_date = "2024-10-31"
query = 'parent.communities.entries.id:"3c1383da-d7ab-4167-8f12-4d8aa0cc637f"'

h = ZenodoHarvester(start_date, end_date, query)
df = h.harvest()
print(df)
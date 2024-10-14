from datetime import date

CURRENT_DATE = str(date.today())

# Default queries for harvest
default_queries = {
    "wos": "OG=(Ecole Polytechnique Federale de Lausanne)",
    "scopus": "AF-ID(60028186) OR AF-ID(60210159) OR AF-ID(60070536) OR AF-ID(60204330) OR AF-ID(60070531) OR AF-ID(60070534) OR AF-ID(60070538) OR AF-ID(60014951) OR AF-ID(60070529) OR AF-ID(60070532) OR AF-ID(60070535) OR AF-ID(60122563) OR AF-ID(60210160) OR AF-ID(60204331)",
    "openalex": "OPENALEX_QUERY_HERE",  # Placeholder for OpenAlex query
    "zenodo": "ZENODO_QUERY_HERE"      # Placeholder for Zenodo query
}

# Define the order of the sources during the deduplication process
source_order = ["scopus", "wos"]

# Define types of unit to retrieve from api.epfl.ch
unit_types = ["Laboratoire", "Groupe", "Chaire"]

# Scopus : EPFL labs internal IDs
scopus_epfl_afids = ["60028186","60210159","60070536","60204330","60070531",
                     "60070534","60070538","60014951","60070529","60070532",
                     "60070535","60122563","60210160","60204331"]

# License conditions for Unpaywall
LICENSE_CONDITIONS = {
    "allowed_licenses": ["cc-by", "public-domain"],
    "allowed_oa_statuses": ["gold", "hybrid", "green"]  # Added allowed OA statuses
}

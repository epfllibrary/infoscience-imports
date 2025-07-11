from datetime import date
import os

CURRENT_DATE = str(date.today())

base_dir = os.path.dirname(os.path.abspath(__file__))
logs_dir = os.path.join(base_dir, "logs")
os.makedirs(logs_dir, exist_ok=True)

# Default queries for harvest
default_queries = {
    "wos": "OG=(Ecole Polytechnique Federale de Lausanne)",
    "scopus": "AF-ID(60028186) OR AF-ID(60210159) OR AF-ID(60070536) OR AF-ID(60204330) OR AF-ID(60070531) OR AF-ID(60070534) OR AF-ID(60070538) OR AF-ID(60014951) OR AF-ID(60070529) OR AF-ID(60070532) OR AF-ID(60070535) OR AF-ID(60122563) OR AF-ID(60210160) OR AF-ID(60204331) OR AF-ID(126395205) OR AF-ID(128154174) OR AF-ID(121763255) OR AF-ID(126033832) OR AF-ID(127851587) OR AF-ID(126100075) OR AF-ID(126035869) OR AF-ID(126394243)",
    "openalex": "authorships.institutions.lineage:i5124864,is_retracted:false,is_paratext:false",
    "zenodo": 'parent.communities.entries.id:"3c1383da-d7ab-4167-8f12-4d8aa0cc637f"',
    "crossref": '{"query.affiliation": "EPFL"}',
}

# Define the order of the sources during the deduplication process
source_order = [
    "scopus",
    "wos",
    "openalex+crossref",
    "crossref",
    "openalex",
    "datacite",
    "zenodo",
]

# Define types of unit to retrieve in priority from api.epfl.ch
unit_types = [
    "Laboratoire",
    "Swiss Plasma Center",
    "Groupe",
    "Chaire",
    "Plateforme",
    # "Centre",
]

excluded_unit_types = [
    "Ecole",
    "Entreprises sur site",
    "Participation",
    "Formation continue",
    "Fondation",
    "Entités hôtes de l'EPFL",
    "Entité technique",
    "Decanat / Etat major",
    "Service central",
    "Divers",
]


# Scopus : EPFL labs internal IDs
scopus_epfl_afids = [
    "60028186",
    "60210159",
    "60070536",
    "60204330",
    "60070531",
    "60070534",
    "60070538",
    "60014951",
    "60070529",
    "60070532",
    "60070535",
    "60122563",
    "60210160",
    "60204331",
    "126395205",
    "128154174",
    "121763255",
    "126033832",
    "127851587",
    "126100075",
    "126035869",
    "126394243",
]

# License conditions for Unpaywall
LICENSE_CONDITIONS = {
    "allowed_licenses": ["cc-by", "public-domain"],
    "allowed_oa_statuses": ["gold", "hybrid", "green"],  # Added allowed OA statuses
}

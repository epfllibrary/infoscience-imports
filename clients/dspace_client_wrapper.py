from dspace.client import DSpaceClient
import logging
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

class DSpaceClientWrapper:
    def __init__(self):
        self.client = DSpaceClient()
        self.client.authenticate()

    def _search_objects(self, query, page=0, size=1, configuration="researchoutputs", dso_type=None):
        return self.client.search_objects(
            query=query,
            page=page,
            size=size,
            dso_type=dso_type,
            configuration=configuration,
        )
        
    def test(self,query):
        return self._search_objects(
                    query=query,
                    page=0,
                    size=1,
                    dso_type="item",
                    configuration="researchoutputs",
                )
        
        
    def find_publication_duplicate(self, x):
        identifier_type = x["source"]
        cleaned_title = clean_title(x["title"])
        pubyear = x["pubyear"]
        if isinstance(pubyear, str) and pubyear.isdigit():
            pubyear = int(pubyear)
        elif not isinstance(pubyear, int):
            raise ValueError("pubyear doit être numérique")
        previous_year = pubyear - 1
        next_year = pubyear + 1

        # Build queries for each matching rule
        if identifier_type == 'wos':
            item_id = str(x['internal_id']).replace("WOS:","").strip()
        elif identifier_type == 'scopus':
            item_id = str(x['internal_id']).replace("SCOPUS_ID:","").strip() 
        else:
            raise ValueError("identifier_type must be 'wos' or 'scopus'")

        query = f"(itemidentifier:{item_id})"
        title_query = f"(title:({cleaned_title}) AND (dateIssued:{pubyear} OR dateIssued:{previous_year} OR dateIssued:{next_year}))"
        doi_query = f"(itemidentifier:{str(x['doi']).strip()})" if "doi" in x else None

        # Check each identifier for duplicates
        for query in [query, title_query, doi_query]:
            if query is None:
                continue

            # Check the researchoutput configuration
            dsos_researchoutputs = self._search_objects(
                query=query,
                page=0,
                size=1,
                dso_type="item",
                configuration="researchoutputs",
            )
            num_items_researchoutputs = len(dsos_researchoutputs)
            ## supervision requests gives error, temporary disabled -> to see with JS
            # Check the supervision configuration
            #dsos_supervision = self._search_objects(
            #    query=query,
            #    page=0,
            #    size=1,
            #    configuration="supervision",
            #)
            #num_items_supervision = len(dsos_supervision)

            # Determine if the item is a duplicate in either configuration
            #is_duplicate = (num_items_researchoutputs > 0) or (
            #    num_items_supervision > 0
            #)

            is_duplicate = (num_items_researchoutputs > 0)

            if is_duplicate:
                logging.info(f"Publication searched with query:{query} founded in Infoscience.")
                return True  # Duplicate found

        logging.info(f"Publication searched with query:{query} not founded in Infoscience.")
        return False  # No duplicates found
        
def clean_title(title):
    title = re.sub(r"<[^>]+>", "", title)
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title   
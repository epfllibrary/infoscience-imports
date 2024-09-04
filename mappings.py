# Define the order of the sources during the deduplication process
source_order = ["scopus", "wos"]

doctypes_mapping_dict = {
    "source_wos": {
        "Article": "Journal articles",
        "Book Chapter": "Books and Book parts",
        "Book": "Books and Book parts",
        "Correction": "Journal articles",
        "Editorial Material":"Journal articles",
        "Letter": "Journal articles",
        "Meeting Abstract": "Conferences, Workshops, Symposiums, and Seminars",
        "Proceedings Paper": "Conferences, Workshops, Symposiums, and Seminars",
        "Review": "Journal articles"
    },
    "source_scopus": {
        "Article": "Journal articles",
        "Article in Press": "Journal articles",
        "Book": "Books and Book parts",
        "Chapter": "Books and Book parts",
        "Conference Paper": "Conferences, Workshops, Symposiums, and Seminars",
        "Conference Review": "Conferences, Workshops, Symposiums, and Seminars",
        "Data Paper": "Journal articles",  #?
        "Editorial": "Journal articles",
        "Letter": "Journal articles",
        "Review": "Journal articles",
        "Business Article": "Journal articles",
        #"Erratum": "",
        #"Note": "",        
        #"Short Survey": "",
        #"Press Release": "",
        #"Other": ""
    },
    "source_openalex": {
        "article": "Journal articles",
        "book": "Books and Book parts",
        "book-chapter": "Books and Book parts",
        "dataset": "Datasets and Code",
        "dissertation": "EPFL thesis",
        "editorial": "Journal articles",
        "letter": "Journal articles",
        "review": "Journal articles",
        "report": "Reports, Documentation, and Standards",
        "standard": "Reports, Documentation, and Standards",
        "preprint": "Preprints and Working Papers"
        #"erratum": "",
        #"grant": "",       
        #"other": "",
        #"paratext": "",
        #"peer-review": "",
        #"reference-entry": "",
        
    },
    "source_crossref": {
        
    }
}

collections_mapping = {'Patents': 'ce5a1b89-cfb3-40eb-bdd2-dcb021e755b7',
 'Projects': '49ec7e96-4645-4bc0-a015-ba4b81669bbc',
 'Teaching Materials': 'c7e018d4-2349-46dd-a8a4-c32cf5f5f9a1',
 'Images, Videos, Interactive resources, and Design': '329f8cd3-dc1a-4228-9557-b27366d71d41',
 'Newspaper, Magazine, or Blog post': '971cc7fa-b177-46e3-86a9-cfac93042e9d',
 'Funding': '8b185e36-0f99-4669-9a46-26a19d4f3eab',
 'Other': '0066acb2-d5c0-49a0-b273-581df34961cc',
 'Datasets and Code': '33a1cd32-7980-495b-a2bb-f34c478869d8',
 'Student works': '305e3dad-f918-48f6-9309-edbeb7cced14',
 'Units': 'bc85ee71-84b0-4f78-96a1-bab2c50b7ac9',
 'Contents': 'e8dea11e-a080-461b-82ee-6d9ab48404f3',
 'Virtual collections': '78f331d1-ee55-48ef-bddf-508488493c90',
 'EPFL thesis': '4af344ef-0fb2-4593-a234-78d57f3df621',
 'Reports, Documentation, and Standards': 'd5ec2987-2ee5-4754-971b-aca7ab4f9ab7',
 'Preprints and Working Papers': 'd8dada3a-c4bd-4c6f-a6d7-13f1b4564fa4',
 'Books and Book parts': '1a71fba2-2fc5-4c02-9447-f292e25ce6c1',
 'Persons': '6acf237a-90d7-43e2-82cf-c3591e50c719',
 'Events': '6e2af01f-8b92-461e-9d08-5e1961b9a97b',
 'Conferences, Workshops, Symposiums, and Seminars': 'e91ecd9f-56a2-4b2f-b7cc-f03e03d2643d',
 'Journals': '9ada82da-bb91-4414-a480-fae1a5c02d1c',
 'Journal articles': '8a8d3310-6535-4d3a-90b6-2a4428097b5b'}

def normalize_doctype_value(source, value):
    # Testing the function
    # print(normalize_doctype_value("source_A", "articles"))          # Output: Article
    # print(normalize_doctype_value("source_B", "RESEARCH_PAPER"))    # Output: Article
    # print(normalize_doctype_value("source_C", "article-journal"))    # Output: Article
    # print(normalize_doctype_value("source_D", "final_report"))       # Output: Report
    # print(normalize_doctype_value("source_A", "thesis"))  
    return mapping.get(source, {}).get(value, None)
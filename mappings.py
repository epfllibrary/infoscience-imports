"""Mappings of source plateform doctypes to Infoscience collections"""

doctypes_mapping_dict = {
    "source_wos": {
        "Article": "Journal articles",
        # "Book Chapter": "Books and Book parts",
        # "Book": "Books and Book parts",
        # "Correction": "Journal articles",
        "Editorial Material": "Journal articles",
        "Letter": "Journal articles",
        # "Meeting Abstract": "Conferences, Workshops, Symposiums, and Seminars",
        # "Proceedings Paper": "Conferences, Workshops, Symposiums, and Seminars",
        "Review": "Journal articles",
    },
    "source_scopus": {
        "Article": "Journal articles",
        "Article in Press": "Journal articles",
        # "Book": "Books and Book parts",
        # "Chapter": "Books and Book parts",
        # "Conference Paper": "Conferences, Workshops, Symposiums, and Seminars",
        # "Conference Review": "Conferences, Workshops, Symposiums, and Seminars",
        "Data Paper": "Journal articles",  # ?
        "Editorial": "Journal articles",
        "Letter": "Journal articles",
        "Review": "Journal articles",
        "Business Article": "Journal articles",
        # "Erratum": "",
        # "Note": "",
        # "Short Survey": "",
        # "Press Release": "",
        # "Other": ""
    },
    "source_openalex": {
        "article": "Journal articles",
        # "book": "Books and Book parts",
        # "book-chapter": "Books and Book parts",
        # "dataset": "Datasets and Code",
        # "dissertation": "EPFL thesis",
        # "editorial": "Journal articles",
        # "letter": "Journal articles",
        # "review": "Journal articles",
        # "report": "Reports, Documentation, and Standards",
        # "standard": "Reports, Documentation, and Standards",
        # "preprint": "Preprints and Working Papers"
        # "erratum": "",
        # "grant": "",
        # "other": "",
        # "paratext": "",
        # "peer-review": "",
        # "reference-entry": "",
    },
    "source_crossref": {},
    "source_zenodo": {
        "dataset": "Datasets and Code",
        # "lesson": "Teaching Materials",
        # "physicalobject": "Other",
        # "presentation": "Conferences, Workshops, Symposiums, and Seminars",
        # "poster": "Conferences, Workshops, Symposiums, and Seminars",
        # "publication/article": "Journal articles",
        # "publication/book": "Books and Book parts",
        # "publication/conferencepaper": "Conferences, Workshops, Symposiums, and Seminars",
        # "publication/deliverable": "Reports, Documentation, and Standards",
        # "publication/journal": "Journal articles",
        # "publication/report": "Reports, Documentation, and Standards",
        # "publication/section": "Books and Book parts",
        # "publication/thesis": "Student works",
        # "image/diagram": "Images, Videos, Interactive resources, and Design",
        # "image/drawing": "Images, Videos, Interactive resources, and Design",
        # "image/figure": "Images, Videos, Interactive resources, and Design",
        # "image/photo": "Images, Videos, Interactive resources, and Design",
        # "image/plot": "Images, Videos, Interactive resources, and Design",
        # "image/other": "Images, Videos, Interactive resources, and Design",
        "software": "Datasets and Code",
        # "video": "Images, Videos, Interactive resources, and Design",
        # "model": "Datasets and Code",
        # "other": "Datasets and Code",
    },
}

collections_mapping = {
    "Patents": {
        "id": "ce5a1b89-cfb3-40eb-bdd2-dcb021e755b7",
        "section": "patent",
    },
    "Projects": {
        "id": "49ec7e96-4645-4bc0-a015-ba4b81669bbc",
        "section": "project",
    },
    "Teaching Materials": {
        "id": "c7e018d4-2349-46dd-a8a4-c32cf5f5f9a1",
        "section": "teaching_details",
    },
    "Images, Videos, Interactive resources, and Design": {
        "id": "329f8cd3-dc1a-4228-9557-b27366d71d41",
        "section": "design_details",
    },
    "Newspaper, Magazine, or Blog post": {
        "id": "971cc7fa-b177-46e3-86a9-cfac93042e9d",
        "section": "media_details",
    },
    "Funding": {
        "id": "8b185e36-0f99-4669-9a46-26a19d4f3eab",
        "section": "funding",
    },
    "Other": {
        "id": "0066acb2-d5c0-49a0-b273-581df34961cc",
        "section": "other_details",
    },
    "Datasets and Code": {
        "id": "33a1cd32-7980-495b-a2bb-f34c478869d8",
        "section": "dataset_details",
    },
    "Student works": {
        "id": "305e3dad-f918-48f6-9309-edbeb7cced14",
        "section": "thesis_details",
    },
    "Units": {
        "id": "bc85ee71-84b0-4f78-96a1-bab2c50b7ac9",
        "section": "orgunit",
    },
    "Contents": {
        "id": "e8dea11e-a080-461b-82ee-6d9ab48404f3",
        "section": "content",
    },
    "Virtual collections": {
        "id": "78f331d1-ee55-48ef-bddf-508488493c90",
        "section": "virtual-collection",
    },
    "EPFL thesis": {
        "id": "4af344ef-0fb2-4593-a234-78d57f3df621",
        "section": "thesis_details",
    },
    "Reports, Documentation, and Standards": {
        "id": "d5ec2987-2ee5-4754-971b-aca7ab4f9ab7",
        "section": "report_details",
    },
    "Preprints and Working Papers": {
        "id": "d8dada3a-c4bd-4c6f-a6d7-13f1b4564fa4",
        "section": "preprint_details",
    },
    "Books and Book parts": {
        "id": "1a71fba2-2fc5-4c02-9447-f292e25ce6c1",
        "section": "book_details",
    },
    "Persons": {
        "id": "6acf237a-90d7-43e2-82cf-c3591e50c719",
        "section": "person",
    },
    "Events": {
        "id": "6e2af01f-8b92-461e-9d08-5e1961b9a97b",
        "section": "events_section",
    },
    "Conferences, Workshops, Symposiums, and Seminars": {
        "id": "e91ecd9f-56a2-4b2f-b7cc-f03e03d2643d",
        "section": "conference_details",
    },
    "Journals": {
        "id": "9ada82da-bb91-4414-a480-fae1a5c02d1c",
        "section": "journal",
    },
    "Journal articles": {
        "id": "8a8d3310-6535-4d3a-90b6-2a4428097b5b",
        "section": "article_details",
    },
}
# Mappings pour les licenses
licenses_mapping = {
    "cc-by": {
        "value": "CC BY",
        "display": "Creative Commons Attribution",
    },
    "cc-by-sa": {
        "value": "CC BY-SA",
        "display": "Creative Commons Attribution-ShareAlike",
    },
    "cc-by-nd": {
        "value": "CC BY-ND",
        "display": "Creative Commons Attribution-NoDerivatives",
    },
    "cc-by-nc": {
        "value": "CC BY-NC",
        "display": "Creative Commons Attribution-NoDerivs",
    },
    "cc-by-nc-sa": {
        "value": "CC BY-NC-SA",
        "display": "Creative Commons Attribution-NonCommercial-ShareAlike",
    },
    "cc-by-nc-nd": {
        "value": "CC BY-NC-ND",
        "display": "Creative Commons Attribution-NonCommercial-NoDerivs",
    },
    "public-domain": {
        "value": "PDM",
        "display": "Creative Commons Attribution-NonCommercial-NoDerivs",
    },
    "NA": {
        "value": "N/A",
        "display": "N/A (Copyrighted)",
    },
}

# Mappings pour les versions
versions_mapping = {
    "publishedVersion": {
        "value": "http://purl.org/coar/version/c_970fb48d4fbd8a85",
        "display": "Published version",
    },
    "acceptedVersion": {
        "value": "http://purl.org/coar/version/c_ab4af688f83e57aa",
        "display": "Accepted version",
    },
    "submittedVersion": {
        "value": "http://purl.org/coar/version/c_71e4c1898caa6e32",
        "display": "Submitted version",
    },
    "NA": {
        "value": "http://purl.org/coar/version/c_be7fb7dd8ff6fe43",
        "display": "Not Applicable (or Unknown)",
    },
}

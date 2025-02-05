"""Mappings of source plateform doctypes to Infoscience collections"""

doctypes_mapping_dict = {
    "source_wos": {
        "Article": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::research article",
        },
        "Proceedings Paper": {
            "collection": "Conferences, Workshops, Symposiums, and Seminars",
            "dc.type": "text::conference output::conference proceedings::conference paper",
        },
        # "Meeting Abstract": {
        #     "collection": "Conferences, Workshops, Symposiums, and Seminars",
        #     "dc.type": "text::conference output::conference proceedings::conference paper",
        # },
        "Review": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::review article",
        },
        "Editorial Material": {
            "collection": "Journal articles",
            "dc.type": "text::journal::editorial",
        },
        "Book Chapter": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph::book part or chapter",
        },
        "Book": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        # "Book Review": {
        #     "collection": "Books and Book parts",
        #     "dc.type": "text",
        # },
        "Early Access": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article",
        },
        # "Letter": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        # "Correction": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        # "Note": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        # "News Item": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        "Data Paper": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::data paper",
        },
    },
    "source_scopus": {
        "Article": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::research article",
        },
        "Article in Press": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::research article",
        },
        # "Book": {
        #     "collection": "Books and Book parts",
        #     "dc.type": "text::book/monograph",
        # },
        # "Book Chapter": {
        #     "collection": "Books and Book parts",
        #     "dc.type": "text::book/monograph::book part or chapter",
        # },
        # "Conference Paper": {
        #     "collection": "Conferences, Workshops, Symposiums, and Seminars",
        #     "dc.type": "text::conference output::conference proceedings::conference paper",
        # },
        # "Conference Review": {
        #     "collection": "Conferences, Workshops, Symposiums, and Seminars",
        #     "dc.type": "text::conference output::conference proceedings::conference paper",
        # },
        "Data Paper": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::data paper",
        },
        "Editorial": {
            "collection": "Journal articles",
            "dc.type": "text::journal::editorial",
        },
        # "Letter": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        "Review": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::review article",
        },
        # "Erratum": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        # "Business Article": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        # "Note": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "Short Survey": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "Press Release": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "Other": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
    },
    "source_openalex": {
        "article": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::research article",
        },
        "book": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        "book-chapter": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph::book part or chapter",
        },
        # "dataset": {
        #     "collection": "Datasets and Code",
        #     "dc.type": "dataset",
        # },
        # "dissertation": {
        #     "collection": "EPFL thesis",
        #     "dc.type": "text::thesis::doctoral thesis",
        # },
        "editorial": {
            "collection": "Journal articles",
            "dc.type": "text::journal::editorial",
        },
        "letter": {
            "collection": "Journal articles",
            "dc.type": "text",
        },
        "review": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::review article",
        },
        # "report": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::report",
        # },
        # "standard": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::technical documentation or standard",
        # },
        "preprint": {
            "collection": "Preprints and Working Papers",
            "dc.type": "text::preprint",
        },
        # "erratum": {
        #     "collection": "Journal articles",
        #     "dc.type": "text",
        # },
        # "grant": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "other": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "paratext": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "peer-review": {
        #     "collection": "",
        #     "dc.type": "",
        # },
        # "reference-entry": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
    },
    "source_crossref": {
        "book": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        "book-chapter": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph::book part or chapter",
        },
        "book-part": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph::book part or chapter",
        },
        "book-section": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph::book part or chapter",
        },
        "book-series": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph::book part or chapter",
        },
        "book-set": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        "book-track": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        # "component": {
        #     "collection": "Other",
        #     "dc.type": "text",
        # },
        "dataset": {
            "collection": "Datasets and Code",
            "dc.type": "dataset",
        },
        # "dissertation": {
        #     "collection": "EPFL thesis",
        #     "dc.type": "text::thesis::doctoral thesis",
        # },
        "edited-book": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        # "journal": {
        #     "collection": "Journal articles",
        #     "dc.type": "text::journal::journal article",
        # },
        "journal-article": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::research article",
        },
        # "journal-issue": {
        #     "collection": "Journal articles",
        #     "dc.type": "",
        # },
        # "journal-volume": {
        #     "collection": "Journal articles",
        #     "dc.type": "text::journal::journal article",
        # },
        "monograph": {
            "collection": "Books and Book parts",
            "dc.type": "text::book/monograph",
        },
        "peer-review": {
            "collection": "Journal articles",
            "dc.type": "text::journal::journal article::research article",
        },
        "posted-content": {
            "collection": "Preprints and Working Papers",
            "dc.type": "text::preprint",
        },
        "proceedings": {
            "collection": "Conferences, Workshops, Symposiums, and Seminars",
            "dc.type": "text::conference output::conference proceedings",
        },
        "proceedings-article": {
            "collection": "Conferences, Workshops, Symposiums, and Seminars",
            "dc.type": "text::conference output::conference proceedings::conference paper",
        },
        # "reference-book": {
        #     "collection": "Books and Book parts",
        #     "dc.type": "text::book/monograph",
        # },
        # "reference-entry": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "report": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::report",
        # },
        # "report-series": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::report",
        # },
        # "standard": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::technical documentation or standard",
        # },
        # "standard-series": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::technical documentation or standard",
        # },
        # "other": {
        #     "collection": "Other",
        #     "dc.type": "text",
        # },
    },
    "source_zenodo": {
        "dataset": {
            "collection": "Datasets and Code",
            "dc.type": "dataset",
        },
        # "lesson": {
        #     "collection": "Teaching Materials",
        #     "dc.type": "text",
        # },
        # "physicalobject": {
        #     "collection": "",
        #     "dc.type": "text",
        # },
        # "presentation": {
        #     "collection": "Conferences, Workshops, Symposiums, and Seminars",
        #     "dc.type": "text::conference output::conference presentation",
        # },
        # "poster": {
        #     "collection": "Conferences, Workshops, Symposiums, and Seminars",
        #     "dc.type": "text::conference output::conference proceedings",
        # },
        # "publication/article": {
        #     "collection": "Journal articles",
        #     "dc.type": "text::journal::journal article",
        # },
        # "publication/book": {
        #     "collection": "Books and Book parts",
        #     "dc.type": "text::book/monograph",
        # },
        # "publication/conferencepaper": {
        #     "collection": "Conferences, Workshops, Symposiums, and Seminars",
        #     "dc.type": "text::conference output::conference proceedings::conference paper",
        # },
        # "publication/deliverable": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::report",
        # },
        # "publication/journal": {
        #     "collection": "Journal articles",
        #     "dc.type": "text::journal::journal article",
        # },
        # "publication/report": {
        #     "collection": "Reports, Documentation, and Standards",
        #     "dc.type": "text::report",
        # },
        # "publication/section": {
        #     "collection": "Books and Book parts",
        #     "dc.type": "text::book/monograph::book part or chapter",
        # },
        # "publication/thesis": {
        #     "collection": "Student works",
        #     "dc.type": "text::thesis::doctoral thesis",
        # },
        # "image/diagram": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "image",
        # },
        # "image/drawing": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "image",
        # },
        # "image/figure": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "image",
        # },
        # "image/photo": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "image",
        # },
        # "image/plot": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "image",
        # },
        # "image/other": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "image",
        # },
        "software": {
            "collection": "Datasets and Code",
            "dc.type": "software",
        },
        # "video": {
        #     "collection": "Images, Videos, Interactive resources, and Design",
        #     "dc.type": "video",
        # },
        "model": {
            "collection": "Datasets and Code",
            "dc.type": "dataset",
        },
        # "other": {
        #     "collection": "",
        #     "dc.type": "",
        # },
    },
}

types_authority_mapping = {
    "text::journal::journal article::data paper": "article-coar-types:c_beb9",
    "text::journal::editorial": "article-coar-types:c_b239",
    "text::journal": "article-coar-types:c_0640",
    "text::journal::journal article": "article-coar-types:c_6501",
    "text::journal::journal article::research article": "article-coar-types:c_2df8fbb1",
    "text::journal::journal article::review article": "article-coar-types:c_dcae04bc",
    "text::journal::journal article::software paper": "article-coar-types:c_7bab",
    "text::book/monograph::book part or chapter": "book-coar-types:c_3248",
    "text::book/monograph": "book-coar-types:c_2f33",
    "text::conference output": "conference-coar-types:c_c94f",
    "text::conference output::conference proceedings::conference paper": "conference-coar-types:c_5794",
    "text::conference output::conference paper not in proceedings": "conference-coar-types:c_18cp",
    "text::conference output::conference proceedings::conference poster": "conference-coar-types:c_6670",
    "text::conference output::conference poster not in proceedings": "conference-coar-types:c_18co",
    "text::conference output::conference presentation": "conference-coar-types:R60J-J5BD",
    "text::conference output::conference proceedings": "conference-coar-types:c_f744",
    "dataset::aggregated data": "dataset-coar-types:ACF7-8YT9",
    "dataset::clinical trial data": "dataset-coar-types:c_cb28",
    "dataset::compiled data": "dataset-coar-types:FXF3-D3G7",
    "dataset": "dataset-coar-types:c_ddb1",
    "dataset::encoded data": "dataset-coar-types:AM6W-6QAW",
    "dataset::experimental data": "dataset-coar-types:63NG-B465",
    "dataset::genomic data": "dataset-coar-types:A8F1-NPV9",
    "dataset::geospatial data": "dataset-coar-types:2H0M-X761",
    "dataset::laboratory notebook": "dataset-coar-types:H41Y-FW7B",
    "dataset::measurement and test data": "dataset-coar-types:DD58-GFSX",
    "dataset::observational data": "dataset-coar-types:FF4C-28RK",
    "dataset::recorded data": "dataset-coar-types:CQMR-7K63",
    "software::research software": "dataset-coar-types:c_c950",
    "dataset::simulation data": "dataset-coar-types:W2XT-7017",
    "software": "dataset-coar-types:c_5ce6",
    "software::source code": "dataset-coar-types:QH80-2R4E",
    "dataset::survey data": "dataset-coar-types:NHD0-W6SY",
    "design": "design-coar-types:542X-3S04",
    "image": "design-coar-types:c_c513",
    "design::industrial design": "design-coar-types:JBNF-DYAD",
    "interactive resource": "design-coar-types:c_e9a0",
    "design::layout design": "design-coar-types:BW7T-YM2G",
    "image::moving image": "design-coar-types:c_8a7e",
    "image::still image": "design-coar-types:c_ecc8",
    "image::moving image::video": "design-coar-types:c_12ce",
    "interactive resource::website": "design-coar-types:c_7ad9",
    "text::blog post": "media-coar-types:c_6947",
    "text::magazine": "media-coar-types:c_2cd9",
    "text::newspaper article": "media-coar-types:c_998f",
    "patent::PCT application": "patent-coar-types:SB3Y-W4EH",
    "patent::design patent": "patent-coar-types:C53B-JCY5",
    "patent": "patent-coar-types:c_15cd",
    "patent::plant patent": "patent-coar-types:Z907-YMBB",
    "patent::plant variety protection": "patent-coar-types:GPQ7-G5VE",
    "patent::software patent": "patent-coar-types:MW8G-3CR8",
    "patent::utility model": "patent-coar-types:9DKX-KSAF",
    "text::preprint": "preprint-coar-types:c_816b",
    "text::working paper": "preprint-coar-types:c_8042",
    "text::report::clinical study": "report-coar-types:c_7877",
    "text::report::data management plan": "report-coar-types:c_ab20",
    "text::report::policy report": "report-coar-types:c_186u",
    "text::report": "report-coar-types:c_93fc",
    "text::report::research protocol": "report-coar-types:YZ1N-ZFT9",
    "text::report::research report": "report-coar-types:c_18ws",
    "text::technical documentation or standard": "report-coar-types:c_71bd",
    "text::report::technical report": "report-coar-types:c_18gh",
    "teaching material": "teaching-coar-types:c_e059",
    "thesis": "thesis-coar-types:c_46ec",
    "thesis::doctoral thesis": "thesis-coar-types:c_db06",
    "student work::bachelor thesis": "student-coar-types:c_7a1f",
    "student work::master thesis": "student-coar-types:c_bdcc",
    "student work::semester or other student projects": "student-coar-types:c_18op",
    "student work": "student-coar-types:c_46ec",
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
        "section": "teaching_",
    },
    "Images, Videos, Interactive resources, and Design": {
        "id": "329f8cd3-dc1a-4228-9557-b27366d71d41",
        "section": "design_",
    },
    "Newspaper, Magazine, or Blog post": {
        "id": "971cc7fa-b177-46e3-86a9-cfac93042e9d",
        "section": "media_",
    },
    "Funding": {
        "id": "8b185e36-0f99-4669-9a46-26a19d4f3eab",
        "section": "funding",
    },
    "Other": {
        "id": "0066acb2-d5c0-49a0-b273-581df34961cc",
        "section": "other_",
    },
    "Datasets and Code": {
        "id": "33a1cd32-7980-495b-a2bb-f34c478869d8",
        "section": "dataset_",
    },
    "Student works": {
        "id": "305e3dad-f918-48f6-9309-edbeb7cced14",
        "section": "thesis_",
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
        "section": "thesis_",
    },
    "Reports, Documentation, and Standards": {
        "id": "d5ec2987-2ee5-4754-971b-aca7ab4f9ab7",
        "section": "report_",
    },
    "Preprints and Working Papers": {
        "id": "d8dada3a-c4bd-4c6f-a6d7-13f1b4564fa4",
        "section": "preprint_",
    },
    "Books and Book parts": {
        "id": "1a71fba2-2fc5-4c02-9447-f292e25ce6c1",
        "section": "book_",
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
        "section": "conference_",
    },
    "Journals": {
        "id": "9ada82da-bb91-4414-a480-fae1a5c02d1c",
        "section": "journal",
    },
    "Journal articles": {
        "id": "8a8d3310-6535-4d3a-90b6-2a4428097b5b",
        "section": "article_",
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

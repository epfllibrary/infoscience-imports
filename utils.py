import pandas as pd
import requests
import json
from collections import defaultdict
from datetime import date
import ast
import logging
import os
from csv import DictWriter
import time
import config
import re


folder_path = "harvested-data"
current_date = str(config.CURRENT_DATE).replace("-", "_")
path = os.path.join(folder_path,current_date)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0',
    'ACCEPT' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'ACCEPT-ENCODING' : 'gzip, deflate, br',
    'ACCEPT-LANGUAGE' : 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'REFERER' : 'https://www.google.com/'
}

def deduplicate_dataframes(df_wos, df_scopus):
    # Combine the two dataframes into one
    combined_df = pd.concat([df_wos, df_scopus], ignore_index=True)
    
    # Create a unique identifier for rows where 'doi' is missing
    combined_df['unique_id'] = combined_df.apply(
        lambda row: row['doi'] if pd.notna(row['doi']) else row['title'] + str(row['pubyear']),
        axis=1
    )
    
    # Sort the combined dataframe to prioritize 'scopus' source in case of duplicates
    combined_df.sort_values(by=['unique_id', 'source'], ascending=[True, False], inplace=True)
    
    # Drop duplicates based on the 'unique_id' column, keeping the first occurrence (which will be 'scopus' if duplicate exists)
    deduplicated_df = combined_df.drop_duplicates(subset='unique_id', keep='first')
    
    # Drop the helper column 'unique_id' as it's no longer needed
    deduplicated_df.drop(columns=['unique_id'], inplace=True)
    
    return deduplicated_df

def generate_main_dataframes(df):
    # Step 1: Add an incremental row_id to the DataFrame
    df['row_id'] = range(1, len(df) + 1)
    new_rows = []

    # Iterate through each row in the DataFrame
    for _, row in df.iterrows():
        row_id = row['row_id']  # Extract the row_id
        source = row['source']  # Extract the source
        authors = row['authors']  # Extract the authors list

        # Iterate through each dictionary in the authors list
        for author_data in authors:
            # Create a new row with the row_id and author data
            new_row = {
                'row_id': row_id,
                'source': source,
                'author': author_data.get('author', None),
                'orcid_id': author_data.get('orcid_id', None),
                'internal_author_id': author_data.get('internal_author_id', None),
                'organizations': author_data.get('organizations', None),
                'suborganization': author_data.get('suborganization', None)
            }
            # Append the new row to the list of new rows
            new_rows.append(new_row)

    # Convert the list of new rows into a new DataFrame
    df_authors = pd.DataFrame(new_rows)
    
    # Step 2: Split the DataFrame into two parts
    # Non-author columns dataframe
    df_metadata= df.drop(columns=['authors'])
    
    return df_metadata, df_authors

def get_doctype_mapping():
    return {
    "Article": {
        '037': 'ARTICLE',
        '336': 'Journal Articles',
        '980': 'ARTICLE'
    },
    "Book Chapter": {
        '037': 'BOOK_CHAP',
        '336': 'Book Chapters',
        '980': 'BOOK_CHAP'
    },
    "Book": {
        '037': 'BOOK',
        '336': 'Books',
        '980': 'BOOK'
    },
    "Correction": {
        '037': 'ARTICLE',
        '336': 'Journal Articles',
        '980': 'ARTICLE'
    },
    "Editorial Material": {
        '037': 'ARTICLE',
        '336': 'Journal Articles',
        '980': 'ARTICLE'
    },
    "Letter": {
        '037': 'ARTICLE',
        '336': 'Journal Articles',
        '980': 'ARTICLE'
    },
    "Meeting Abstract": {
        '037': 'CONF',
        '336': 'Conference Papers',
        '980': 'CONF'
    },
    "Proceedings Paper": {
        '037': 'CONF',
        '336': 'Conference Papers',
        '980': 'CONF'
    },
    "Review": {
        '037': 'ARTICLE',
        '336': 'Reviews',
        '980': 'REVIEW'
    }
}


def populate_controle_file(workflow_path, type, label, source, data):
    field_names = ["authority_type", "label", "source_metadata", "harvested_metadata"]
    harvest_metadata = {
        "authority_type": type,
        "label": label,
        "source_metadata": source,
        "harvested_metadata": data,
    }
    with open(
        os.path.join(
            workflow_path, "controle_epfl_authors_and_labs_metadata.csv"
        ),
        "a",
        encoding="utf-8",
        errors="ignore",
    ) as f_object:
        dictwriter_object = DictWriter(f_object, fieldnames=field_names)
        dictwriter_object.writerow(harvest_metadata)


def process_searchapi_lab(acro):
    try:
        url = f"https://search-api.epfl.ch/api/unit/?acro={acro}"
        response = requests.request("GET", url, headers=headers)
        resp = response.text
        data = json.loads(resp)
    except Exception as e:
        # print(acro, e)
        return None

    if not data:
        return None

    result = {}
    result["acro"] = acro

    if "code" in data:
        uid = f'U{str(data["code"])}'
    else:
        uid = ""
    result["uid"] = uid
    result["recid"] = ""

    manager = ""
    if "head" in data and data["head"] and "email" in data["head"]:
        manager = data["head"]["email"]

    result["manager"] = manager
    result["liaison"] = ""

    return result


def get_author_infos_from_searchapi(name):
    arr_name = name.strip().lower().split(", ")
    if len(arr_name) > 1:
        # query_name = f"{arr_name[0]}+{arr_name[1][0]}" # matching with the firstname first letter not optimal (risk of false positives)
        query_name = f"{arr_name[0]}+{arr_name[1]}"
    else:
        query_name = f"{arr_name[0]}"
    try:
        url = f"https://search-api.epfl.ch/api/ldap/?q={query_name}"
        resp = requests.request("GET", url, headers=headers).text
    except Exception as e:
        # print(name,e)
        pass
    # on s'emmerde pas avec les faux positifs et on ne garde que le résultat sûr
    if len(json.loads(resp)) == 1:
        result = {}
        result["name"] = name
        result["sciper_id"] = str(json.loads(resp)[0]["sciper"])
        result["recid"] = ""
        # labs_complete = [{"acronym": x["acronym"], "code": x["code"]} for x in json.loads(resp)[0]["accreds"]]
        result["labs"] = [
            x["acronym"] for x in json.loads(resp)[0]["accreds"] if x["rank"] == 0
        ]
        return result
    else:
        return None

def get_lab_infos_from_searchapi(acro):
    try:
        url = f"https://search-api.epfl.ch/api/unit/?q={acro}"
        resp = requests.request("GET", url, headers=headers).text
    except Exception as e:
        print(e)
        # return None
        pass
    if type(json.loads(resp)) is dict:
        return [process_searchapi_lab(acro)]
    else:
        return [process_searchapi_lab(x["acronym"]) for x in json.loads(resp)]


def enrich_lab(workflow_path, acro):
    searchapi_try_lab = get_lab_infos_from_searchapi(acro)
    if searchapi_try_lab is not None:
        populate_controle_file(
            workflow_path, "labo", acro, "search-api", searchapi_try_lab
        )
        return searchapi_try_lab
    else:
        # populate_controle_file(workflow_path,"labo",acro,"","")
        return None


def enrich_author(workflow_path, name):
    time.sleep(2)
    result = {}
    labs = []
    searchapi_try_auth = get_author_infos_from_searchapi(name)
    # in  first place : request infoscience authors authorities
    if (
        (searchapi_try_auth is not None)
        & (searchapi_try_auth != (None,))
        & (searchapi_try_auth != None)
    ):
        result["author"] = searchapi_try_auth
        populate_controle_file(
            workflow_path, "author", name, "search-api", searchapi_try_auth
        )
        for x in searchapi_try_auth["labs"]:
            labs.extend(enrich_lab(workflow_path, x))
        result["lab"] = labs
        return result
    else:
        populate_controle_file(workflow_path, "author", name, "", "")
        return None


def get_units_for_id(workflow_path, wos_id):
    df_addresses = pd.read_csv(
        os.path.join(workflow_path, "AddressesAndNames.csv"), sep=",", encoding="utf-8"
    )
    # Filtrer les lignes correspondant au wos_id donné
    df_filtered = df_addresses[df_addresses["wos_id"] == wos_id]

    # Initialiser une liste pour stocker les informations des labs
    labs_info = []

    for _, row in df_filtered.iterrows():
        # Vérifier si le champ 'author_infos' existe et n'est pas vide
        author_infos = row.get("author_infos", None)

        # Convertir en chaîne vide si 'author_infos' est NaN
        if pd.isna(author_infos):
            author_infos = ""

        author_infos = str(author_infos).strip()

        if author_infos:
            try:
                # Extraire les informations 'labs' du champ 'author_infos'
                author_infos_dict = json.loads(author_infos.replace("'", '"'))
                labs = author_infos_dict.get("lab", [])

                # Dédupliquer les labs
                labs_info.extend(labs)
            except json.JSONDecodeError:
                print(
                    f"Erreur de décodage JSON pour wos_id {wos_id} avec author_infos: {author_infos}"
                )

    # Dédupliquer les éléments dans labs_info
    unique_labs_info = {json.dumps(lab, sort_keys=True) for lab in labs_info}
    unique_labs_info = [json.loads(lab) for lab in unique_labs_info]

    return unique_labs_info


def clean_title(title):
    title = re.sub(r"<[^>]+>", "", title)
    title = re.sub(r"[^\w\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title

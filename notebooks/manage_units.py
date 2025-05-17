import marimo

__generated_with = "0.12.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    import pandas as pd
    import marimo as mo
    sys.path.append(os.path.abspath(".."))
    return mo, os, pd, sys


@app.cell
def _():
    from dspace.dspace_rest_client.client import DSpaceClient
    from clients.api_epfl_client import ApiEpflClient
    from mappings import MAPPING_UNITS_CODES, MAPPING_UNITS_EN, MAPPING_UNITS_FR
    return (
        ApiEpflClient,
        DSpaceClient,
        MAPPING_UNITS_CODES,
        MAPPING_UNITS_EN,
        MAPPING_UNITS_FR,
    )


@app.cell
def _(DSpaceClient):
    d = DSpaceClient()
    authenticated = d.authenticate()
    if not authenticated:
        print('Error logging in! Giving up.')
        exit(1)
    return authenticated, d


@app.cell
def get_collections():
    def get_collections(d):
        """
        Retrieve all collections from top-level communities in a DSpace instance.

        Parameters:
        d (object): A DSpace API client instance that provides methods such as 
                    `get_communities(top=True)` and `get_collections(community=...)`.

        Returns:
        list of dict: A list of dictionaries, each containing the 'name' and 'uuid'
                      of a collection under the top-level communities.
        """
        result = []
        # Get all top-level communities
        top_communities = d.get_communities(top=True)

        # For each community, get its collections
        for top_community in top_communities:
            collections = d.get_collections(community=top_community)
            for collection in collections:
                result.append({
                    'name': collection.name,
                    'uuid': collection.uuid
                })
        return result
    return (get_collections,)


@app.cell
def get_uuid_by_name():
    def get_uuid_by_name(collections, name):
        """
        Find the UUID of a collection given its name.

        Parameters:
        collections (list of dict): A list of collections, where each collection is a 
                                    dictionary with 'name' and 'uuid' keys.
        name (str): The name of the collection to search for.

        Returns:
        str or None: The UUID of the collection if found; otherwise, None.
        """
        for collection in collections:
            if collection.get('name') == name:
                return collection.get('uuid')
        return None
    return (get_uuid_by_name,)


@app.cell
def find_duplicate_units():
    def find_duplicate_units(df, key='unitid', status_col='status'):
        """
        Identify and sort candidate duplicate organizational units.

        Units are considered duplicates if they share the same `unitid`. Within each group
        of duplicates, rows are sorted so that units with status == 'true' appear first.

        Parameters:
        df (pd.DataFrame): The DataFrame containing unit data.
        key (str): The column to check for duplicates (default: 'unitid').
        status_col (str): The column indicating status (default: 'status').

        Returns:
        pd.DataFrame: A DataFrame with duplicated units, grouped and sorted.
        """
        df_valid = df[df[key].notna()]

        # Find duplicated rows based on the key
        duplicates = df_valid[df_valid.duplicated(subset=[key], keep=False)]

        # Sort by key (e.g. unitid), and prioritize status == 'true'
        duplicates_sorted = duplicates.sort_values(
            by=[key, status_col],
            ascending=[True, False]  # status 'true' comes first (alphabetically, "true" > "false")
        )

        return duplicates_sorted
    return (find_duplicate_units,)


@app.cell
def _(pd):
    def find_closed_with_open_match(df, key='unitid', status_col='status'):
        """
        Find closed units (status == 'false') that have a corresponding open unit
        (status == 'true') with the same key, and enrich them with open unit metadata.

        Parameters:
        df (pd.DataFrame): DataFrame containing unit data.
        key (str): Column used to group units (default: 'unitid').
        status_col (str): Column indicating unit status (default: 'status').

        Returns:
        pd.DataFrame: A DataFrame of closed units with matching open unit metadata added.
        """
        # Filter rows with non-null key and status
        df_clean = df[df[key].notna() & df[status_col].notna()]

        # Group by the key (e.g., 'unitid')
        grouped = df_clean.groupby(key)

        enriched_closed_units = []

        for unitid, group in grouped:
            open_units = group[group[status_col] == 'true']
            closed_units = group[group[status_col] == 'false']

            # If both open and closed units exist for this unitid
            if not open_units.empty and not closed_units.empty:
                # Use the first open unit as the reference (you can adapt this logic)
                open_ref = open_units.iloc[0]

                # Enrich each closed unit with open unit metadata
                closed_copy = closed_units.copy()
                closed_copy['open_acronym'] = open_ref.get('acronym')
                closed_copy['open_unitid'] = open_ref.get('unitid')
                closed_copy['open_parent'] = open_ref.get('parent')
                closed_copy['open_url'] = open_ref.get('url')
                closed_copy['open_uuid'] = open_ref.get('uuid')


                enriched_closed_units.append(closed_copy)

        # Combine all enriched closed units
        if enriched_closed_units:
            return pd.concat(enriched_closed_units, ignore_index=True)
        else:
            return pd.DataFrame(columns=df.columns.tolist() + ['open_acronym', 'open_unitid', 'open_parent', 'open_url', 'open_uuid'])
    return (find_closed_with_open_match,)


@app.cell
def _(pd):
    def get_outputs_by_unit(d, unit_uuid, size=100):
        """
        Fetch and structure research outputs (publications) linked to a specific organizational unit.

        Parameters:
        d (object): DSpace API client object with a `search_objects` method.
        unit_uuid (str): The UUID of the unit/authority to search publications for.
        size (int): Number of results to fetch (default: 100).

        Returns:
        pd.DataFrame: A DataFrame containing structured metadata for each publication.
        """
        # Build the search query
        query = f"unitOrLab_authority:({unit_uuid})"
        configuration = "researchoutputs"
        sort_order = "dc.date.accessioned,DESC"

        # Execute the search
        routputs = d.search_objects(
            query=query,
            page=0,
            size=size,
            sort=sort_order,
            dso_type="item",
            configuration=configuration
        )

        # Parse the search results into structured records
        researchoutput = []

        for r in routputs:
            researchoutput.append({
                'uuid': r.uuid,
                'title': r.metadata.get('dc.title', [{}])[0].get('value'),
                'type': r.metadata.get('dc.type', [{}])[0].get('value'),
                'authors': set(x.get('value') for x in r.metadata.get('dc.contributor.author', [{}]) if x.get('value')),
                'scipers': set(x.get('value') for x in r.metadata.get('cris.virtual.sciperId', [{}]) if x.get('value')),
                'journal': r.metadata.get('dc.relation.journal', [{}])[0].get('value'),
                'issued': r.metadata.get('dc.date.issued', [{}])[0].get('value'),
                'created': r.metadata.get('dc.date.created', [{}])[0].get('value'),
                'units': set(x.get('value') for x in r.metadata.get('dc.description.sponsorship', [{}]) if x.get('value')),
                'peerreviewed': r.metadata.get('epfl.peerreviewed', [{}])[0].get('value'),
                'epfl': r.metadata.get('epfl.writtenAt', [{}])[0].get('value'),
            })

        # Convert to DataFrame
        return pd.DataFrame(researchoutput)
    return (get_outputs_by_unit,)


@app.cell
def _(pd):
    def get_items_from_entity(d, size=100, configuration="orgunt", sort="dc.date.accessioned,DESC", page=0):
        """
        Retrieve organizational units from a DSpace instance using the 'orgunit' configuration.

        Parameters:
        d (object): DSpace API client instance with a `search_objects` method.
        size (int): Number of results to retrieve per page (default: 100).
        sort (str): Sorting criteria for results (default: 'dc.date.accessioned,DESC').
        page (int): Page number to fetch (default: 0).

        Returns:
        pd.DataFrame: DataFrame containing unit metadata for each organizational unit.
        """
        query = "*:*"
        dsos = d.search_objects(
            query=query,
            page=page,
            size=size,
            sort=sort,
            dso_type="item",
            configuration=configuration
        )

        units = []
        for dso in dsos:
            md = dso.metadata
            units.append({
                'uuid': dso.uuid,
                'name': md.get('dc.title', [{}])[0].get('value'),
                'acronym': md.get('oairecerif.acronym', [{}])[0].get('value'),
                'unitid': md.get('epfl.unit.code', [{}])[0].get('value'),
                'type': md.get('dc.type', [{}])[0].get('value'),
                'status': md.get('epfl.orgUnit.active', [{}])[0].get('value'),
                'level': md.get('epfl.orgUnit.level', [{}])[0].get('value'),
                'url': md.get('oairecerif.identifier.url', [{}])[0].get('value'),
                'parent': md.get('organization.parentOrganization', [{}])[0].get('value'),
            })

        return pd.DataFrame(units)
    return (get_items_from_entity,)


@app.cell
def _(d, get_items_from_entity):
    df_units = get_items_from_entity(d, configuration="orgunit")
    df_units
    return (df_units,)


@app.cell
def _(pd):
    def enrich_units(df_units, api_client):
        """
        Enrich df_units row-by-row based on the acronym using EPFL API data.
        Ensures strict row-to-row alignment (no length mismatch).
        """
        # Copy df to avoid modifying original
        df_units = df_units.copy()

        # Prepare empty columns
        df_units['unittype_id'] = None
        df_units['unittype_label'] = None
        df_units['cf'] = None
        df_units['url'] = None
        df_units['responsible_id'] = None

        for idx, row in df_units.iterrows():
            acronym = row.get('acronym')

            if not acronym or pd.isna(acronym):
                continue  # leave values as None

            try:
                unit = api_client.fetch_unit_by_unique_id(acronym, format="epfl")

                if not unit or not isinstance(unit, dict):
                    continue

                df_units.at[idx, 'unittype_id'] = unit.get('unittype', {}).get('id')
                df_units.at[idx, 'unittype_label'] = unit.get('unittype', {}).get('label')
                df_units.at[idx, 'cf'] = unit.get('cf')
                df_units.at[idx, 'url'] = unit.get('url')
                df_units.at[idx, 'responsible_id'] = unit.get('responsible', {}).get('id')

            except Exception as e:
                # On error, just keep the row with None (already default)
                continue

        return df_units
    return (enrich_units,)


@app.cell
def _(ApiEpflClient, df_units, enrich_units):
    df_units_enriched = enrich_units(df_units, ApiEpflClient)
    df_units_enriched
    return (df_units_enriched,)


@app.cell
def map_unit_types_to_dspace():
    def map_unit_types_to_dspace(df_units, mapping_codes, mapping_en, mapping_fr):
        """
        Add columns to df_units by mapping 'unittype_label' to:
        - a code (dspace_type_code),
        - a label in English (dspace_type_en),
        - a label in French (dspace_type_fr).

        Parameters:
        df_units (pd.DataFrame): DataFrame with a column 'unittype_label'.
        mapping_codes (dict): Mapping from label to DSpace code.
        mapping_en (dict): Mapping from label to English label.
        mapping_fr (dict): Mapping from label to French label.

        Returns:
        pd.DataFrame: The enriched DataFrame with 3 new columns.
        """
        df_units = df_units.copy()
        label_lower = df_units['unittype_label'].str.lower()

        # Map values using get() to avoid KeyErrors for unknown labels

        dspace_type_code = label_lower.map(mapping_codes.get)
        df_units['dspace_type_code'] = dspace_type_code
        df_units['dspace_type_en'] = dspace_type_code.map(mapping_en.get)
        df_units['dspace_type_fr'] = dspace_type_code.map(mapping_fr.get)

        return df_units
    return (map_unit_types_to_dspace,)


@app.cell
def _(
    MAPPING_UNITS_CODES,
    MAPPING_UNITS_EN,
    MAPPING_UNITS_FR,
    df_units_enriched,
    map_unit_types_to_dspace,
):
    df_units_final = map_unit_types_to_dspace(
        df_units_enriched,
        mapping_codes=MAPPING_UNITS_CODES,
        mapping_en=MAPPING_UNITS_EN,
        mapping_fr=MAPPING_UNITS_FR
    )
    return (df_units_final,)


@app.cell
def _(df_units_final):
    df_units_final
    return


@app.cell
def _(df_units_final, mo):
    transformed_df = mo.ui.dataframe(df_units_final)
    transformed_df
    return (transformed_df,)


@app.cell
def activate_units_with_cf():
    def activate_units_with_cf(df_units):
        """
        Set status to 'true' for rows where cf is not null/empty and status is 'false'.

        Parameters:
        df_units (pd.DataFrame): Must contain 'cf' and 'status' columns.

        Returns:
        pd.DataFrame: Updated DataFrame with modified status.
        """
        df_units = df_units.copy()

        # Condition: cf is not null/empty and status is 'false'
        condition = (
            df_units['cf'].astype(str).str.strip().ne('') & 
            df_units['cf'].notna() &
            df_units['status'].astype(str).str.lower().eq('false')
        )

        df_units.loc[condition, 'status'] = 'true'
        return df_units
    return (activate_units_with_cf,)


@app.cell
def _(activate_units_with_cf, df_units_final):
    df_units_refined = activate_units_with_cf(df_units_final)
    df_units_refined
    return (df_units_refined,)


@app.cell
def update_unit():
    def update_unit(d, item_uuid, dspace_type_code=None, url=None, cf=None):
        """
        Update the dc.type, oairecerif.identifier.url and epfl.orgUnit.cf metadata
        of an organizational unit via PATCH operation using d.api_patch.
        Applies inference rules on type if not provided.

        Parameters:
        d (object): DSpace API client.
        item_uuid (str): UUID of the unit item.
        dspace_type_code (str or None): Type code to assign (e.g. 'CENTRE').
        url (str or None): URL to assign.
        cf (str or int, optional): Code FNS (cf) to assign.

        Returns:
        list of str: List of patch log messages.
        """
        def is_valid(val):
            return val is not None and str(val).strip().lower() not in ["", "null"]

        results = []
        item = d.get_item(item_uuid)
        item_url = item.links["self"]["href"]
        metadata = item.metadata

        # --- RULE: infer type if needed ---
        type_none = str(dspace_type_code).strip().lower()
        if type_none in ["none"]:
            unit_name = metadata.get('dc.title', [{}])[0].get('value', '')
            unit_name_stripped = unit_name.strip()
            unit_name_lower = unit_name_stripped.lower()

            keywords = ["laboratoire", "laboratory", "unité du prof."]
            starts_with_prof = unit_name_stripped.startswith("Prof.")

            if any(kw in unit_name_lower for kw in keywords) or starts_with_prof:
                dspace_type_code = "LABO"
                results.append("Inferred type 'LABO' from unit name")

        # --- dc.type ---
        if is_valid(dspace_type_code):
            existing_type = metadata.get('dc.type', [])
            value = {"value": dspace_type_code.strip(), "language": None, "authority": None}

            path = "/metadata/dc.type/0" if existing_type else "/metadata/dc.type/-"
            op = "replace" if existing_type else "add"
            value_payload = value if op == "replace" else [value]

            try:
                response = d.api_patch(item_url, op, path, value_payload)
                results.append(f"{op.capitalize()}d dc.type '{dspace_type_code}': {response.status_code}")
            except Exception as e:
                results.append(f"Error updating dc.type: {e}")

        # --- URL ---
        if is_valid(url):
            existing_url = metadata.get('oairecerif.identifier.url', [])
            value = {"value": url.strip(), "language": None, "authority": None}

            path = "/metadata/oairecerif.identifier.url/0" if existing_url else "/metadata/oairecerif.identifier.url/-"
            op = "replace" if existing_url else "add"
            value_payload = value if op == "replace" else [value]

            try:
                response = d.api_patch(item_url, op, path, value_payload)
                results.append(f"{op.capitalize()}d URL '{url}': {response.status_code}")
            except Exception as e:
                results.append(f"Error updating URL: {e}")

        # --- CF ---
        if is_valid(cf):
            existing_cf = metadata.get('epfl.orgUnit.cf', [])
            value = {
                "value": str(cf).strip(),
                "language": None,
                "authority": None,
                "securityLevel": 0
            }

            path = "/metadata/epfl.orgUnit.cf/0" if existing_cf else "/metadata/epfl.orgUnit.cf/-"
            op = "replace" if existing_cf else "add"
            value_payload = value if op == "replace" else [value]

            try:
                response = d.api_patch(item_url, op, path, value_payload)
                results.append(f"{op.capitalize()}d CF '{cf}': {response.status_code}")
            except Exception as e:
                results.append(f"Error updating CF: {e}")

            # --- Status (epfl.orgUnit.active → true) ---
        try:
            value = {"value": "true", "language": None, "authority": None}
            response = d.api_patch(item_url, "replace", "/metadata/epfl.orgUnit.active/0", value)
            results.append(f"Replaced status with 'true': {response.status_code}")
        except Exception as e:
            results.append(f"Error updating status to true: {e}")

        if not results:
            results.append("No patch operation performed (no valid type, URL, or CF).")

        return results
    return (update_unit,)


@app.cell
def _(pd, update_unit):
    def batch_update_units(d, df_units):
        """
        Batch update units in DSpace based on df_units content.
        Only applies to rows where status is 'true' (string or bool).
        Uses the update_unit() function per row.

        Parameters:
        d (object): DSpace API client.
        df_units (pd.DataFrame): Must contain 'uuid', 'status', 'dspace_type_code', 'url', 'cf', and 'acronym'.

        Returns:
        pd.DataFrame: Report of patch actions per unit.
        """
        report_rows = []

        # Robust filtering of "true" values
        df_valid = df_units[df_units['status'].astype(str).str.lower() == "true"]

        for _, row in df_valid.iterrows():
            uuid = row.get('uuid')
            acronym = row.get('acronym')
            type_code = row.get('dspace_type_code')
            url = row.get('url')
            cf = row.get('cf')

            try:
                patch_log = update_unit(d, uuid, dspace_type_code=type_code, url=url, cf=cf)
                report_rows.append({
                    'uuid': uuid,
                    'acronym': acronym,
                    'status': 'success',
                    'patch_actions': " || ".join(patch_log)
                })
            except Exception as e:
                report_rows.append({
                    'uuid': uuid,
                    'acronym': acronym,
                    'status': 'error',
                    'patch_actions': str(e)
                })

        return pd.DataFrame(report_rows)
    return (batch_update_units,)


@app.cell
def _(batch_update_units, d, df_units_refined):
    df_up_units_report = batch_update_units(d, df_units_refined)
    df_up_units_report
    return (df_up_units_report,)


@app.cell
def patch_sponsorship_unit():
    def patch_sponsorship_unit(d, item_uuid, obsolete_unit, active_unit):
        """
        Patch 'dc.description.sponsorship' of one item to replace an obsolete unit with an active one.
        Returns patch log and sponsorship acronyms before and after the operation.
        """
        responses = []
        item = d.get_item(item_uuid)
        item_url = item.links["self"]["href"]
        md = item.metadata

        sponsorships = md.get('dc.description.sponsorship', [])
        obsolete_found = []
        active_present = False

        # Record pre-patch values
        sponsorships_before = [x.get('value') for x in sponsorships if x.get('value')]

        last_response = None

        for i, entry in enumerate(sponsorships):
            val = entry.get('value')
            auth = entry.get('authority')

            if val == obsolete_unit['acronym'] and auth == obsolete_unit['uuid']:
                obsolete_found.append(i)
            if val == active_unit['acronym'] and auth == active_unit['uuid']:
                active_present = True

        if active_present and obsolete_found:
            for idx in obsolete_found:
                path = f"/metadata/dc.description.sponsorship/{idx}"
                response = d.api_patch(item_url, "remove", path, None)
                last_response = response
                responses.append(f"Removed obsolete unit at index {idx}: {response.status_code}")

        elif not active_present and obsolete_found:
            first_idx = obsolete_found[0]
            path = f"/metadata/dc.description.sponsorship/{first_idx}"
            new_value = {
                "value": active_unit['acronym'],
                "language": None,
                "authority": active_unit['uuid']
            }
            response = d.api_patch(item_url, "replace", path, new_value)
            last_response = response
            responses.append(f"Replaced obsolete with active at index {first_idx}: {response.status_code}")

            for idx in obsolete_found[1:]:
                path = f"/metadata/dc.description.sponsorship/{idx}"
                response = d.api_patch(item_url, "remove", path, None)
                last_response = response
                responses.append(f"Removed duplicate obsolete unit at index {idx}: {response.status_code}")

        else:
            responses.append("No action taken (no obsolete unit found or already clean)")
            last_response = None

        # Extract post-patch values from last response (if any patch was applied)
        if last_response and last_response.status_code == 200:
            metadata_after = last_response.json().get("metadata", {})
            sponsorships_after = [x.get('value') for x in metadata_after.get("dc.description.sponsorship", []) if x.get('value')]
        else:
            sponsorships_after = sponsorships_before  # unchanged

        return responses, sponsorships_before, sponsorships_after
    return (patch_sponsorship_unit,)


@app.cell
def _(patch_sponsorship_unit, pd):
    def batch_replace_unit(d, obsolete_unit, active_unit, size=50, max_pages=None):
        """
        Batch-replace obsolete units in all items by calling patch_sponsorship_unit on each.
        Returns a detailed DataFrame with patch actions and before/after sponsorships.
        """
        query = f"unitOrLab_authority:({obsolete_unit['uuid']})"
        config = "researchoutputs"
        sort = "dc.date.accessioned,DESC"

        items = d.search_objects(query=query, page=0, size=size, sort=sort, dso_type="item", configuration=config, max_pages=max_pages)

        report_rows = []

        for item in items:
            item_uuid = item.uuid
            item_title = item.metadata.get('dc.title', [{}])[0].get('value')

            try:
                patch_log, before_vals, after_vals = patch_sponsorship_unit(d, item_uuid, obsolete_unit, active_unit)
                report_rows.append({
                    'item_uuid': item_uuid,
                    'title': item_title,
                    'status': 'success',
                    'patch_actions': " || ".join(patch_log),
                    'sponsorships_before': " || ".join(before_vals),
                    'sponsorships_after': " || ".join(after_vals)
                })
            except Exception as e:
                report_rows.append({
                    'item_uuid': item_uuid,
                    'title': item_title,
                    'status': 'error',
                    'patch_actions': str(e),
                    'sponsorships_before': '',
                    'sponsorships_after': ''
                })

        return pd.DataFrame(report_rows)
    return (batch_replace_unit,)


@app.cell
def _(df_units_refined, find_closed_with_open_match):
    df_closed_duplicates = find_closed_with_open_match(df_units_refined)
    df_closed_duplicates
    return (df_closed_duplicates,)


@app.cell
def _(batch_replace_unit, d):
    obsolete_unit = {"acronym": "UPLARUS", "uuid": "68963f33-a82e-4971-909f-9ab4d24b5d60"}
    active_unit = {"acronym": "VLSC", "uuid": "f4a705b5-ef31-4598-8e4b-b1eabc8d3b69"}

    df_replace_unit_report = batch_replace_unit(d, obsolete_unit, active_unit, size=100, max_pages=15)

    # Affichage ou export du rapport
    df_replace_unit_report
    return active_unit, df_replace_unit_report, obsolete_unit


if __name__ == "__main__":
    app.run()

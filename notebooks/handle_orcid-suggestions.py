import marimo

__generated_with = "0.11.20"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import os
    sys.path.append(os.path.abspath(".."))
    return os, sys


@app.cell
def _():
    from dspace.dspace_rest_client.client import DSpaceClient
    return (DSpaceClient,)


@app.cell
def _(DSpaceClient):
    d = DSpaceClient()
    return (d,)


@app.cell
def _():
    import pandas as pd

    def process_orcid_suggestions(orcid_suggestions):
        """
        Process ORCID suggestions and store them in a DataFrame.

        :param orcid_suggestions: JSON response from the get_orcid_suggestions method.
        :return: DataFrame containing the processed ORCID suggestions.
        """
        # Extract relevant data from the JSON response
        data = []
        for suggestion in orcid_suggestions:
            data.append({
                'id': suggestion['id'],
                'displayName': suggestion['display'],
                'source': suggestion['source'],
                'totalSuggestions': suggestion['total'],
            })

        # Create a DataFrame from the extracted data
        df = pd.DataFrame(data)
        return df
    return pd, process_orcid_suggestions


@app.cell
def _(d, process_orcid_suggestions):
    # Fetch ORCID suggestions
    orcid_suggestions = d.get_external_suggestions(page=0, size=50)

    # Process the suggestions into a DataFrame
    df_orcid_suggestions = process_orcid_suggestions(orcid_suggestions)

    # Display or further process the DataFrame
    if not df_orcid_suggestions.empty:
        print("ORCID Suggestions DataFrame created")
    else:
        print("No ORCID suggestions were found.")

    df_orcid_suggestions
    return df_orcid_suggestions, orcid_suggestions


@app.cell
def _(d, df_orcid_suggestions, pd):
    # Aggregating all suggestions
    all_suggestions = []
    for index, row in df_orcid_suggestions.iterrows():
        target_id = row['id']
        display_name = row['displayName']
        source = row['source']


        # Split target_id into source and ID
        try:
            source, id = target_id.split(":")
        except ValueError as e:
            print(f"Error splitting target_id: {e}")
            continue  # Skip this iteration if splitting fails

        # Retrieve suggestions for the current target
        suggestions_response = d.get_suggestions_by_target(target=id, page=0, size=50, source=source)
        if suggestions_response and "suggestions" in suggestions_response:
            suggestions = suggestions_response["suggestions"]

            # Add target reference to each suggestion
            for suggestion in suggestions:
                if isinstance(suggestion, dict):  # Ensure suggestion is a dictionary
                    all_suggestions.append({
                        "internal_id": suggestion.get("id"),
                        "title": suggestion.get("dc.title", [None])[0] if suggestion.get("dc.title") else None,
                        "date_issued": suggestion.get("dc.date.issued", [None])[0] if suggestion.get("dc.date.issued") else None,
                        "pubyear": suggestion.get("pubyear"),
                        "target_id": target_id,
                        "target_user": display_name,
                        "source": source
                    })
                else:
                    print(f"Unexpected suggestion format: {suggestion}")
        else:
            print(f"No suggestions found for target {target_id}")

    # Create a DataFrame for all aggregated suggestions
    df_all_suggestions = pd.DataFrame(all_suggestions)

    # Display the resulting DataFrame
    df_all_suggestions
    return (
        all_suggestions,
        df_all_suggestions,
        display_name,
        id,
        index,
        row,
        source,
        suggestion,
        suggestions,
        suggestions_response,
        target_id,
    )


@app.cell
def _(df_all_suggestions):
    from data_pipeline.deduplicator import DataFrameProcessor

    deduplicator = DataFrameProcessor(df_all_suggestions)
    df_final,df_unloaded = deduplicator.deduplicate_infoscience(df_all_suggestions)
    return DataFrameProcessor, deduplicator, df_final, df_unloaded


@app.cell
def _(df_final):
    df_final
    return


@app.cell
def _(df_unloaded):
    df_unloaded
    return


if __name__ == "__main__":
    app.run()

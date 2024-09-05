import pandas_gbq
from datetime import datetime

PROJECT_ID = "jeremy-chia"


def write_df_to_gbq(df, dataset_id, table_id, if_exists="replace"):
    """
    Write a DataFrame to Google BigQuery, using the variable name to determine the table ID.

    :param df: The DataFrame to write
    :param dataset_id: BigQuery dataset ID
    :param table_id: BigQuery table ID
    :param if_exists: What to do if the table already exists (options: 'replace', 'append', 'fail')
    """
    # Construct the full table ID
    table_full_id = f"{dataset_id}.{table_id}"

    # Add timestamp
    df["_accessed_at_utc"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z%z")

    # Write the DataFrame to BigQuery
    pandas_gbq.to_gbq(
        df, destination_table=table_full_id, project_id=PROJECT_ID, if_exists=if_exists
    )

    print(
        f"DataFrame written to BigQuery table: {table_full_id} in project: {PROJECT_ID}"
    )

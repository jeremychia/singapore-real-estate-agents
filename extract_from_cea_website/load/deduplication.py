import pandas_gbq
import pandas as pd
from typing import Union, List
import load

project_id = "jeremy-chia"


def _read_data(transaction_type: str) -> pd.DataFrame:
    """
    Read data from Google BigQuery for a given transaction type.

    Parameters:
    transaction_type (str): The transaction type to query from the database.

    Returns:
    pd.DataFrame: The resulting DataFrame sorted by '_accessed_at_utc'.
    """
    df = pandas_gbq.read_gbq(
        query_or_table=f"estate_agents.{transaction_type}", project_id=project_id
    )

    # sort by _accessed_at_utc (when data was downloaded) as dedup is by keeping first value
    df.sort_values(by="_accessed_at_utc", inplace=True)

    return df


def _validate_primary_key(
    df: pd.DataFrame, primary_key: Union[str, List[str]], secondary_key: str = None
) -> bool:
    """
    Validate the one-to-one mapping of a secondary key to the primary key in the DataFrame.
    Skips primary key uniqueness validation since deduplication is assumed.

    Parameters:
    df (pd.DataFrame): The DataFrame to validate.
    primary_key (Union[str, List[str]]): The column name (string) or list of column names representing the primary key.
    secondary_key (str, optional): The column name to validate for one-to-one mapping with the primary key.

    Raises:
    KeyError: If any column in the primary or secondary key does not exist in the DataFrame.
    ValueError: If the secondary key is not one-to-one with the primary key.

    Returns:
    bool: True if the validation passes.
    """

    # check if primary_key is a string or a list
    if isinstance(primary_key, str):
        primary_key = [primary_key]  # convert to list for consistency

    # check if all columns in primary_key exist in the DataFrame
    missing_columns = [col for col in primary_key if col not in df.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in DataFrame: {', '.join(missing_columns)}")

    # check one-to-one mapping if secondary_key is provided
    if secondary_key:
        if secondary_key not in df.columns:
            raise KeyError(f"Missing column in DataFrame: {secondary_key}")

        # group by primary_key and check the number of unique secondary keys
        grouped = df.groupby(primary_key)[secondary_key].nunique()
        if (grouped > 1).any():
            raise ValueError(
                f"There are multiple '{secondary_key}' values for the same primary key '{primary_key}'."
            )

    return True


def _deduplicate(df: pd.DataFrame, primary_key: Union[str, List[str]]) -> pd.DataFrame:
    """
    Deduplicate a DataFrame based on the primary key, keeping the first occurrence.
    Validates the one-to-one mapping of 'registrationNumber' to the primary key.

    Parameters:
    df (pd.DataFrame): The DataFrame to deduplicate.
    primary_key (Union[str, List[str]]): The column name (string) or list of column names representing the primary key.

    Raises:
    ValueError: If the primary key does not result in unique rows after validation.

    Returns:
    pd.DataFrame: A deduplicated DataFrame.
    """

    if _validate_primary_key(df, primary_key, secondary_key="registrationNumber"):
        # keep first because values are sorted by _accessed_at_utc (when data was downloaded)
        deduplicated = df.drop_duplicates(
            subset=primary_key, keep="first", inplace=False, ignore_index=True
        )

    else:
        raise ValueError(f"Primary key '{primary_key}' is not unique.")

    return deduplicated


def deduplicate_data(
    primary_key,
    transaction_types=["hdb_rental", "hdb_resale", "private_rental", "private_sale"],
):
    for transaction_type in transaction_types:
        df = _read_data(transaction_type)
        print(f"Pre-deduplicated data for {transaction_type} has: {len(df)} rows")

        deduplicated_df = _deduplicate(df, primary_key)
        print(
            f"Post-deduplicated data for {transaction_type} has: {len(deduplicated_df)} rows"
        )

        pandas_gbq.to_gbq(
            dataframe=deduplicated_df,
            destionation_table=f"estate_agents.{transaction_type}",
            project_id=project_id,
            if_exists="replace",
        )

    return 0


def _deduplicate_agents(df, primary_key):
    deduplicated_df = df.drop_duplicates(
        subset=primary_key, keep="last", inplace=False, ignore_index=True
    )

    return deduplicated_df


def deduplicate_agents():
    df = _read_data("agents")
    print(f"Pre-deduplicated data for agents has: {len(df)} rows")

    deduplicated_df = _deduplicate_agents(
        df, primary_key=["registrationNumber", "licenseNumber"]
    )
    print(f"Post-deduplicated data for agents has: {len(deduplicated_df)} rows")

    pandas_gbq.to_gbq(
        dataframe=deduplicated_df,
        destination_table="estate_agents.agents",
        project_id=project_id,
        if_exists="replace",
    )

    return 0

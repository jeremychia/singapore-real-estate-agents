import pandas_gbq
import load

project_id = "jeremy-chia"


def _read_data(transaction_type):
    df = pandas_gbq.read_gbq(
        query_or_table=f"estate_agents.{transaction_type}", project_id=project_id
    )

    # sort by _accessed_at_utc (when data was downloaded) as dedup is by keeping first value
    df.sort_values(by="_accessed_at_utc", inplace=True)

    return df


def _validate_primary_key(df, primary_key, secondary_key=None):
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


def _deduplicate(df, primary_key):
    if _validate_primary_key(df, primary_key, secondary_key="registrationNumber"):
        # keep first because values are sorted by _accessed_at_utc (when data was downloaded)
        deduplicated = df.drop_duplicates(
            subset=primary_key, keep="first", inplace=False, ignore_index=True
        )

    else:
        raise ValueError(f"Primary key '{primary_key}' is not unique.")

    return deduplicated


def _write_data(df, transaction_type):
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f"estate_agents.{transaction_type}",
        project_id=project_id,
        if_exists="replace",
    )

    return 0


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

        load.write_df_to_gbq(deduplicated_df,  "estate_agents", transaction_type, if_exists="replace")

    return 0

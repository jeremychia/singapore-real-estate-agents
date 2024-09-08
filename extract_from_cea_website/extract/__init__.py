import requests
import pandas as pd
from datetime import datetime, timedelta

import load


def iteratively_retrieve_data(url, headers, base_payload, page_size=100):
    # Start from page 1 and retrieve data in chunks of the specified page_size
    page = 1
    all_results = []

    while True:
        # Update the base payload with pagination information
        payload = (
            base_payload.copy()
        )  # Copy to avoid mutating the original base_payload
        payload.update({"page": page, "pageSize": page_size})

        print(f"Requesting page: {page}, pageSize: {page_size}")

        # Make the request
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        # Append the results from this page to the main list
        all_results.extend(
            data.get("data", [])
        )  # Adjust 'data' based on the API's response format

        # Check if there are more pages to request
        if len(data.get("data", [])) < page_size:
            # If the number of results is less than the page size, we've retrieved all the data
            break

        # Move to the next page
        page += 1

    print(f"Total results: {len(all_results)}")

    return all_results


def retrieve_property_data(url, headers, txn_payload, registration_number):
    """
    Helper function to retrieve property transaction data for a specific registration number and URL.
    """
    # Update the txn_payload with the current registration number
    txn_payload["registrationNumber"] = registration_number

    # Retrieve data using the iteratively_retrieve_data function
    data = iteratively_retrieve_data(url, headers, txn_payload)

    return data


def retrieve_all_data_for_registration_numbers(
    registration_numbers, headers, batch_size=100
):
    """
    Function to retrieve property data for multiple registration numbers across multiple transaction types.

    :param registration_numbers: List of registration numbers to iterate over
    :param headers: Headers for the requests
    :return: Dictionary of DataFrames for each transaction type
    """
    # URLs for the different transaction types
    urls = {
        "hdb_resale": "https://www.cea.gov.sg/aceas/api/internet/property-txn/v1/public-register/hdb-resale/filter",
        "hdb_rental": "https://www.cea.gov.sg/aceas/api/internet/property-txn/v1/public-register/hdb-rental/filter",
        "private_rental": "https://www.cea.gov.sg/aceas/api/internet/property-txn/v1/public-register/private-rental/filter",
        "private_sale": "https://www.cea.gov.sg/aceas/api/internet/property-txn/v1/public-register/private-sale/filter",
    }

    # Base payload for transactions
    txn_payload = {"sortAscFlag": False, "sort": "transactionDate"}

    # Time Log
    average_time = timedelta(seconds=0)

    # Store results in a dictionary of DataFrames
    all_data = {key: [] for key in urls}  # Store data as lists

    count_registration_numbers = len(registration_numbers)
    # Iterate over each registration number and each transaction type
    for idx, registration_number in enumerate(registration_numbers):
        start_time = datetime.now()
        print(
            f"Progress: {registration_number} ({idx+1} out of {count_registration_numbers})"
        )
        for key, url in urls.items():
            print(
                f"Retrieving data for {key} and registration number {registration_number}"
            )
            # Retrieve data for this registration number and transaction type
            data = retrieve_property_data(
                url, headers, txn_payload, registration_number
            )
            # Append to the corresponding DataFrame in the dictionary
            for record in data:
                record["registrationNumber"] = registration_number
                all_data[key].append(record)

        # Compute end-time
        end_time = datetime.now()
        time_diff = end_time - start_time
        average_time = (average_time * idx + time_diff) / (idx + 1)
        remaining_time = average_time * (count_registration_numbers - idx - 1)
        completion_time = (end_time + remaining_time).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"Complete: {idx+1} out of {count_registration_numbers} (Average time: {round(time_diff.total_seconds(), 1)}). Estimated completion: {completion_time}"
        )

        if (idx + 1) % batch_size == 0:
            hdb_resale_df = pd.DataFrame(all_data["hdb_resale"])
            load.write_df_to_gbq(
                hdb_resale_df, "estate_agents", "hdb_resale", if_exists="append"
            )

            hdb_rental_df = pd.DataFrame(all_data["hdb_rental"])
            load.write_df_to_gbq(
                hdb_rental_df, "estate_agents", "hdb_rental", if_exists="append"
            )

            private_rental_df = pd.DataFrame(all_data["private_rental"])
            load.write_df_to_gbq(
                private_rental_df, "estate_agents", "private_rental", if_exists="append"
            )

            private_sale_df = pd.DataFrame(all_data["private_sale"])
            load.write_df_to_gbq(
                private_sale_df, "estate_agents", "private_sale", if_exists="append"
            )

            all_data = {key: [] for key in urls}  # Store data as lists

    # After loop, check if there's still remaining data in all_data and write it to GBQ
    if any(all_data[key] for key in all_data):
        print("Writing remaining data to GBQ...")

        if all_data["hdb_resale"]:
            hdb_resale_df = pd.DataFrame(all_data["hdb_resale"])
            load.write_df_to_gbq(
                hdb_resale_df, "estate_agents", "hdb_resale", if_exists="append"
            )

        if all_data["hdb_rental"]:
            hdb_rental_df = pd.DataFrame(all_data["hdb_rental"])
            load.write_df_to_gbq(
                hdb_rental_df, "estate_agents", "hdb_rental", if_exists="append"
            )

        if all_data["private_rental"]:
            private_rental_df = pd.DataFrame(all_data["private_rental"])
            load.write_df_to_gbq(
                private_rental_df, "estate_agents", "private_rental", if_exists="append"
            )

        if all_data["private_sale"]:
            private_sale_df = pd.DataFrame(all_data["private_sale"])
            load.write_df_to_gbq(
                private_sale_df, "estate_agents", "private_sale", if_exists="append"
            )

    return 0

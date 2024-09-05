import requests
import pandas as pd


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

    return pd.DataFrame(all_results)


def retrieve_property_data(url, headers, txn_payload, registration_number):
    """
    Helper function to retrieve property transaction data for a specific registration number and URL.
    """
    # Update the txn_payload with the current registration number
    txn_payload["registrationNumber"] = registration_number

    # Retrieve data using the iteratively_retrieve_data function
    df = iteratively_retrieve_data(url, headers, txn_payload)

    # Add the registration number to the DataFrame
    df["registrationNumber"] = registration_number

    return df


def retrieve_all_data_for_registration_numbers(registration_numbers, headers):
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

    # Store results in a dictionary of DataFrames
    all_data = {key: pd.DataFrame() for key in urls}

    # Iterate over each registration number and each transaction type
    for registration_number in registration_numbers:
        for key, url in urls.items():
            print(
                f"Retrieving data for {key} and registration number {registration_number}"
            )
            # Retrieve data for this registration number and transaction type
            df = retrieve_property_data(url, headers, txn_payload, registration_number)
            # Append to the corresponding DataFrame in the dictionary
            all_data[key] = pd.concat([all_data[key], df], ignore_index=True)

    return all_data

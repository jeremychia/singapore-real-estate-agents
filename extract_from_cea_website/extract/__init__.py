import requests
import pandas as pd
from datetime import datetime, timedelta
import sys
import logging

import load


import time
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def countdown_sleep(seconds, message_prefix=""):
    """Sleep with a countdown timer displayed in the terminal."""
    for remaining in range(seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        timer = f"{mins:02d}:{secs:02d}"
        sys.stdout.write(f"\r{message_prefix}Resuming in {timer}...")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 60 + "\r")  # Clear the line
    sys.stdout.flush()


def iteratively_retrieve_data(url, headers, base_payload, page_size=100):
    """
    Retrieves data from a paginated API, handling potential JSON decoding errors and 429 errors.
    Implements exponential backoff for rate limiting.
    """
    page = 1
    all_results = []
    backoff_seconds = 60  # Reduced initial backoff (was 128)
    max_backoff_seconds = 300  # Cap at 5 minutes

    while True:
        payload = base_payload.copy()  # Avoid mutating the original base_payload
        payload.update({"page": page, "pageSize": page_size})
        logger.info(f"Requesting page {page} (pageSize: {page_size})")

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            if not response.text:
                logger.warning(f"Empty response for page {page}, skipping")
                page += 1
                continue

            data = response.json()
            results_count = len(data.get("data", []))
            all_results.extend(data.get("data", []))
            logger.debug(f"Page {page}: retrieved {results_count} records")

            if results_count < page_size:
                break

            page += 1
            backoff_seconds = 60  # Reset backoff on success

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning(f"Rate limited (429) on page {page}")
                countdown_sleep(backoff_seconds, f"⏳ Rate limited on page {page}. ")
                backoff_seconds = min(backoff_seconds * 2, max_backoff_seconds)
                continue
            else:
                logger.error(f"HTTP error on page {page}: {e}")
                page += 1
                continue

        except requests.exceptions.JSONDecodeError as e:
            logger.error(f"JSON decode error on page {page}: {e}")
            page += 1
            continue

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error on page {page}: {e}")
            page += 1
            continue

    logger.info(f"✓ Retrieved {len(all_results):,} total records")
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
    CEA_BASE_URL = "https://eservices.cea.gov.sg"
    urls = {
        "hdb_resale": f"{CEA_BASE_URL}/aceas/api/internet/property-txn/v1/public-register/hdb-resale/filter",
        "hdb_rental": f"{CEA_BASE_URL}/aceas/api/internet/property-txn/v1/public-register/hdb-rental/filter",
        "private_rental": f"{CEA_BASE_URL}/aceas/api/internet/property-txn/v1/public-register/private-rental/filter",
        "private_sale": f"{CEA_BASE_URL}/aceas/api/internet/property-txn/v1/public-register/private-sale/filter",
    }

    # Base payload for transactions
    txn_payload = {"sortAscFlag": False, "sort": "transactionDate"}

    # Time Log
    average_time = timedelta(seconds=0)

    # Store results in a dictionary of DataFrames
    all_data = {key: [] for key in urls}  # Store data as lists

    count_registration_numbers = len(registration_numbers)
    logger.info(f"Starting extraction for {count_registration_numbers:,} registration numbers")
    
    # Iterate over each registration number and each transaction type
    try:
        for idx, registration_number in enumerate(registration_numbers):
            start_time = datetime.now()
            progress_pct = ((idx + 1) / count_registration_numbers) * 100
            logger.info(
                f"[{idx+1}/{count_registration_numbers}] ({progress_pct:.1f}%) Processing {registration_number}"
            )
            for key, url in urls.items():
                logger.debug(f"  → Fetching {key}")
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
            logger.info(
                f"  ✓ Completed in {time_diff.total_seconds():.1f}s | "
                f"Avg: {average_time.total_seconds():.1f}s | ETA: {completion_time}"
            )

            if (idx + 1) % batch_size == 0:
                try:
                    logger.info(f"📤 Writing batch ({batch_size} records) to BigQuery...")
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
                        private_rental_df,
                        "estate_agents",
                        "private_rental",
                        if_exists="append",
                    )

                    private_sale_df = pd.DataFrame(all_data["private_sale"])
                    load.write_df_to_gbq(
                        private_sale_df,
                        "estate_agents",
                        "private_sale",
                        if_exists="append",
                    )

                    all_data = {key: [] for key in urls}  # Store data as lists
                    logger.info("✓ Batch written successfully")
                except Exception as e:
                    logger.error(f"Error writing batch to BigQuery: {e}")

    except Exception as e:
        logger.error(f"Critical error during processing: {e}")

    # After loop, check if there's still remaining data in all_data and write it to GBQ
    finally:
        if any(all_data[key] for key in all_data):
            logger.info("📤 Writing remaining data to BigQuery...")

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
                    private_rental_df,
                    "estate_agents",
                    "private_rental",
                    if_exists="append",
                )

            if all_data["private_sale"]:
                private_sale_df = pd.DataFrame(all_data["private_sale"])
                load.write_df_to_gbq(
                    private_sale_df, "estate_agents", "private_sale", if_exists="append"
                )
            
            logger.info("✓ All data written to BigQuery")

    return 0

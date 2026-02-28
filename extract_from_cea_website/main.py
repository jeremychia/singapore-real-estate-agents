import argparse
import pandas_gbq
import pandas as pd
import requests
from curl_cffi import requests as cffi_requests

# Assuming you have these modules defined elsewhere
import extract
import load
import load.deduplication as deduplication
from load import get_credentials, TOKEN_PATH

PROJECT_ID = "singapore-real-estate-agents"

CEA_BASE_URL = "https://eservices.cea.gov.sg"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": CEA_BASE_URL,
    "Referer": f"{CEA_BASE_URL}/aceas/public-register/sales/1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="145", "Not:A-Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

DIRECTORY_URL = f"{CEA_BASE_URL}/aceas/api/internet/profile/v2/public-register/filter"
VOWELS = ["a", "e", "i", "o", "u"]


def preflight():
    """Run preflight checks to verify all dependencies and connections."""
    print("Running preflight checks...")
    checks_passed = True

    # 1. Check GCP credentials file exists
    print(f"  [1/3] Checking GCP credentials file... ", end="")
    if TOKEN_PATH.exists():
        print("✓")
    else:
        print(f"✗ (not found at {TOKEN_PATH})")
        checks_passed = False

    # 2. Test BigQuery connection
    print("  [2/3] Testing BigQuery connection... ", end="")
    try:
        credentials = get_credentials()
        pandas_gbq.read_gbq(
            "SELECT 1",
            project_id=PROJECT_ID,
            credentials=credentials,
        )
        print("✓")
    except Exception as e:
        print(f"✗ ({e})")
        checks_passed = False

    # 3. Test CEA API reachability
    print("  [3/3] Testing CEA API connection... ", end="")
    try:
        # Use curl_cffi to bypass CloudFront WAF (impersonates Chrome browser)
        response = cffi_requests.post(
            DIRECTORY_URL,
            headers=HEADERS,
            json={"page": 1, "pageSize": 1, "sortAscFlag": True, "sort": "name", "name": "a", "profileType": 2},
            impersonate="chrome120",
            timeout=10,
        )
        if response.status_code == 200:
            data = response.json()
            if "data" in data:
                print(f"✓ (found {data.get('totalCount', 'N/A')} agents)")
            else:
                print("✓")
        elif response.status_code == 403:
            print("⚠ (403 - CloudFront WAF active, try from a different network)")
        else:
            print(f"✗ (status code: {response.status_code})")
            checks_passed = False
    except Exception as e:
        print(f"✗ ({e})")
        checks_passed = False

    if checks_passed:
        print("All preflight checks passed!")
    else:
        print("Some preflight checks failed.")

    return checks_passed


def scrape_agent_directory():
    """Scrapes agent directory using vowel-based filtering."""
    for vowel in VOWELS:
        print(f"searching: {vowel}...")
        directory_payload = {
            "sortAscFlag": True,
            "sort": "name",
            "name": f"{vowel}",
            "profileType": 2,
        }

        agents_df = extract.iteratively_retrieve_data(
            DIRECTORY_URL, HEADERS, directory_payload
        )
        load.write_df_to_gbq(agents_df, "estate_agents", "agents", if_exists="append")

    deduplication.deduplicate_agents()


def scrape_agent_details(start_registration_number=None):
    """Scrapes detailed agent information from registration numbers."""
    sql = f"""
    SELECT DISTINCT registrationNumber
    FROM `{PROJECT_ID}.estate_agents.agents`
    """

    if start_registration_number:
        sql += f" WHERE registrationNumber >= '{start_registration_number}'"

    sql += " ORDER BY registrationNumber ASC"

    agents_df = pandas_gbq.read_gbq(
        query_or_table=sql, project_id=PROJECT_ID, credentials=get_credentials()
    )
    registration_numbers = list(agents_df["registrationNumber"])
    extract.retrieve_all_data_for_registration_numbers(registration_numbers, HEADERS)
    deduplication.deduplicate_data(primary_key=["id"])


def main():
    """Main execution script."""
    parser = argparse.ArgumentParser(description="Scrape real estate agent data.")

    parser.add_argument(
        "commands",
        nargs="*",
        choices=["preflight", "directory", "details"],
        help="Specify 'preflight', 'directory' and/or 'details' commands.",
    )

    parser.add_argument(
        "-rn",
        "--start_registration",
        type=str,
        help="Start registration number (e.g., R012345A)",
        nargs="?",
        default=None,
    )

    args = parser.parse_args()

    if "preflight" in args.commands:
        preflight()

    if "directory" in args.commands:
        scrape_agent_directory()

    if "details" in args.commands:
        scrape_agent_details(args.start_registration)

    if not args.commands:
        parser.print_help()

    return None


if __name__ == "__main__":
    main()

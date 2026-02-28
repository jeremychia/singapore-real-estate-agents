import argparse
import pandas_gbq
import pandas as pd
import requests
import logging
from curl_cffi import requests as cffi_requests

# Assuming you have these modules defined elsewhere
import extract
import load
import load.deduplication as deduplication
from load import get_credentials, TOKEN_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

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
    logger.info("Running preflight checks...")
    checks_passed = True

    # 1. Check GCP credentials file exists
    logger.info("  [1/3] Checking GCP credentials file...")
    if TOKEN_PATH.exists():
        logger.info("        ✓ Found")
    else:
        logger.error(f"        ✗ Not found at {TOKEN_PATH}")
        checks_passed = False

    # 2. Test BigQuery connection
    logger.info("  [2/3] Testing BigQuery connection...")
    try:
        credentials = get_credentials()
        pandas_gbq.read_gbq(
            "SELECT 1",
            project_id=PROJECT_ID,
            credentials=credentials,
        )
        logger.info("        ✓ Connected")
    except Exception as e:
        logger.error(f"        ✗ {e}")
        checks_passed = False

    # 3. Test CEA API reachability
    logger.info("  [3/3] Testing CEA API connection...")
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
                logger.info(f"        ✓ Connected (found {data.get('totalCount', 'N/A')} agents)")
            else:
                logger.info("        ✓ Connected")
        elif response.status_code == 403:
            logger.warning("        ⚠ 403 - CloudFront WAF active, try from a different network")
        else:
            logger.error(f"        ✗ Status code: {response.status_code}")
            checks_passed = False
    except Exception as e:
        logger.error(f"        ✗ {e}")
        checks_passed = False

    if checks_passed:
        logger.info("✅ All preflight checks passed!")
    else:
        logger.warning("⚠️ Some preflight checks failed.")

    return checks_passed


def scrape_agent_directory():
    """Scrapes agent directory using vowel-based filtering."""
    logger.info("Starting agent directory scrape...")
    for i, vowel in enumerate(VOWELS):
        logger.info(f"[{i+1}/{len(VOWELS)}] Searching for agents with name containing '{vowel}'")
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

    logger.info("Running deduplication...")
    deduplication.deduplicate_agents()
    logger.info("✅ Agent directory scrape complete!")


def scrape_agent_details(start_registration_number=None):
    """Scrapes detailed agent information from registration numbers."""
    logger.info("Starting agent details scrape...")
    
    sql = f"""
    SELECT DISTINCT registrationNumber
    FROM `{PROJECT_ID}.estate_agents.agents`
    """

    if start_registration_number:
        sql += f" WHERE registrationNumber >= '{start_registration_number}'"
        logger.info(f"Starting from registration number: {start_registration_number}")

    sql += " ORDER BY registrationNumber ASC"

    logger.info("Fetching registration numbers from BigQuery...")
    agents_df = pandas_gbq.read_gbq(
        query_or_table=sql, project_id=PROJECT_ID, credentials=get_credentials()
    )
    registration_numbers = list(agents_df["registrationNumber"])
    logger.info(f"Found {len(registration_numbers):,} registration numbers to process")
    
    extract.retrieve_all_data_for_registration_numbers(registration_numbers, HEADERS)
    
    logger.info("Running deduplication...")
    deduplication.deduplicate_data(primary_key=["id"])
    logger.info("✅ Agent details scrape complete!")


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

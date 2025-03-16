import argparse
import os
import pandas_gbq
import pandas as pd

# Assuming you have these modules defined elsewhere
import extract
import load
import load.deduplication as deduplication

PROJECT_ID = "singapore-real-estate-agents"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'",
}

DIRECTORY_URL = (
    "https://www.cea.gov.sg/aceas/api/internet/profile/v2/public-register/filter"
)
VOWELS = ["a", "e", "i", "o", "u"]


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

    agents_df = pandas_gbq.read_gbq(query_or_table=sql, project_id=PROJECT_ID)
    registration_numbers = list(agents_df["registrationNumber"])
    extract.retrieve_all_data_for_registration_numbers(registration_numbers, HEADERS)
    deduplication.deduplicate_data(primary_key=["id"])


def main():
    """Main execution script."""
    parser = argparse.ArgumentParser(description="Scrape real estate agent data.")

    parser.add_argument(
        "commands",
        nargs="*",
        choices=["directory", "details"],
        help="Specify 'directory' and/or 'details' commands.",
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

    if "directory" in args.commands:
        scrape_agent_directory()

    if "details" in args.commands:
        scrape_agent_details(args.start_registration)

    if not args.commands:
        parser.print_help()

    return None


if __name__ == "__main__":
    main()

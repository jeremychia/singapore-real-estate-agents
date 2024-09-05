import os

import extract
import load

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
}

directory_payload = {"sortAscFlag": True, "sort": "name", "name": "a", "profileType": 2}

directory_url = (
    "https://www.cea.gov.sg/aceas/api/internet/profile/v2/public-register/filter"
)

agents_df = extract.iteratively_retrieve_data(directory_url, headers, directory_payload)
load.write_df_to_gbq(agents_df, "estate_agents", "agents")

registration_numbers = list(agents_df["registrationNumber"])
all_data = extract.retrieve_all_data_for_registration_numbers(
    registration_numbers, headers
)

hdb_resale_df = all_data["hdb_resale"]
load.write_df_to_gbq(hdb_resale_df, "estate_agents", "hdb_resale")

hdb_rental_df = all_data["hdb_rental"]
load.write_df_to_gbq(hdb_rental_df, "estate_agents", "hdb_rental")

private_rental_df = all_data["private_rental"]
load.write_df_to_gbq(private_rental_df, "estate_agents", "private_rental")

private_sale_df = all_data["private_sale"]
load.write_df_to_gbq(private_sale_df, "estate_agents", "private_sale")

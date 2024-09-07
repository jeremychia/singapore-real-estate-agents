import os
import pandas_gbq

import extract
import load

PROJECT_ID = "jeremy-chia"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
}

directory_payload = {"sortAscFlag": True, "sort": "name", "name": "a", "profileType": 2}

directory_url = (
    "https://www.cea.gov.sg/aceas/api/internet/profile/v2/public-register/filter"
)

# agents_df = extract.iteratively_retrieve_data(directory_url, headers, directory_payload)
# load.write_df_to_gbq(agents_df, "estate_agents", "agents")

sql = """
select registrationNumber
from `jeremy-chia.estate_agents.agents`
where registrationNumber > 'R003701C'
order by registrationNumber asc
"""

# start from R003706D

agents_df = pandas_gbq.read_gbq(
    query_or_table=sql,
    project_id=PROJECT_ID
)

registration_numbers = list(agents_df["registrationNumber"])
extract.retrieve_all_data_for_registration_numbers(
    registration_numbers, headers
)

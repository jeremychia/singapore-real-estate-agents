import requests
import pandas as pd
import pandas_gbq
import os
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

possible_mobile_numbers_sg = [str(number) for number in range(80004300, 98999999 + 1)]

mobile_numbers = []
ids = []
registration_numbers = []
names = []

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
}

for idx, mobile_number in enumerate(possible_mobile_numbers_sg):
    mobile_number_payload = {
        "page": 1,
        "pageSize": 10,
        "sortAscFlag": True,
        "sort": "name",
        "contactNumber": mobile_number,
        "profileType": 2,
    }

    directory_url = (
        "https://www.cea.gov.sg/aceas/api/internet/profile/v2/public-register/filter"
    )
    response = requests.post(directory_url, headers=headers, json=mobile_number_payload)
    data = response.json().get("data")

    if data:
        mobile_numbers.append(mobile_number)
        ids.append(data[0]["id"])
        registration_numbers.append(data[0]["registrationNumber"])
        names.append(data[0]["name"])

    if idx % 100 == 0:
        print(f"Processed {idx} mobile numbers")


mobile_numbers_df = pd.DataFrame(
    {
        "mobile_number": mobile_numbers,
        "id": ids,
        "registration_number": registration_numbers,
        "name": names,
        "_accessed_at_utc": [datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z%z")]
        * len(mobile_numbers),
    }
)

pandas_gbq.to_gbq(
    mobile_numbers_df, "estate_agents.mobile_numbers", "jeremy-chia", if_exists="append"
)

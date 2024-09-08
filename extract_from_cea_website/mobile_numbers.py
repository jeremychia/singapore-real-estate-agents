import requests
import pandas as pd
import pandas_gbq
import os
from datetime import datetime, timedelta

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

possible_mobile_numbers_sg = [str(number) for number in range(81034300, 98999999 + 1)]

mobile_numbers = []
ids = []
registration_numbers = []
names = []

headers = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json",
}

# Time Log
average_time = timedelta(seconds=0)
count_mobile_numbers = len(possible_mobile_numbers_sg)

for idx, mobile_number in enumerate(possible_mobile_numbers_sg):
    start_time = datetime.now()
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
        print(f"found match for: {mobile_number}")
        mobile_numbers.append(mobile_number)
        ids.append(data[0]["id"])
        registration_numbers.append(data[0]["registrationNumber"])
        names.append(data[0]["name"])

    # Compute end-time
    end_time = datetime.now()
    time_diff = end_time - start_time
    average_time = (average_time * idx + time_diff) / (idx + 1)

    if idx % 100 == 0 and idx != 0:
        remaining_time = average_time * (count_mobile_numbers - idx - 1)
        completion_time = (end_time + remaining_time).strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"Complete: {idx+1} out of {count_mobile_numbers} (Average time: {round(time_diff.total_seconds(), 1)}). Estimated completion: {completion_time}"
        )

    if idx % 1000 == 0 and idx != 0:
        print(
            f"Uploading: {possible_mobile_numbers_sg[idx-1000]} to {possible_mobile_numbers_sg[idx]}"
        )
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
            mobile_numbers_df,
            "estate_agents.mobile_numbers",
            "jeremy-chia",
            if_exists="append",
        )

        mobile_numbers = []
        ids = []
        registration_numbers = []
        names = []

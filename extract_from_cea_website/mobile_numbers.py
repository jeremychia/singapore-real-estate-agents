import requests
import pandas as pd
import pandas_gbq
import os
from datetime import datetime, timedelta
import time

PROJECT_ID = "singapore-real-estate-agents"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "token/gcp_token.json"

possible_mobile_numbers_sg = [str(number) for number in range(81168776, 98999999 + 1)]

mobile_numbers = []
ids = []
registration_numbers = []
names = []

# Time Log
average_time = timedelta(seconds=0)
count_mobile_numbers = len(possible_mobile_numbers_sg)

# Define the API URL
directory_url = "https://www.cea.gov.sg/aceas/api/internet/profile/v2/public-register/filter"

# Implement a retry strategy with exponential backoff
def request_with_retry(mobile_number, retries=5, delay=1):
    """ Function to make the request with retries and exponential backoff in case of failure """

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9,as;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=UTF-8",
        "DNT": "1",
        "Host": "www.cea.gov.sg",
        "Origin": "https://www.cea.gov.sg",
        "Referer": f"https://www.cea.gov.sg/aceas/public-register/sales/1?page=1&pageSize=10&sortAscFlag=true&sort=name&contactNumber={mobile_number}",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"'
    }

    for attempt in range(retries):
        try:
            response = requests.post(directory_url, headers=headers, json=mobile_number_payload)
            
            # Check if we hit rate limit (status code 463)
            if response.status_code == 463:  # Too many requests
                print(f"Rate limit exceeded for {mobile_number}. Retrying...")
                time.sleep(delay)  # wait before retrying
                delay *= 2  # Exponential backoff
                continue
            
            # Check if the response is successful (200 OK)
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code} for {mobile_number}")
                return None
            
            # Attempt to parse the response as JSON
            try:
                return response.json().get("data")
            except ValueError as e:
                print(f"JSON decoding failed for {mobile_number}: {e}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {mobile_number} (Attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                print(f"Max retries reached for {mobile_number}. Skipping.")
                return None

# Time tracking and logging
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

    data = request_with_retry(mobile_number)

    if data:
        print(f"Found match for: {mobile_number}")
        mobile_numbers.append(mobile_number)
        ids.append(data[0]["id"])
        registration_numbers.append(data[0]["registrationNumber"])
        names.append(data[0]["name"])

    # Time tracking and logging
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
            f"{PROJECT_ID}",
            if_exists="append",
        )
        # Clear data for the next upload batch
        mobile_numbers = []
        ids = []
        registration_numbers = []
        names = []

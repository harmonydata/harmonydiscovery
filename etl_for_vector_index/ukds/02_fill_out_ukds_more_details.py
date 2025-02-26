import json
import time

import requests
from tqdm import tqdm

with open("intermediate_output/ukds_studies_intermediate_data.json", "r", encoding="utf-8") as f:
    ukds_studies = json.loads(f.read())
print("Number of studies found:", len(ukds_studies))

ukds_all_data = [None] * len(ukds_studies)
for idx in range(len(ukds_studies)):
    ukds_all_data[idx] = [ukds_studies[idx], None]

headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    # 'cookie': 'UKDSCC=false',
    'if-modified-since': 'Mon, 26 Jul 1997 05:00:00 GMT',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://beta.ukdataservice.ac.uk/datacatalogue/studies/study?id=4442',
    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

IS_RESUME = True
if IS_RESUME:
    with open("intermediate_output/ukds_all_raw_data.json", "r", encoding="utf-8") as f:
        ukds_all_data = json.loads(f.read())

def save():
    print("Saving backup")
    with open("intermediate_output/ukds_all_raw_data.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(ukds_all_data, indent=4))

for idx, study in enumerate(tqdm(ukds_studies)):
    if ukds_all_data[idx][1] is not None:
        print (f"Skipping {idx} as already done")
        continue
    params = {
        'id': study["id"],
    }

    try:
        response_this_study = requests.get(
            'https://beta.ukdataservice.ac.uk/Umbraco/Surface/Discover/Study',
            params=params,
            headers=headers,
        )

        ukds_all_data[idx][1] = response_this_study.json()
    except:
        print(f"Error at index {idx}")
        time.sleep(30)

    time.sleep(5)

    if idx % 10 == 0:
        save()

save()

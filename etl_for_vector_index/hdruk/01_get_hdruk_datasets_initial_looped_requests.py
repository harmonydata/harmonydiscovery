import json
import time

import requests

import json
import pathlib
from pathlib import Path
import os

maximum_results = 99999

all_results = []


output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

for start_page in range(0, maximum_results, 40):
    if start_page >= maximum_results:
        break
    url = f'https://api.www.healthdatagateway.org/api/v1/search?tab=Datasets&datasetIndex={start_page}'
    print(f"On page {start_page}: querying URL {url}")
    response = requests.get(url)

    num_hits = len(response.json()["datasetResults"]['data'])

    print("Name of first result:", response.json()["datasetResults"]['data'][0]["name"])

    dc = response.json()['summary']["datasetCount"]
    if dc < maximum_results:
        maximum_results = dc
        print(f"Setting the maximum results to {maximum_results}. We will stop requesting after this many requests.")

    all_results.append(response.json())

    print(f"Percentage done: {start_page / maximum_results:.0%}")

    time.sleep(10)

print("Number of results found:", len(all_results))

with open("intermediate_output/hdruk_datasets_intermediate_data.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(all_results, indent=4))

import json
from time import sleep

import requests
from tqdm import tqdm

with open("intermediate_output/hdruk_datasets_intermediate_data.json", "r", encoding="utf-8") as f:
    hdruk_datasets = json.loads(f.read())

hdruk_all_data = []

for results_batch in hdruk_datasets:
    for result in results_batch["datasetResults"]["data"]:
        hdruk_all_data.append([result, []])

print("Number of studies found:", len(hdruk_all_data))

for idx in tqdm(range(len(hdruk_all_data))):
    dataset = hdruk_all_data[idx][0]

    dataset_id = dataset['datasetid']

    url = f'https://api.www.healthdatagateway.org/api/v1/datasets/{dataset_id}'

    try:
        print(url)
        response = requests.get(url)

        hdruk_all_data[idx][1] = response.json()

        sleep(5)
    except:
        print(f"Error at index {idx}")
        sleep(60)

    if idx % 10 == 1 or idx == len(hdruk_all_data) - 1:
        print("Saving to disk...")
        with open("intermediate_output/hdruk_all_raw_data.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(hdruk_all_data, indent=4))

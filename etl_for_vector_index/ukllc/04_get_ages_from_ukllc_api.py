import json
import os

import requests

# define API key
API_KEY = os.environ['UKLLC_FASTAPI_KEY']

input_file_datasets = "intermediate_output/datasets.json"

print(f"Reading datasets from {input_file_datasets}...")

with open("intermediate_output/datasets.json", "r", encoding="utf-8") as f:
    all_datasets = json.loads(f.read())

print(f"Successfully read datasets from {input_file_datasets}")

ages_data = {}
# define base url
url1 = "https://metadata-api-4a09f2833a54.herokuapp.com/dataset-ages/?"
# for each data source / dataset pair build url and request
for i in all_datasets:
    # format source and dataset
    source = i['source']
    dataset = i['table']
    url2 = "source={}&table_name={}".format(source, dataset)
    # full URL
    url = url1 + url2
    # make request
    r = requests.get(url, headers={'access_token': API_KEY})
    print(r.status_code)
    # convert to text
    data = r.text
    # if data found
    if r.status_code == 200:
        # convert to json
        pj = json.loads(data)
        # build dict key incl source and dataset
        k = '_'.join([source, dataset])
        # build dict of all outputs
        ages_data[k] = pj

output_file = "intermediate_output/json.json"

print(f"Writing ages data to {output_file}...")

with open(output_file, "w", encoding="utf-8") as f:
    f.write(json.dumps(ages_data, indent=4))
print(f"Successfully wrote ages data to {output_file}")

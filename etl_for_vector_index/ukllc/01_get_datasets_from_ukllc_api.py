import json
import os

import requests

# define API key
API_KEY = os.environ['UKLLC_FASTAPI_KEY']

# get all datasets
url = "https://metadata-api-4a09f2833a54.herokuapp.com/all-datasets"
r = requests.get(url, headers={'access_token': API_KEY})
print(r.status_code)
all_datasets = r.json()

output_file = "intermediate_output/datasets.json"

print (f"Writing datasets to {output_file}...")

with open(output_file, "w", encoding="utf-8") as f:
    f.write(json.dumps(all_datasets, indent=4))

print (f"Successfully wrote datasets to {output_file}")
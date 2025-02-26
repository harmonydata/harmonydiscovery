import json
import os

import requests

# define API key
API_KEY = os.environ['UKLLC_FASTAPI_KEY']

# test endpoint with filter and token
url = "https://metadata-api-4a09f2833a54.herokuapp.com/all-sources"
r = requests.get(url, headers={'access_token': API_KEY})

output_file = "intermediate_output/sources.json"

print(f"Writing sources to {output_file}...")

with open(output_file, "w", encoding="utf-8") as f:
    f.write(json.dumps(r.json(), indent=4))

print(f"Successfully wrote sources to {output_file}")

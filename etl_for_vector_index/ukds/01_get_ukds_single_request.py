import json
import os
import pathlib
from pathlib import Path

import requests

output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

payload = {
    "rows": 10000,
}

response = requests.post('https://beta.ukdataservice.ac.uk/Umbraco/Surface/Discover/Studies',
                         json=payload)

print("Number of results found:", len(response.json()["results"]))

ukds_studies = response.json()["results"]

with open("intermediate_output/ukds_studies_intermediate_data.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(ukds_studies, indent=4))

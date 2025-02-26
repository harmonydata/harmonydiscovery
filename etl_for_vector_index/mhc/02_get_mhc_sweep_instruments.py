import json
import os
import pathlib
from pathlib import Path

import requests

output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

output_file = os.path.join(output_folder, "mhc_sweep_instruments.json")

headers = {
    'accept': 'application/json',
    'Range-Unit': 'items',
}

response = requests.get(
    'https://www.cataloguementalhealth.ac.uk:443/testing/api/v2/sweep_instruments?limit=10000&standard_instrument=eq.Yes',
    headers=headers)

with open(output_file, "w", encoding="utf-8") as f:
    f.write(json.dumps(response.json(), indent=4))

import json
import time

import requests
from tqdm import tqdm

import json
import pathlib
from pathlib import Path
import os

output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

total_pages = 99999

all_results = []

def save():
    print ("Saving...")
    with open("intermediate_output/adruk_datasets_intermediate_data.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(all_results, indent=4))

for start_page in tqdm(range(1, total_pages, 1)):
    if start_page >= total_pages:
        break
    try:
        print(f"Page {start_page}")
        url = f'https://api-datacatalogue.adruk.org/api/dataset?searchTerm=*&include=dataset::datastandard::terminology::dataclass::dataelement&pageNumber={start_page}'
        print(url)
        response = requests.get(url, headers={"X-Api-Version": "2.0"})
        num_hits = len(response.json()['content'])

        print("Name of first result:", response.json()['content'][0]["title"])

        all_results.append(response.json())

        if response.json()['totalPages'] < total_pages:
            total_pages = response.json()['totalPages']
            print (f"There are {total_pages} pages in the result set")

        time.sleep(2)
    except:
        print(f"Error occurred at page {start_page}")

    if start_page % 10 == 1:
        save()

print("Number of results found:", len(all_results))

save()
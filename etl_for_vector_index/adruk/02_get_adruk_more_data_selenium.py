import json
import re
import time

from selenium.webdriver.chrome import webdriver
from selenium.webdriver.common.by import By
from tqdm import tqdm

import json
import pathlib
from pathlib import Path
import os

output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

driver = webdriver.WebDriver()

with open("intermediate_output/adruk_datasets_intermediate_data.json", "r", encoding="utf-8") as f:
    adruk_datasets = json.loads(f.read())

# The Div headers we are interested in in the HTML page
key_strings = ["Spatial Coverage:", "Description:", "Start Date:", "End Date:", "Access Rights:"]

adruk_data_full = []

for adruk_datasets_batch in adruk_datasets:
    items_in_batch = adruk_datasets_batch['content']
    for item in items_in_batch:
        id = item["id"]
        search_result_type = item['searchResultType']
        if search_result_type == "PHYSICAL":  # other types are  'DataElement': 143676, 'DataClass': 1118})
            adruk_data_full.append([item, None, None])

for idx in tqdm(range(len(adruk_data_full))):
    item = adruk_data_full[idx][0]
    url_tail = re.sub(r' ', '/', item["searchDetails"]["id"])
    url = "https://datacatalogue.adruk.org/browser/dataset/" + url_tail
    print(url)

    driver.get(url)
    time.sleep(10)
    elements = list(driver.find_elements(By.XPATH, "//div"))

    element_texts = [x.text for x in elements]

    values_from_html = {}

    for key_string in key_strings:
        is_found = False
        for text in element_texts:
            if text.startswith(key_string):
                is_found = True
            else:
                if is_found:
                    if len(text.strip()) > 0:
                        key_string_clean = re.sub(r':', '', re.sub(r' ', '_', key_string)).lower()
                        values_from_html[key_string_clean] = text
                        break

    adruk_data_full[idx][1] = url
    adruk_data_full[idx][2] = values_from_html

    print("Values:\n" + json.dumps(values_from_html, indent=4))

    with open("intermediate_output/adruk_datasets_full_data.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(adruk_data_full, indent=4))

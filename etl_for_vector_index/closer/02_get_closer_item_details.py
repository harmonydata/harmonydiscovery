import os
import sys

from tqdm import tqdm

pw = os.environ["CLOSER_PW"]
username = os.environ["CLOSER_USERNAME"]
import os.path

import json
from time import sleep

from colectica_api import ColecticaObject

C = ColecticaObject("discovery.closer.ac.uk", username, pw)

with open("intermediate_output/closer_items.json", "r", encoding="utf-8") as f:
    closer_items = json.loads(f.read())

item_types = ["Study", "Variable", "Question", "Data File", "Instrument"]

if len(sys.argv) > 1:
    item_types = [sys.argv[1]]
else:
    print(f"Processing all variable types: {item_types}")

for item_type in closer_items:
    print(f"Looking at {item_type}...")

    output_file = f"intermediate_output/{item_type}.json"
    items_already_got = set()

    if os.path.isfile(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            lines = f.read().split("\n")
        for line in lines:
            if len(line.strip()) > 0:
                d = json.loads(line)
                agency_id = d["AgencyId"]
                identifier = d["Identifier"]
                tup = (agency_id, identifier)
                items_already_got.add(tup)
    print(f"Number of items already downloaded: {len(items_already_got)}")

    with open(output_file, "a", encoding="utf-8") as f:

        items_not_already_downloaded = set()

        for item_to_request in closer_items[item_type]["Results"]:

            agency_id = item_to_request["AgencyId"]
            identifier = item_to_request["Identifier"]
            tup = (agency_id, identifier)

            if tup not in items_already_got:
                items_not_already_downloaded.add(tup)

        print(f"There are {len(items_not_already_downloaded)} items left to download.")

        for tup in tqdm(items_not_already_downloaded):

            agency_id, identifier = tup

            url = f"https://discovery.closer.ac.uk/item/{agency_id}/{identifier}"
            print(url)
            try:
                df, X = C.item_info_set(AgencyId=agency_id,
                                        Identifier=identifier)
                f.write(json.dumps(X) + "\n")
                f.flush()

                sleep(5)
            except:

                print(f"Error getting data for {url}")

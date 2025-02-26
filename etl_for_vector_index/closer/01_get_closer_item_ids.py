import json
import pathlib
from pathlib import Path
import os

pw = os.environ["CLOSER_PW"]
username = os.environ["CLOSER_USERNAME"]

output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

from colectica_api import ColecticaObject

C = ColecticaObject("discovery.closer.ac.uk", username, pw)

closer_items = {}
for item_type in [
    "Study", "Question", "Instrument", "Data Collection", 'Question Group', "Concept", "Variable", "Organization"]:
    print(f"Getting {item_type}")
    closer_items[item_type] = C.search_item(item_type=C.item_code(item_type), search_term="*", MaxResults=100000000)

with open("intermediate_output/closer_items.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(closer_items, indent=4))

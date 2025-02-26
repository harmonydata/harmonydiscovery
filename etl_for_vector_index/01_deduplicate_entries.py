import json
import operator
import os
from collections import Counter
from datetime import datetime

from schema import HarmonyResource

sources = set()
for dir in os.listdir("."):
    if os.path.isdir(dir):
        if os.path.isfile(dir + "/resources_for_vector_index.json"):
            sources.add(dir)
sources = sorted(sources)

print(sources)

all_resources = []

for source in sources:
    file = source + "/resources_for_vector_index.json"
    with open(file, "r", encoding="utf-8") as f:
        print(file)
        for l in f:
            d = json.loads(l)
            r = HarmonyResource(**d)
            all_resources.append(r)

print("Number of resources before merging", len(all_resources))

ctr = Counter()
title_to_dataset = {}
for x in all_resources:
    ctr[x.name] += 1
    if x.name not in title_to_dataset:
        title_to_dataset[x.name] = []
    title_to_dataset[x.name].append(x)

merged_resources = []
for title, count in sorted(ctr.items(), key=operator.itemgetter(1), reverse=True):
    if count == 1:
        merged_resources.extend(title_to_dataset[title])
    elif count > 1:
        # merge
        merged_resource = title_to_dataset[title][0]

        all_text = ""
        sources = set()
        variables = []
        urls = []
        dois = []

        for dataset_to_merge in title_to_dataset[title]:
            sources.add(dataset_to_merge.source)
            if len(dataset_to_merge.all_text) > len(all_text):
                all_text = dataset_to_merge.all_text

            if len(dataset_to_merge.variables) > 0:
                variables.extend(dataset_to_merge.variables)

            if len(dataset_to_merge.urls) > 0:
                urls.extend(dataset_to_merge.urls)

            if len(dataset_to_merge.dois) > 0:
                dois.extend(dataset_to_merge.dois)

            for k, v in dataset_to_merge.extra_data_schema.items():
                if k not in merged_resource.extra_data_schema:
                    merged_resource.extra_data_schema[k] = v
                else:
                    if type(v) is list and type(merged_resource.extra_data_schema[k]) is list:
                        items_already_seen = set()
                        for item in merged_resource.extra_data_schema[k]:
                            items_already_seen.add(json.dumps(item, sort_keys=True))
                        for item in v:
                            candidate_to_add = json.dumps(item, sort_keys=True)
                            if candidate_to_add not in items_already_seen:
                                merged_resource.extra_data_schema[k].append(item)
                            items_already_seen.add(candidate_to_add)
                    elif type(v) is str and type(merged_resource.extra_data_schema[k]) is str:
                        old_value = merged_resource.extra_data_schema[k]
                        if v != old_value:
                            print(f"Warning! not sure how to merge field {k} with values {v} and {old_value}")
                    else:
                        print(f"Error! not sure how to merge field {k} with value {v}")
                        print(json.dumps(merged_resource.extra_data_schema, indent=4))
                        print(json.dumps(dataset_to_merge.extra_data_schema, indent=4))

        merged_resource.all_text = all_text
        merged_resource.source = " ".join(sorted(sources))
        merged_resource.urls = sorted(set(urls))
        merged_resource.dois = sorted(set(dois))

        merged_resources.append(merged_resource)

print("Number of resources after merging", len(merged_resources))

top_level_resource_types = set()
id_counter = Counter()
for resource in merged_resources:
    id_counter[resource.id] += 1
    top_level_resource_types.add(resource.resource_type.value)

print("Resource types found at top level:", top_level_resource_types)
if "variable" in top_level_resource_types:
    print("Variable cannot be top level")
    1 / 0

if max(id_counter.values()) > 1:
    date_stamp = datetime.now().strftime("%Y%b%d_%H_%M")
    duplicate_ids_file = f"/tmp/{date_stamp}_duplicate_ids.txt"
    print(f"Warning! Duplicate IDs found. They will be written to {duplicate_ids_file}")
    with open(duplicate_ids_file, "w", encoding="utf-8") as f:
        for k, v in sorted(id_counter.items(), key=operator.itemgetter(1), reverse=True):
            if v > 1:
                f.write(f"{k}\t{v}\n")
else:
    print("Yay! The IDs are unique")

with open("merged_resources.json", "w", encoding="utf-8") as f:
    for resource in merged_resources:
        f.write(resource.model_dump_json() + "\n")

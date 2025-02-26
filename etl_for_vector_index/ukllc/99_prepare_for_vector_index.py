import sys

sys.path.append("..")

import re
from collections import Counter

import numpy as np

from schema import HarmonyResource, Sex, ResourceType
from util import get_clean_start_year, get_clean_end_year, save_resources, get_clean_sample_size
from dateutil.parser import parse
import json

with open("intermediate_output/datasets.json", "r", encoding="utf-8") as f:
    datasets = json.loads(f.read())

with open("intermediate_output/sources.json", "r", encoding="utf-8") as f:
    sources = json.loads(f.read())

with open("intermediate_output/all_labels.json", "r", encoding="utf-8") as f:
    all_labels = json.loads(f.read())

with open("intermediate_output/ages.json", "r", encoding="utf-8") as f:
    ages_data = json.loads(f.read())
# TODO: need lower and upper bound ages

harmony_resources = []

print(f"Indexing the variables. There will be variables from {len(all_labels)} sources")

variable_id_counter = Counter()
source_and_dataset_to_harmony_variables = {}

for source_and_dataset, ukllc_variables in all_labels.items():
    unique_vars_found = {}
    for ukllc_variable in ukllc_variables:
        variable_label = ukllc_variable["variable_label"]
        variable_name = ukllc_variable["variable_name"]
        if variable_name is None:
            variable_name = variable_label
        if variable_label is None:
            variable_label = variable_name
        value_label = ukllc_variable["value_label"]
        tup = (variable_label, variable_name)
        if tup not in unique_vars_found:
            unique_vars_found[tup] = set()
        if value_label is not None:
            unique_vars_found[tup].add(value_label)

    for (variable_label, variable_name), response_options in unique_vars_found.items():
        variable_id = f"ukllc/{source_and_dataset}/{variable_name}".lower()

        variable_id_counter[variable_id] += 1
        variable_id_def_unique = f"{variable_id}_{variable_id_counter[variable_id]}"

        response_options = list(response_options)

        resource = HarmonyResource(
            all_text=variable_label,
            age_lower=0,
            age_upper=0,
            country_codes=["GB"],
            description=variable_label,
            data_access="",
            dois=[],
            duration_years=0,
            end_year=0,
            genetic_data_collected=False,
            geographic_coverage="",
            id=variable_id_def_unique,
            instruments=[],
            language_codes=["en"],
            num_sweeps=0,
            num_variables=0,
            original_content="",
            question="",
            response_options=response_options,
            resource_type=ResourceType.variable,
            sample_size=0,
            sex=Sex.all,
            source="ukllc",
            start_year=0,
            study_design=["longitudinal"],
            name=variable_name,
            keywords=[],
            urls=[],
            variables=[],
            extra_data_schema={}
        )

        source_and_dataset_for_harmony = "ukllc/" + source_and_dataset
        if source_and_dataset_for_harmony not in source_and_dataset_to_harmony_variables:
            source_and_dataset_to_harmony_variables[source_and_dataset_for_harmony] = []
        source_and_dataset_to_harmony_variables[source_and_dataset_for_harmony].append(resource)

print(f"Creating the sources. There will be {len(sources)} sources")
for source in sources:
    internal_source_id = source["source"]

    source_id = ("ukllc/" + internal_source_id).lower()

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "UK Longitudinal Linkage Collaboration: UK LLC",
            "url": "https://explore.ukllc.ac.uk",
            "image": "https://ukllc.ac.uk/wp-content/themes/ukllc-theme/assets/images/UKLLC_Logo_Landscape_Black.svg"
        }],
    }

    urls = [f"https://explore.ukllc.ac.uk/?source={internal_source_id}"]

    # TODO
    age_lower = 0
    age_upper = 999
    variables = []
    description = source["source_name"]
    duration_years = 0
    start_year = 0
    end_year = 0

    resource = HarmonyResource(
        all_text=description,
        age_lower=age_lower,
        age_upper=age_upper,
        country_codes=["GB"],
        description=description,
        data_access="",
        dois=[],
        duration_years=duration_years,
        end_year=0,
        genetic_data_collected=False,
        geographic_coverage="UK",
        id=source_id,
        instruments=[],
        language_codes=["en"],
        num_sweeps=0,
        num_variables=len(variables),
        original_content=json.dumps(source, indent=4),
        question="",
        response_options=[],
        resource_type=ResourceType.source,
        sample_size=0,
        sex=Sex.all,
        source="ukllc",
        start_year=0,
        study_design=["longitudinal"],
        name=source["source_name"],
        keywords=[],
        urls=urls,
        variables=variables,
        extra_data_schema=extra_data_schema
    )
    harmony_resources.append(resource)

print(f"Created {len(harmony_resources)} Harmony resources so far")

print(f"Creating the datasets. There will be about {len(datasets)} datasets")

for dataset in datasets:

    if dataset["table_name"] is None:
        continue

    dataset_id = ("ukllc/" + dataset["source"] + "_" + dataset["table"]).lower()

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "UK Longitudinal Linkage Collaboration: UK LLC",
            "url": "https://explore.ukllc.ac.uk",
            "image": "https://ukllc.ac.uk/wp-content/themes/ukllc-theme/assets/images/UKLLC_Logo_Landscape_Black.svg"
        }],
    }

    collection_start_str = dataset["collection_start"]
    collection_end_str = dataset["collection_end"]

    if collection_start_str is not None:
        start_year = get_clean_start_year(collection_start_str)
        collection_start_dt = parse(collection_start_str)
    else:
        start_year = 0
        collection_start_dt = None

    if collection_end_str is not None:
        try:
            end_year = get_clean_end_year(collection_end_str)
            collection_end_dt = parse(collection_end_str)
        except:
            print("couldn't parse ", collection_end_str)
            end_year = 0
            collection_end_dt = None
    else:
        end_year = 0
        collection_end_dt = None

    if collection_start_dt is not None and collection_end_dt is not None:
        extra_data_schema["temporalCoverage"] = collection_start_dt.strftime(
            "%y-%m") + ".." + collection_end_dt.strftime("%y-%m")
        duration_years = int(np.round((collection_end_dt - collection_start_dt).days / 365))

    elif collection_start_dt is not None:
        extra_data_schema["temporalCoverage"] = collection_start_dt.strftime("%y-%m") + ".."
        duration_years = 0

    description = dataset["long_desc"]
    if description is None:
        description = dataset["short_desc"]
    if description is None:
        description = dataset["table_name"]

    sample_size = get_clean_sample_size(dataset["participants_invited"])

    urls = [f"https://explore.ukllc.ac.uk/?source={dataset['source']}"]

    if dataset["links"] is not None:
        for segment in dataset["links"].split(")"):
            segment = re.sub(r'.*\(', '', segment)
            if "http" in segment:
                urls.append(segment)

    # TODO
    age_lower = 0
    age_upper = 999
    variables = []

    variables = source_and_dataset_to_harmony_variables.get(dataset_id, [])

    resource = HarmonyResource(
        all_text=description,
        age_lower=age_lower,
        age_upper=age_upper,
        country_codes=["GB"],
        description=description,
        data_access="",
        dois=[],
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=False,
        geographic_coverage="UK",
        id=dataset_id,
        instruments=[],
        language_codes=["en"],
        num_sweeps=0,
        num_variables=len(variables),
        original_content=json.dumps(dataset, indent=4),
        question="",
        response_options=[],
        resource_type=ResourceType.dataset,
        sample_size=sample_size,
        sex=Sex.all,
        source="ukllc",
        start_year=start_year,
        study_design=["longitudinal"],
        name=dataset["table_name"],
        keywords=[],
        urls=urls,
        variables=variables,
        extra_data_schema=extra_data_schema
    )
    harmony_resources.append(resource)
    # print (len(variables))

print(f"Created {len(harmony_resources)} Harmony resources so far")

save_resources(harmony_resources)

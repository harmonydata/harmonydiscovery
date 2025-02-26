import sys

sys.path.append("..")

import bz2
import datetime
import pickle as pkl
import re
from collections import Counter

import numpy as np
import pandas as pd
from tqdm import tqdm

from schema import HarmonyResource, Sex, ResourceType
from util import get_clean_start_year, get_clean_end_year, save_resources, get_clean_sample_size, get_age_limit, \
    clean_study_design, get_countries

df_datasets = pd.read_pickle("intermediate_output/datasets.pkl.bz2")

df_sources = pd.read_pickle("intermediate_output/sources.pkl.bz2")

df_dataset_ages = pd.read_pickle("intermediate_output/dataset_ages.pkl.bz2")
df_dataset_ages["source_table"] = (df_dataset_ages["source"] + "_" + df_dataset_ages["table_name"]).apply(
    lambda x: x.lower())
df_dataset_ages.set_index("source_table", inplace=True)

with bz2.open("intermediate_output/table_metadata.pkl.bz2", "r") as f:
    all_table_metadata = pkl.load(f)

harmony_resources = []

all_variables_in_ukllc = {}

# Get all the dataset IDs from the dataframes
all_dataset_ids = []
dataset_id_to_variables = {}
for idx in range(len(df_datasets)):
    dataset_id = ("ukllc/" + df_datasets.source.iloc[idx].lower() + "_" + df_datasets.table.iloc[idx]).lower()
    all_dataset_ids.append(dataset_id)
    dataset_id_to_variables[dataset_id] = []

print("Getting variables from the list of metadata tables")

variable_ids_seen = Counter()

variable_to_num_parent_datasets_found = {}
variables_where_we_could_not_identify_parent_study = set()
for metadata_table_name, metadata_df in tqdm(all_table_metadata.items()):
    table_name_parts = metadata_table_name.split("_")
    # ukllc_source_and_dataset = "ukllc/" + table_name_parts[1] + "/" + table_name_parts[2]

    ukllc_source_and_dataset = "ukllc/" + re.sub(r'^metadata_', '', metadata_table_name)

    for variable_name, subset in metadata_df.groupby("Variable Name"):

        variable_id_for_harmony = ukllc_source_and_dataset + "/" + variable_name.lower()

        disambiguation_number = variable_ids_seen[variable_id_for_harmony]
        variable_ids_seen[variable_id_for_harmony] += 1
        if disambiguation_number > 0:
            variable_id_for_harmony = f"{variable_id_for_harmony}_{disambiguation_number}"

        ukllc_source = subset.Source.iloc[0]

        description = subset["Variable Description"].iloc[0]

        if pd.isna(description) or description is None:
            description = variable_name

        response_options = list(subset["Value Description"].dropna().apply(lambda x: str(x)))
        if len(response_options) == 0:
            response_options = list(subset["Value"].dropna().apply(lambda x: str(x)))


        resource = HarmonyResource(
            all_text=description,
            age_lower=0,
            age_upper=0,
            country_codes=["GB"],
            country="",
            description=description,
            data_access="",
            dois=[],
            duration_years=0,
            end_year=0,
            genetic_data_collected=False,
            geographic_coverage="",
            id=variable_id_for_harmony,
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
            study_design=[],
            name=variable_name,
            keywords=[],
            urls=[],
            variables=[],
            extra_data_schema={}
        )

        all_variables_in_ukllc[variable_id_for_harmony] = resource

        candidate_datasets_this_variable = []
        for candidate_dataset_id in all_dataset_ids:
            if variable_id_for_harmony.startswith(candidate_dataset_id):
                candidate_datasets_this_variable.append(candidate_dataset_id)
        variable_to_num_parent_datasets_found[variable_id_for_harmony] = len(candidate_datasets_this_variable)
        if len(candidate_datasets_this_variable) > 1:
            candidate_dataset = max(candidate_datasets_this_variable, key=lambda x: len(x))
        elif len(candidate_datasets_this_variable) == 1:
            candidate_dataset = candidate_datasets_this_variable[0]
        else:
            candidate_dataset = None

        if candidate_dataset is not None:
            dataset_id_to_variables[candidate_dataset].append(resource)
        else:
            variables_where_we_could_not_identify_parent_study.add(variable_id_for_harmony)

source_id_to_dataset_resource = {}

for idx in range(len(df_datasets)):
    name = df_datasets.table_name.iloc[idx]
    if pd.isna(name) or name is None:
        name = df_datasets.table.iloc[idx]

    short_description = df_datasets.short_desc.iloc[idx]
    if pd.isna(short_description) or short_description is None:
        short_description = ""

    description = df_datasets.long_desc.iloc[idx]
    if pd.isna(description) or description is None:
        description = df_datasets.short_desc.iloc[idx]
    if pd.isna(description) or description is None:
        description = df_datasets.source_name.iloc[idx]

    all_text = name + " " + short_description
    if short_description != description:
        all_text += " " + description

    dataset_id = ("ukllc/" + df_datasets.source.iloc[idx].lower() + "_" + df_datasets.table.iloc[idx]).lower()

    ukllc_source_id = df_datasets.source.iloc[idx].lower()

    participant_count = df_datasets.participant_count.iloc[idx]
    if pd.isna(participant_count) or participant_count is None:
        participant_count = 0
    if type(participant_count) is str:
        participant_count = get_clean_sample_size(participant_count)

    start_date = get_clean_start_year(df_datasets.collection_start.iloc[idx])
    end_date = get_clean_end_year(df_datasets.collection_end.iloc[idx])

    if start_date > 0 and end_date > 0:
        duration_years = end_date - start_date
    else:
        duration_years = 0

    if df_datasets.collection_end.iloc[idx] == "ongoing":
        duration_years = datetime.datetime.now().year - start_date

    source = df_datasets.source.iloc[idx]
    dataset_table = df_datasets.table.iloc[idx]

    source_table_lookup = f"{source}_{dataset_table}".lower()
    age_lower = 0
    age_upper = 999
    if source_table_lookup in df_dataset_ages.index:
        ages_row = df_dataset_ages.loc[source_table_lookup]
        if not pd.isna(ages_row.lf):
            age_lower = int(np.round(ages_row.lf))
        if not pd.isna(ages_row.uf):
            age_upper = int(np.round(ages_row.uf))
    else:
        print(f"Could not get min and max age for table {source_table_lookup}")

    url = f"https://explore.ukllc.ac.uk/?source={source}"

    links = df_datasets.links.iloc[idx]
    if not pd.isna(links) and links is not None:
        original_url = re.sub(r'\).*', '', links)
        original_url = re.sub(r'.+\(', '', original_url)
    else:
        original_url = ""

    topics = []
    if not pd.isna(df_datasets.topic_tags.iloc[idx]) and df_datasets.topic_tags.iloc[idx] is not None:
        topics = df_datasets.topic_tags.iloc[idx].split(",")

    variables = dataset_id_to_variables.get(dataset_id, [])

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "UK Longitudinal Linkage Collaboration: UK LLC",
            "url": "https://explore.ukllc.ac.uk"
        }],
    }

    resource = HarmonyResource(
        all_text=all_text,
        age_lower=age_lower,
        age_upper=age_upper,
        country_codes=["GB"],
        country="",
        description=description,
        data_access="",
        dois=[],
        duration_years=duration_years,
        end_year=end_date,
        genetic_data_collected=False,
        geographic_coverage="",
        id=dataset_id,
        instruments=[],
        language_codes=["en"],
        num_sweeps=0,
        num_variables=len(variables),
        original_content="",
        question="",
        response_options=[],
        resource_type=ResourceType.dataset,
        sample_size=participant_count,
        sex=Sex.all,
        source="ukllc",
        start_year=start_date,
        study_design=[],
        name=name,
        keywords=topics,
        urls=[url, original_url],
        variables=variables,
        extra_data_schema=extra_data_schema
    )
    harmony_resources.append(resource)

    if ukllc_source_id not in source_id_to_dataset_resource:
        source_id_to_dataset_resource[ukllc_source_id] = []
    source_id_to_dataset_resource[ukllc_source_id].append(resource)

datasets_included_as_child_of_other_resource = set()

for idx in range(len(df_sources)):

    name = df_sources.source_name.iloc[idx]

    description = df_sources.Aims.iloc[idx]

    variables = []

    this_source = df_sources["source"].iloc[idx]
    source_id = "ukllc/" + this_source.lower()

    sample_size = get_clean_sample_size(df_sources["participant_count"].iloc[idx])
    if pd.isna(sample_size) or sample_size is None:
        sample_size = 0

    start_date = get_clean_start_year(df_sources["Start date"].iloc[idx])
    end_date = 0
    cand_collection_ends = set(df_datasets[df_datasets.source == this_source].collection_end)
    if "ongoing" not in cand_collection_ends:
        for candidate_collection_end_str in cand_collection_ends:
            candidate_collection_end = get_clean_end_year(candidate_collection_end_str)
            if candidate_collection_end > end_date:
                end_date = candidate_collection_end

    duration_years = 0
    if end_date > 0 and start_date > 0:
        duration_years = end_date - start_date
    elif "ongoing" in cand_collection_ends:
        duration_years = datetime.datetime.now().year - start_date

    source = df_sources.source.iloc[idx]

    age_lower, age_upper = get_age_limit(df_sources["Age at recruitment"].iloc[idx])

    url = f"https://explore.ukllc.ac.uk/?source={source}"

    topics = []
    if not pd.isna(df_sources.Themes.iloc[idx]) and df_sources.Themes.iloc[idx] is not None:
        topics = df_sources.Themes.iloc[idx].split(",")

    links = df_sources.Website.iloc[idx]
    if not pd.isna(links) and links is not None:
        original_url = re.sub(r'\).*', '', links)
        original_url = re.sub(r'.+\(', '', original_url)
    else:
        original_url = ""

    datasets_inside_this_source = source_id_to_dataset_resource.get(this_source.lower(), [])
    num_sweeps = len(datasets_inside_this_source)
    num_variables = 0
    for dataset in datasets_inside_this_source:
        num_variables += dataset.num_variables
        datasets_included_as_child_of_other_resource.add(dataset.id)

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "UK Longitudinal Linkage Collaboration: UK LLC",
            "url": "https://explore.ukllc.ac.uk"
        }],
    }

    owner = df_sources["Owner"].iloc[idx]
    if pd.isna(owner) or owner is None:
        owner = ""

    if owner != "":
        extra_data_schema["copyrightHolder"] = [{
            "@type": "Organization",
            "name": owner
        }]

    study_design = clean_study_design("longitudinal " + str(df_sources["Study type"].iloc[idx]))

    countries = df_sources["Geographic coverage - Nations"].iloc[idx]
    if pd.isna(countries) or countries is None:
        countries = ""
    country_codes = get_countries(countries)

    geo = df_sources["Geographic coverage - Regions"].iloc[idx]
    if pd.isna(geo) or geo is None:
        geo = ""

    resource = HarmonyResource(
        all_text=description,
        age_lower=age_lower,
        age_upper=age_upper,
        country_codes=country_codes,
        country=countries,
        description=description,
        data_access="",
        dois=[],
        duration_years=duration_years,
        end_year=end_date,
        genetic_data_collected=False,
        geographic_coverage=geo,
        id=source_id,
        instruments=[],
        language_codes=["en"],
        num_sweeps=num_sweeps,
        num_variables=num_variables,
        original_content="",
        question="",
        response_options=[],
        resource_type=ResourceType.source,
        sample_size=sample_size,
        sex=Sex.all,
        source="ukllc",
        start_year=start_date,
        study_design=study_design,
        name=name,
        keywords=topics,
        urls=[url, original_url],
        variables=datasets_inside_this_source,
        extra_data_schema=extra_data_schema
    )
    harmony_resources.append(resource)

print(f"Initial resource count: {len(harmony_resources)}")
harmony_resources_with_duplicates_removed = []
for resource in harmony_resources:
    if resource.id not in datasets_included_as_child_of_other_resource:
        harmony_resources_with_duplicates_removed.append(resource)
harmony_resources = harmony_resources_with_duplicates_removed
print(f"Resource count after removing top level items which are also children elsewhere: {len(harmony_resources)}")

save_resources(harmony_resources)

variables_resolved_counter = Counter()
for v in variable_to_num_parent_datasets_found.values():
    variables_resolved_counter[v] += 1
print("Variables and number of containing datasets identified: ", variables_resolved_counter)

if len(variables_where_we_could_not_identify_parent_study) > 0:
    VARIABLE_ERRORS_FILE = "/tmp/variables_without_parent_studies.txt"
    print(
        f"There were {len(variables_where_we_could_not_identify_parent_study)} variables where we could not identify the parent study (orphan variables)")
    print(f"These have been written to {VARIABLE_ERRORS_FILE}")
    with open(VARIABLE_ERRORS_FILE, "w", encoding="utf-8") as f:
        for v in variables_where_we_could_not_identify_parent_study:
            f.write(str(v) + "\n")

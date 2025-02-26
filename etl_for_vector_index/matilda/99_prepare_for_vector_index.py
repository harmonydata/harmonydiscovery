import re
import sys

sys.path.append("..")

import pandas as pd

from schema import HarmonyResource, Sex, ResourceType
from util import get_clean_start_year, get_clean_end_year, save_resources, get_age_limit, \
    clean_study_design

df = pd.read_excel("Merged Survey Metadata and Survey Items Data File.xlsx")

harmony_resources = []
variables = []
this_study = None

from collections import Counter

id_ctr = Counter()

for idx in range(len(df)):
    question_text = df.Items.iloc[idx]
    name_year = df["Name_Year"].iloc[idx]
    scale = df["Scale"].iloc[idx]
    item_no = df["Item Number"].iloc[idx]
    name = df["Name"].iloc[idx]
    age_lower, _ = get_age_limit(df["Min Age"].iloc[idx])
    a, b = get_age_limit(df["Max Age"].iloc[idx])
    if a != 0:
        age_upper = a
    elif b != 0:
        age_upper = b
    else:
        age_upper = 0
    data_access = df["Dataset Link"].iloc[idx]
    if pd.isna(data_access):
        data_access = ""
    years = df["Year"].iloc[idx]
    start_year = get_clean_start_year(years)
    end_year = get_clean_end_year(years)
    duration_years = end_year - start_year
    study_design = clean_study_design(df["Study type"].iloc[idx])

    name_year_cleaned = re.sub(r'[^a-z0-9]', '', name_year.lower())
    scale_cleaned = re.sub(r'[^a-z0-9]', '', scale.lower())
    if len(scale_cleaned) > 20:
        scale_cleaned = scale_cleaned[:20]
    item_no_cleaned = re.sub(r'[^a-z0-9]', '', str(item_no).lower())
    variable_id_for_harmony = f"matilda/{name_year_cleaned}/{scale_cleaned}/{item_no_cleaned}".lower()
    study_id_for_harmony = f"matilda/{name_year_cleaned}/{scale_cleaned}".lower()

    id_ctr[variable_id_for_harmony] += 1
    id_ctr[study_id_for_harmony] += 1

    variable_id_for_harmony = f"{variable_id_for_harmony}_{id_ctr[variable_id_for_harmony]}"
    study_id_for_harmony = f"{study_id_for_harmony}_{id_ctr[study_id_for_harmony]}"

    urls = set()
    for column in ["Manual/Guide/Report", "Manual/Guide/Report 2", "Manual/Guide/Report 3", "Manual/Guide/Report 4",
                   "Manual/Guide/Report 5"]:
        u = df[column].iloc[idx]
        if not pd.isna(u) and len(str(u)) > 20:
            urls.add(u)

    variable = HarmonyResource(
        all_text=question_text,
        age_lower=age_lower,
        age_upper=age_upper,
        country_codes=["AU"],
        # country="Australia",
        description=question_text,
        data_access=data_access,
        dois=[],
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=False,
        geographic_coverage="",
        id=variable_id_for_harmony,
        instruments=[],
        language_codes=["en"],
        num_sweeps=0,
        num_variables=0,
        original_content="",
        question="",
        response_options=[],
        resource_type=ResourceType.variable,
        sample_size=0,
        sex=Sex.all,
        source="matilda",
        start_year=start_year,
        study_design=[],
        name=question_text,
        keywords=[],
        urls=[],
        variables=[],
        extra_data_schema={}
    )

    if this_study is None or this_study.name != name:
        this_study = HarmonyResource(
            all_text=name,
            age_lower=0,
            age_upper=999,
            country_codes=["AU"],
            # country="Australia",
            description=name,
            data_access=data_access,
            dois=[],
            duration_years=0,
            end_year=0,
            genetic_data_collected=False,
            geographic_coverage="",
            id=study_id_for_harmony,
            instruments=[],
            language_codes=["en"],
            num_sweeps=0,
            num_variables=0,
            original_content="",
            question="",
            response_options=[],
            resource_type=ResourceType.study,
            sample_size=0,
            sex=Sex.all,
            source="matilda",
            start_year=0,
            study_design=study_design,
            name=name,
            keywords=[],
            urls=sorted(urls),
            variables=[],
            extra_data_schema={}
        )
        harmony_resources.append(this_study)
    this_study.variables.append(variable)

save_resources(harmony_resources)

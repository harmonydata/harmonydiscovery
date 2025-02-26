import hashlib
import json
from collections import Counter

import sys
sys.path.append("..")

from schema import HarmonyResource, Sex, ResourceType
from util import get_age_limit, get_countries, get_clean_start_year, is_natural_language, \
    save_resources, strip_html_tags
from util import get_is_genetic
import numpy as np
import re
harmony_resources = []

with open("intermediate_output/hdruk_all_raw_data.json", "r", encoding="utf-8") as f:
    hdruk_data = json.loads(f.read())

var_counts = []

hdr_var_id_counts = Counter()

for item, item_extra_data_holder in hdruk_data:
    dataset_id = item['datasetid']

    url = f'https://web.www.healthdatagateway.org/dataset/{dataset_id}'

    variables = []
    if len(item_extra_data_holder) > 0:
        item_extra_data = item_extra_data_holder['data']

        for d in item_extra_data["datasetfields"]["technicaldetails"]:

            for element in d["elements"]:

                if "id" in item:
                    parent_id = str(item["id"])
                else:
                    parent_id = str(item["datasetid"])

                hdruk_var_id_before_deduplicating  = "hdruk/" +  parent_id + "/" + hashlib.md5(json.dumps(element).encode()).hexdigest()
                hdr_var_id_counts[hdruk_var_id_before_deduplicating] +=1

                hdruk_var_id = f"{hdruk_var_id_before_deduplicating}_{hdr_var_id_counts[hdruk_var_id_before_deduplicating]}"

                description = element["description"]
                if description is None:
                    description = ""
                label = element["label"]
                if label is None or type(label) is not str:
                    label = ""

                all_text = description + " " + label

                variable = HarmonyResource(
                    age_lower=0,
                    age_upper=0,
                    all_text=all_text,
                    # country="",
                    country_codes=[],
                    data_access="",
                    description=description,
                    dois=[],
                    duration_years=0,
                    end_year=0,
                    genetic_data_collected=False,
                    geographic_coverage="",
                    id=hdruk_var_id,
                    instruments=[],
                    language_codes=[],
                    original_content="",
                    num_variables=0,
                    num_sweeps=0,
                    question="",
                    resource_type=ResourceType.variable,
                    response_options=[],
                    sample_size=0,
                    sex=Sex.all,
                    source="hdruk",
                    start_year=0,
                    study_design=[],
                    name=label,
                    keywords=[],
                    urls=[],
                    variables=[],
                    extra_data_schema={}
                )

                variables.append(variable)

    else:
        item_extra_data = {}

    var_counts.append(len(variables))

    all_text = strip_html_tags(" ".join([x for x in item.values() if is_natural_language(x)]))

    if 'datasetfields' in item and 'abstract' in item['datasetfields']:
        all_text = item['datasetfields']['abstract']

    dois = []
    if "document_links" in item_extra_data:
        for d in item_extra_data['document_links']['doi']:
            dois.append(d)

    geo = re.sub(r'\[|\]|\'', '', str(item['datasetfields']['geographicCoverage']))

    if "ageBand" in item['datasetfields']:
        ages_str = item['datasetfields']['ageBand']
    else:
        ages_str = ""

    age_lower, age_upper = get_age_limit(ages_str)

    item_json = json.dumps(item)
    is_genetic = get_is_genetic(item_json)

    start_year = 0
    end_year = 0
    duration_years = 0
    accessibility = ""

    if "datasetfields" in item_extra_data:
        if "datasetStartDate" in item_extra_data['datasetfields']:
            start_year = get_clean_start_year(item_extra_data['datasetfields']['datasetStartDate'])

        if "datasetEndDate" in item_extra_data['datasetfields']:
            end_year = get_clean_start_year(item_extra_data['datasetfields']['datasetEndDate'])

        if start_year > 0 and end_year > 0:
            duration_years = end_year - start_year

    data_access = ""
    if "datasetv2" in item_extra_data:
        data_access = item_extra_data['datasetv2']['accessibility']['access']['accessService']
        if "documentation" in item_extra_data['datasetv2'] and "description" in item_extra_data[ "datasetv2"]["documentation"] and len(item_extra_data[ "datasetv2"]["documentation"]["description"]) > len(all_text):
            all_text = item_extra_data[ "datasetv2"]["documentation"]["description"]

    if data_access == "" and "datasetfields" in item_extra_data and 'accessRights' in item_extra_data['datasetfields']:
        data_access = item_extra_data['datasetfields']['accessRights']

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "Health Data Research Innovation Gateway",
            "url": "https://www.hdruk.ac.uk/access-to-health-data/"
        }],
    }

    if "datasetfields" in item_extra_data and 'publisher' in item_extra_data['datasetfields']:
        extra_data_schema["publisher"] = [{
            "@type": "Organization",
            "name": item_extra_data['datasetfields']['publisher']
        }]


    resource = HarmonyResource(
        age_lower=age_lower,
        age_upper=age_upper,
        all_text=all_text,
        country_codes=get_countries(geo),
        # country=geo,
        data_access=data_access,
        description=all_text,
        dois=dois,
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=is_genetic,
        geographic_coverage=geo,
        id="hdruk/" + dataset_id,
        instruments= [],
        language_codes=["en"],
        num_variables=0,
        num_sweeps=0,
        original_content=json.dumps([item, item_extra_data_holder], indent=4),
        question="",
        resource_type=ResourceType.dataset,
        response_options=[],
        sample_size=0,
        sex=Sex.all,
        source="hdruk",
        start_year=start_year,
        study_design=[],
        name=item['name'],
        keywords=[],
        urls=[url],
        variables=variables,
        extra_data_schema=extra_data_schema
    )

    harmony_resources.append(resource)

print(max(var_counts), len(var_counts), np.median(var_counts))

save_resources(harmony_resources)

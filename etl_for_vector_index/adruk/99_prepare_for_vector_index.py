import json
import random
import re
from urllib.parse import quote_plus

import sys
sys.path.append("..")

from schema import HarmonyResource, Sex, ResourceType
from util import get_countries, get_clean_start_year, get_dois, is_natural_language, \
    save_resources
from util import get_is_genetic

re_sample_size = re.compile(r'(\d+) (?:participants|respondents|subjects)')

harmony_resources = []
with open("intermediate_output/adruk_datasets_intermediate_data.json", "r", encoding="utf-8") as f:
    adruk_datasets_intermediate = json.loads(f.read())
with open("intermediate_output/adruk_datasets_full_data.json", "r", encoding="utf-8") as f:
    adruk_datasets = json.loads(f.read())

from collections import Counter

id_counter = Counter()

study_id_to_variables = {}

for item in adruk_datasets_intermediate:

    for content in item["content"]:

        if content["searchResultType"] == "DataElement":

            study_id = "adruk/" + content["id"]

            url_tail = f"{content['id']}/{content['originId']}/{content['detail']['dataClassId']}"
            # url = "https://datacatalogue.adruk.org/browser/dataset/" + url_tail

            escaped_term = quote_plus(content['title'])

            url = f"https://datacatalogue.adruk.org/browser/search?searchterm={escaped_term}&include=dataset::datastandard::terminology::dataclass::dataelement"

            data_class_id = content['detail']['dataClassId']

            #             data_class_data = adruk_data_class_to_item[data_class_id]
            abstract = content['abstract']

            summary_title = content["summaryTittle"]

            all_text = summary_title + " " + abstract

            if study_id not in study_id_to_variables:
                study_id_to_variables[study_id] = []
            adruk_id = f"adruk/{url_tail}"

            id_counter[adruk_id] += 1

            adruk_id_unique = f"{adruk_id}_{id_counter[adruk_id]}"

            variable = HarmonyResource(
                age_lower=0,
                age_upper=999,
                all_text=all_text,
                # country="",
                country_codes=[],
                data_access="",
                description=summary_title,
                dois=[],
                duration_years=0,
                end_year=0,
                genetic_data_collected=False,
                geographic_coverage="",
                id=adruk_id_unique,
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
                source="adruk",
                start_year=0,
                study_design=[],
                name=content['title'],
                keywords=[],
                urls=[],
                variables=[],
                extra_data_schema={}
            )

            study_id_to_variables[study_id].append(variable)

            if random.random() < 0.0001:
                print(url)

for item, web_url, item_extra_data in adruk_datasets:
    if item_extra_data is None:
        continue
    id = item["id"]

    all_text = " ".join([x for x in item.values() if is_natural_language(x) or x == "title"]) + " " + " ".join(
        [x for x in item_extra_data.values() if is_natural_language(x)])

    sample_size = 0
    sample_size_matches = re_sample_size.findall(re.sub(r',', '', all_text))
    if len(sample_size_matches) > 0:
        sample_size = int(sample_size_matches[0])

    geo = item_extra_data.get("spatial_coverage", "")

    is_genetic = get_is_genetic(all_text)

    start_year = get_clean_start_year(item_extra_data["start_date"])
    if "end_date" in item_extra_data:
        end_year = get_clean_start_year(item_extra_data["end_date"])
    else:
        end_year = 0
    duration_years = end_year - start_year
    if duration_years < 0:
        duration_years = 0

    study_id = "adruk/" + id

    variables = study_id_to_variables.get(study_id, [])

    description = item_extra_data.get("description", "")

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "ADR UK",
            "url": "https://datacatalogue.adruk.org",
            "image": "https://www.adruk.org/fileadmin/uploads/adruk/Logos/Colour_SVG/ADRUK_-_logo_rgb_rgb_colour.svg"
        }]
    }

    if "publisher" in item:
        extra_data_schema["publisher"] = [{
            "@type": "Organization",
            "name": item["publisher"],
        }]

    if "img" in item:
        extra_data_schema["image"] = item["img"]

    resource = HarmonyResource(
        age_lower=0,
        age_upper=999,
        all_text=all_text,
        country_codes=get_countries(geo),
        # country=geo,
        data_access=item_extra_data.get("access_rights", ""),
        description=description,
        dois=get_dois(all_text),
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=is_genetic,
        geographic_coverage=geo,
        id="adruk/" + id,
        instruments=[],
        language_codes=["en"],
        num_sweeps=0,
        num_variables=0,
        original_content=json.dumps(item, indent=4),
        question="",
        resource_type=ResourceType.dataset,
        response_options=[],
        sample_size=sample_size,
        sex=Sex.all,
        source="adruk",
        start_year=start_year,
        study_design=[],
        name=item["title"],
        keywords=item.get("keywords", []),
        urls=[web_url],
        variables=variables,
        extra_data_schema=extra_data_schema
    )

    harmony_resources.append(resource)

save_resources(harmony_resources)

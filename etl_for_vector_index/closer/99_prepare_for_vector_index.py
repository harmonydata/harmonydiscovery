import json

import pandas as pd

from schema import HarmonyResource, Sex, ResourceType
from util import get_age_limit, get_countries, get_clean_start_year, get_clean_end_year, get_dois, is_natural_language, \
    save_resources, strip_html_tags
from util import get_is_genetic

harmony_resources = []

closer_entities = {}

for closer_entity_type in ['Study', 'Question', 'Variable', 'Organization', 'Data File']:

    closer_entities[closer_entity_type] = {}
    with open(f"intermediate_output/{closer_entity_type}.json", "r", encoding="utf-8") as f:
        file_content = f.read()
    for line in file_content.split("\n"):
        if len(line.strip()) > 0:
            item = json.loads(line)
            agency_id = item["AgencyId"]
            identifier = item["Identifier"]
            tup = (agency_id, identifier)
            closer_entities[closer_entity_type][tup] = item

    print(f"Loaded {len(closer_entities[closer_entity_type])} {closer_entity_type}s")

df = pd.read_csv("intermediate_output/closer_study_relationships.csv")  # TODO: refresh this to most up to date value

lookup_closer_var_to_study = {}
for row in df.itertuples():
    lookup_closer_var_to_study[(row.Agency, row.Identifier)] = (row.source_agency, row.source_identifier)

study_id_to_variables = {}

for entity_type in ["Question", "Variable"]:

    for closer_variable_tup, closer_variable in closer_entities[entity_type].items():

        variable_id = closer_variable["AgencyId"] + "/" + closer_variable["Identifier"]
        closer_id = f"closer/{variable_id}"
        url = f"https://discovery.closer.ac.uk/item/{variable_id}"

        if closer_variable_tup not in lookup_closer_var_to_study:
            print(f"Skipping {closer_variable_tup} because we could not find related study")
            continue
        study_id_tup = lookup_closer_var_to_study[closer_variable_tup]

        study_id = "closer/" + "/".join(study_id_tup)

        if study_id not in study_id_to_variables:
            study_id_to_variables[study_id] = []

        question_text = ""
        if "QuestionText" in closer_variable and "en-GB" in closer_variable["QuestionText"]:
            question_text = closer_variable["QuestionText"]["en-GB"]

        variable_name = ""
        description = ""

        if "Label" in closer_variable and "en-GB" in closer_variable["Label"]:
            description = closer_variable["Label"]["en-GB"]

        if "ItemName" in closer_variable and "en-GB" in closer_variable["ItemName"]:
            variable_name = closer_variable["ItemName"]["en-GB"]

        all_text = (question_text + " " + description).strip()

        if all_text == "":
            all_text = variable_name

        resource_type = ResourceType.variable

        if all_text != "":
            variable = HarmonyResource(
                age_lower=0,
                age_upper=999,
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
                id=closer_id,
                instruments=[],
                language_codes=[],
                original_content="",
                num_variables=0,
                num_sweeps=0,
                question=question_text,
                resource_type=resource_type,
                response_options=[],
                sample_size=0,
                sex=Sex.all,
                source="closer",
                start_year=0,
                study_design=[],
                name=variable_name,
                keywords=[],
                urls=[],
                variables=[],
                extra_data_schema={}
            )

            study_id_to_variables[study_id].append(variable)

for k, v in study_id_to_variables.items():
    print(f"Study {k} has {len(v)} variables")

for item_type in ["Study", "Data File"]:
    for tup, item in closer_entities[item_type].items():

        closer_item_id = item['AgencyId'] + "/" + item['Identifier']
        study_id = "closer/" + closer_item_id

        url = f"https://discovery.closer.ac.uk/item/{closer_item_id}"

        if item_type == "Study":
            all_text = " ".join([x for x in item["StudyAbstract"].values() if is_natural_language(x)])
            sample_size = 0
            resource_type = ResourceType.study
        else:
            all_text = item['DisplayLabel']

            sample_size = item["FileStructure"]["CaseQuantity"]
            resource_type = ResourceType.dataset

        if "<" in all_text:
            all_text = strip_html_tags(all_text)

        item_json = json.dumps(item)
        is_genetic = get_is_genetic(item_json)

        geo = ""

        start_date = 0
        end_date = 0

        if "Coverage" in item:

            if "SpatialCoverage" in item['Coverage'] and "Description" in item['Coverage']["SpatialCoverage"]:
                geo = item['Coverage']["SpatialCoverage"]["Description"].get("en-GB", "")

            if "TemporalCoverage" in item['Coverage'] and "Dates" in item['Coverage']["TemporalCoverage"]:
                for dates_obj in item['Coverage']["TemporalCoverage"]["Dates"]:

                    if "Date" in dates_obj and dates_obj["Date"] is not None:
                        start_date = get_clean_start_year(dates_obj["Date"]["StringValue"])
                        end_date = start_date
                    elif "DateRange" in dates_obj and dates_obj["DateRange"] is not None:
                        start_date = get_clean_start_year(dates_obj["DateRange"]["StartDate"]["StringValue"])
                        end_date = get_clean_end_year(dates_obj["DateRange"]["StartDate"]["StringValue"])

        extra_data_schema = {
            "includedInDataCatalog": [{
                "@type": "DataCatalog",
                "name": "CLOSER Discovery",
                "url": "https://discovery.closer.ac.uk/",
                "image": "https://closer.ac.uk/wp-content/themes/closer-2022/www/images/closer-logo-full.svg"
            }],
        }

        funders_str = ""
        if "FundingSources" in item:
            funders = []
            for funding_source in item["FundingSources"]:
                if "Agency" in funding_source and funding_source["Agency"] is not None:
                    agency_id = funding_source["Agency"]["AgencyId"]
                    identifier = funding_source["Agency"]["Identifier"]
                    tup = (agency_id, identifier)
                    org = closer_entities["Organization"][(agency_id, identifier)]

                    funders.append(org["ItemName"]["en-GB"])
            funders_str = ", ".join(funders)
            if len(funders) > 0:
                extra_data_schema["funder"] = [{
                    "@type": "Organization",
                    "name": f,
                } for f in funders]

        publishers = []
        for pub in item['DublinCoreMetadata']['Publishers']:
            publishers.append(pub["DisplayLabel"])
        if len(publishers) > 0:
            extra_data_schema["publisher"] = [{
                "@type": "Organization",
                "name": p,
            } for p in publishers]
        publishers_str = ", ".join(publishers)

        creators = []
        for creator in item['DublinCoreMetadata']['Creators']:
            creators.append(creator["DisplayLabel"])
        if len(creators) > 0:
            extra_data_schema["creator"] = [{
                "@type": "Organization",
                "name": c,
            } for c in creators]

        age_lower = 0
        age_upper = 999
        if "CustomFields" in item:
            for custom_field in item["CustomFields"]:
                if "en-GB" in custom_field["Title"] and custom_field["Title"]["en-GB"] == "LifeStageDescription":
                    age_lower, age_upper = get_age_limit(custom_field["StringValue"])

        dois = get_dois(item_json)

        duration_years = end_date - start_date
        if duration_years < 0:
            duration_years = 0

        variables = study_id_to_variables.get(study_id, [])

        resource = HarmonyResource(
            all_text=all_text,
            age_lower=age_lower,
            age_upper=age_upper,
            country_codes=get_countries(geo),
            # country=geo,
            description=all_text,
            data_access="",
            dois=dois,
            duration_years=duration_years,
            end_year=end_date,
            genetic_data_collected=is_genetic,
            geographic_coverage=geo,
            id=study_id,
            instruments=[],
            language_codes=["en"],
            num_sweeps=0,
            num_variables=len(variables),
            original_content=json.dumps(item, indent=4),
            question="",
            response_options=[],
            resource_type=resource_type,
            sample_size=sample_size,
            sex=Sex.all,
            source="closer",
            start_year=start_date,
            study_design=[],
            name=item["DisplayLabel"],
            keywords=[],
            urls=[url],
            variables=variables,
            extra_data_schema=extra_data_schema
        )

        harmony_resources.append(resource)

save_resources(harmony_resources)

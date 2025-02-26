import hashlib
import json
import re

import sys
sys.path.append("..")

from schema import HarmonyResource, Sex, ResourceType
from util import numbers_regex, re_exclude, get_age_limit, get_countries, strip_html_tags, get_clean_sample_size, \
    clean_sex, clean_study_design, clean_topics, get_clean_start_year, get_clean_end_year, get_dois, \
    is_natural_language, \
    save_resources, re_url

harmony_resources = []

re_question_filler = re.compile(r"(?i)^(haveyourecently|manyitems|varied)$")

re_question_no_only = re.compile(r'(?i)^\s*q?\d+\s*$')

re_question_yes_no = re.compile(r'(?i)^[a-z]\d*yesnoitems$')


def clean_response_option(response_option):
    response_option = response_option.strip()
    response_option = re.sub(r'(?i)^q?(?:\d+|[a-z])(?:-\d+)?\.?\s+', '', response_option)
    return response_option


with open("intermediate_output/mhc_sweep_instruments.json", "r", encoding="utf-8") as f:
    mhc_instruments = json.loads(f.read())
with open("intermediate_output/mhc_studies.json", "r", encoding="utf-8") as f:
    mhc_studies = json.loads(f.read())

study_id_to_mhc_sweep_instruments = {}
for mhc_sweep_instrument in mhc_instruments:
    if mhc_sweep_instrument["study_id"] not in study_id_to_mhc_sweep_instruments:
        study_id_to_mhc_sweep_instruments[mhc_sweep_instrument["study_id"]] = []
    study_id_to_mhc_sweep_instruments[mhc_sweep_instrument["study_id"]].append(mhc_sweep_instrument)

study_id_to_variables = {}

variable_ids_used = set()

for mhc_sweep_instrument in mhc_instruments:

    sweep_id = mhc_sweep_instrument["sweep_id"]

    if mhc_sweep_instrument["topic"] is not None:
        topic = mhc_sweep_instrument["topic"]

        topics = [t.strip() for t in topic.split(",")]
    else:
        topics = []

    reporting_term = mhc_sweep_instrument["reporting_term"]
    if reporting_term is not None:
        reporting_term = reporting_term.lower()
    else:
        reporting_term = ""
    informant = mhc_sweep_instrument["informant"]
    if informant is not None:
        informant = informant.lower()
    else:
        informant = ""

    response_options = mhc_sweep_instrument["response_scale"].split("<br>")
    response_options = [clean_response_option(r) for r in response_options if
                        r.strip() != "" and len(re_question_no_only.findall(r)) == 0]

    study_id = "mhc/" + mhc_sweep_instrument["study_id"].lower()

    if study_id not in study_id_to_variables:
        study_id_to_variables[study_id] = []

    age_lower, age_upper = get_age_limit(mhc_sweep_instrument["sweep_id"])
    if age_lower == 0 and age_upper == 999 and "focus" in mhc_sweep_instrument:
        age_lower, age_upper = get_age_limit(mhc_sweep_instrument["focus"])

    if age_lower > 0 and age_upper == 999:
        age_upper = age_lower

    start_year = get_clean_start_year(mhc_sweep_instrument["sweep_id"])
    end_year = get_clean_end_year(mhc_sweep_instrument["sweep_id"])

    instrument_name = mhc_sweep_instrument["scale"]

    for question_line in mhc_sweep_instrument["questions"].split("<br>"):
        if len(question_line) > 0:
            question_number = None
            question_text = question_line
            matches = numbers_regex.findall(question_line)
            if matches:
                question_number = re.sub(r'\W', '', matches[0])
                question_text = numbers_regex.sub("", question_line).strip()

            question_text = question_text.strip()

            question_text_only_alpha = re.sub(r'[^a-z]', '', question_text.lower())

            if question_text == "" or len(re_question_filler.findall(question_text_only_alpha)) or len(
                    re_exclude.findall(question_text)) > 0:
                continue

            if len(re_question_yes_no.findall(re.sub(r'[^a-z0-9]', '', question_text.lower()))) > 0:
                continue

            variable_id = study_id + "/" + sweep_id.lower() + "/" + hashlib.md5(
                question_text.lower().strip().encode()).hexdigest()
            if variable_id in variable_ids_used:
                for extra_digit in range(2, 1000):
                    tmp_variable_id = f"{variable_id}_{extra_digit}"
                    if tmp_variable_id not in variable_ids_used:
                        variable_id = tmp_variable_id
                        break
            variable_ids_used.add(variable_id)

            all_text = question_text + " " + " ".join(response_options)

            variable = HarmonyResource(
                age_lower=age_lower,
                age_upper=age_upper,
                all_text=all_text,
                # country="",
                country_codes=[],
                data_access="",
                description=question_text,
                dois=[],
                duration_years=0,
                end_year=end_year,
                genetic_data_collected=False,
                geographic_coverage="",
                id=variable_id,
                instruments=[instrument_name],
                language_codes=[],
                original_content="",
                num_variables=0,
                num_sweeps=0,
                question=question_text,
                resource_type=ResourceType.variable,
                response_options=response_options,
                sample_size=0,
                sex=Sex.all,
                source="mhc",
                start_year=start_year,
                study_design=[],
                name=question_text,
                keywords=topics,
                urls=[],
                variables=[],
                extra_data_schema={}
            )

            study_id_to_variables[study_id].append(variable)

# De-duplicate variables

print("Deduplicating sweep variables...")

for study_id, variables_in_this_study in list(study_id_to_variables.items()):

    print(f"\tStudy {study_id} has {len(variables_in_this_study)} variables before deduplication")
    deduplicated_variables = []

    all_text_to_variables_in_study = {}

    for variable in variables_in_this_study:
        if variable.all_text not in all_text_to_variables_in_study:
            all_text_to_variables_in_study[variable.all_text] = []
        all_text_to_variables_in_study[variable.all_text].append(variable)

    for all_text, variables_matching_this_text_within_this_study in all_text_to_variables_in_study.items():
        if len(variables_matching_this_text_within_this_study) == 1:
            deduplicated_variables.append(variables_matching_this_text_within_this_study[0])
        else:
            min_year = min([v.start_year for v in variables_matching_this_text_within_this_study])
            max_year = max([v.end_year for v in variables_matching_this_text_within_this_study])
            min_age = min([v.age_lower for v in variables_matching_this_text_within_this_study])
            max_age = max([v.age_upper for v in variables_matching_this_text_within_this_study])

            max_duration = max([v.duration_years for v in variables_matching_this_text_within_this_study])
            all_instruments = set()
            for v in variables_matching_this_text_within_this_study:
                for i in v.instruments:
                    all_instruments.add(i)
            variables_matching_this_text_within_this_study[0].instruments = list(all_instruments)

            all_topics = set()
            for v in variables_matching_this_text_within_this_study:
                for t in v.keywords:
                    all_topics.add(t)
            variables_matching_this_text_within_this_study[0].keywords = list(all_topics)

            if max_year > 0 and min_year > 0:
                variables_matching_this_text_within_this_study[0].duration_years = max_year - min_year

            variables_matching_this_text_within_this_study[0].start_year = min_year
            variables_matching_this_text_within_this_study[0].end_year = max_year
            variables_matching_this_text_within_this_study[0].age_lower = min_age
            variables_matching_this_text_within_this_study[0].age_upper = max_age
            variables_matching_this_text_within_this_study[0].duration_years = max_duration
            variables_matching_this_text_within_this_study[0].num_sweeps = len(
                variables_matching_this_text_within_this_study)

            deduplicated_variables.append(variables_matching_this_text_within_this_study[0])

    print(f"\t\tStudy {study_id} has {len(deduplicated_variables)} variables after deduplication")
    study_id_to_variables[study_id] = deduplicated_variables

for study in mhc_studies:

    original_url = ""

    for match in re_url.findall(study['website']):
        original_url = match
        break

    age_lower, age_upper = get_age_limit(study["age_at_recruitment"])

    all_text = " ".join([x for x in study.values() if is_natural_language(x)])
    dois = get_dois(all_text)

    all_text_clean = strip_html_tags(all_text)

    start_year = get_clean_start_year(study["start_date"])

    sweep_instruments = study_id_to_mhc_sweep_instruments.get(study["study_id"], [])

    end_year = 0  # TODO: get this from sweeps - or maybe latest data?

    instruments = set()
    sweeps = set()
    for instr in sweep_instruments:
        e = get_clean_end_year(instr["sweep_id"])
        if e > end_year:
            end_year = e
        if instr["scale"] is not None and len(instr["scale"]) > 1:
            instruments.add(instr["scale"].strip())
        if instr["sweep_id"] is not None and len(instr["sweep_id"]) > 1:
            sweeps.add(instr["sweep_id"].strip())
    instruments = list(instruments)

    duration_years = 0  # TODO

    if end_year > 0 and start_year > 0:
        duration_years = end_year - start_year

    study_id = f"mhc/{study['study_id'].lower()}"

    variables = study_id_to_variables.get(study_id, [])

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "Catalogue of Mental Health Measures",
            "url": "https://www.cataloguementalhealth.ac.uk/",
            "image": "https://www.cataloguementalhealth.ac.uk/img/sitelogo.png"
        }],
    }

    if "institution" in study and study["institution"] != "" and study["institution"] is not None:
        extra_data_schema["publisher"] = [{
            "@type": "Organization",
            "name": study["institution"]
        }]

    if "funders" in study and study["funders"] != "" and study["funders"] is not None:
        extra_data_schema["funder"] = [{
            "@type": "Organization",
            "name": study["funders"]
        }]

    extra_data_schema["image"] = f"https://www.cataloguementalhealth.ac.uk/img/studies/{study['study_id']}.png"

    resource = HarmonyResource(
        age_lower=age_lower,
        age_upper=age_upper,
        all_text=all_text_clean,
        country_codes=get_countries(study["geographic_coverage_nations"] + " " + study["geographic_coverage"]),
        # country=study["geographic_coverage"],
        data_access=study['data_access'],
        description=strip_html_tags(study['aims']),
        dois=dois,
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=study["genetic_data_collected"],
        geographic_coverage=study["geographic_coverage_nations"],
        id=study_id,
        instruments=instruments,
        language_codes=["en"],
        original_content=json.dumps(study, indent=4),
        num_variables=len(variables),
        num_sweeps=len(sweeps),
        question="",
        response_options=[],
        resource_type=ResourceType.study,
        sample_size=get_clean_sample_size(study['sample_size_at_recruitment']),
        sex=clean_sex(study["sex"]),
        source="mhc",
        start_year=start_year,
        study_design=clean_study_design(f"{study['study_design']} {study['sample_characteristics']}"),
        name=study["title"],
        keywords=clean_topics(study["related_themes"]),
        urls=[f"https://www.cataloguementalhealth.ac.uk/?content=study&studyid={study['study_id']}", original_url],
        variables=variables,
        extra_data_schema=extra_data_schema
    )

    harmony_resources.append(resource)

save_resources(harmony_resources)

import datetime
import json
import operator
import re
import sys
from collections import Counter
from typing import List

from bs4 import BeautifulSoup

from schema import Sex, HarmonyResource

sys.path.append("/home/thomas/projects_internal/language_named_entity_recognition")

from language_named_entity_recognition.language_finder import find_languages

re_word = re.compile(r'(\w+)')

import re
import country_named_entity_recognition

re_exclude = re.compile(r'(?i)^if \d\w*(?: and \d\w*)? = (?:yes|no).?$')
numbers_regex = re.compile(r'(?i)^q?\d+[abcdefg]?\.?\W')

re_only_num = re.compile(r'^\d+$')
re_num = re.compile(r'(\d+)')
re_age = re.compile(r'(\b\d\d?\b)')
re_year = re.compile(r'((?:19|20)\d\d)')

re_url = re.compile(
    "(https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*))")


def get_is_genetic(text):
    text = text.lower()
    if "genetic" in text or "genomic" in text:
        return True
    return False


def get_age_limit(age_str: str):
    age_str = str(age_str).lower()
    numbers_present = re_age.findall(age_str)

    if "month" in age_str:
        return 0, 0

    if "birth" in age_str:
        age_lower = 0
        if len(numbers_present) > 0:
            age_upper = numbers_present[0]
        else:
            age_upper = 0
    else:
        if len(numbers_present) == 2:
            age_lower = int(numbers_present[0])
            age_upper = int(numbers_present[1])
        elif len(numbers_present) == 1:
            age_lower = int(numbers_present[0])
            age_upper = 999
        else:
            age_lower = 0
            age_upper = 999
    return age_lower, age_upper


def clean_topics(joined_topics: str) -> List:
    return [x.strip() for x in joined_topics.split(",")]


def clean_sex(orig_sex: str):
    orig_sex = str(orig_sex).lower()
    is_female = False
    is_male = False
    if "female" in orig_sex or "women" in orig_sex or "woman" in orig_sex or "girl" in orig_sex:
        is_female = True
    if "male" in orig_sex or "men" in orig_sex or "man" in orig_sex or "boy" in orig_sex:
        is_male = True
    if is_female and not is_male:
        return Sex.female
    elif is_male and not is_female:
        return Sex.male
    return Sex.all


def get_clean_sample_size(sample_size_text: str):
    sample_size_text = re.sub(r',', '', str(sample_size_text))

    numbers_present = re_num.findall(sample_size_text)

    if len(numbers_present) > 0:
        return int(numbers_present[0])

    return 0


def get_clean_start_year(start_year_text: str):
    numbers_present = re_year.findall(str(start_year_text))

    if len(numbers_present) > 0:
        return int(numbers_present[0])

    return 0


def get_clean_end_year(start_year_text: str):
    numbers_present = re_year.findall(str(start_year_text))

    if len(numbers_present) > 0:
        return int(numbers_present[-1])

    return 0


re_doi = re.compile(r'(?:https?://)?(?:dx.)?doi.org.+?(?:\"|\'|<|$)')


def get_dois(all_text: str):
    dois = re_doi.findall(str(all_text))

    if len(dois) > 0:
        return [re.sub(r'"|\'', '', doi) for doi in dois]

    return []


re_uk_nations = re.compile(r'(?i)\b(?:england|scotland|northern ireland|wales)\b')


def get_countries(location_desc: str):
    location_desc = str(location_desc)

    countries = []
    for country in country_named_entity_recognition.find_countries(location_desc):
        countries.append(country[0].alpha_2)

    if len(re_uk_nations.findall(location_desc)) > 0 or len(countries) == 0:
        countries.append("GB")

    countries = sorted(set(countries))

    return countries


def strip_html_tags(html_str):
    soup = BeautifulSoup(html_str)

    return soup.get_text().strip()


def is_natural_language(field_content):
    if type(field_content) is not str:
        return False
    if len(re_only_num.findall(field_content)) > 0:
        return False
    if field_content.startswith("http") and len(field_content) < 100:
        return False
    return True


def get_languages(text: str):
    text = str(text)

    tokens = re_word.findall(text)

    found = set()
    for lang in find_languages(tokens):
        found.add(lang[0][0])

    if len(found) == 0:
        found.add("en")

    return list(found)


def clean_study_design(study_design_description):
    study_design_norm = re.sub("[- _]", " ", study_design_description.lower())

    kws_found = set()

    for k in "ageing cohort/occupational/geo/birth/cohort/longitudinal/survey/observational/descriptive/household/prospective/retrospective/case control/experimental/historical/time series/geospatial/twin/census/administrative/pregnancy/mixed methods/qualitative/panel/registry/linked/cross sectional".split(
            "/"):
        if k in study_design_norm:
            kws_found.add(k)

    return list(kws_found)


def save_resources(harmony_resources: List[HarmonyResource]):
    """

    :param harmony_resources: The list of resources should not contain duplicate IDs.
    A warning will be issued if there are.
    The calling function should ensure all items are unique.
    """
    print(f"Saving {len(harmony_resources)} resources to disk...")

    num_empty_texts = 0
    for resource in harmony_resources:
        if resource.all_text == "":
            num_empty_texts += 1
    print(f"Number of objects with all_text empty: {num_empty_texts}")

    print("Checking IDs are all unique...")
    id_counter = Counter()
    for resource in harmony_resources:
        id_counter[resource.id] += 1
        for variable in resource.variables:
            id_counter[variable.id] += 1
    if max(id_counter.values()) > 1:
        date_stamp = datetime.datetime.now().strftime("%Y%b%d_%H_%M")
        duplicate_ids_file = f"/tmp/{date_stamp}_duplicate_ids.txt"
        print(f"Warning! Duplicate IDs found. They will be written to {duplicate_ids_file}")
        with open(duplicate_ids_file, "w", encoding="utf-8") as f:
            for k, v in sorted(id_counter.items(), key=operator.itemgetter(1), reverse=True):
                if v > 1:
                    f.write(f"{k}\t{v}\n")
    else:
        print("Yay! The IDs are unique")

    with open("resources_for_vector_index.json", "w", encoding="utf-8") as f:
        for resource in harmony_resources:
            resource_dict = json.loads(resource.model_dump_json())
            f.write(json.dumps(resource_dict) + "\n")


print(get_age_limit("age 24 (2009)"))

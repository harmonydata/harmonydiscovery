import re
import sys
from collections import Counter

sys.path.append("..")
import pandas as pd

from schema import HarmonyResource, Sex, ResourceType
from util import get_clean_start_year, save_resources

re_non_alpha = re.compile(r'(?i)[^a-z]')

harmony_resources = []

hardcoded_study_id_to_url = {
    '1970_bcs': 'https://cls.ucl.ac.uk/cls-studies/1970-british-cohort-study/',
    'mcs': 'https://cls.ucl.ac.uk/cls-studies/millennium-cohort-study/',
    '1958_ncds': 'https://cls.ucl.ac.uk/cls-studies/1958-national-child-development-study/',
    'next_steps': 'https://cls.ucl.ac.uk/cls-studies/next-steps/'
}


def get_study_url(study_id):
    return hardcoded_study_id_to_url.get(study_id, 'https://cls.ucl.ac.uk/data-access-training/exploring-our-data/')


df = pd.read_excel("intermediate_output/cls_metadata_for_datatables_harmony.xlsx")

df.dropna(subset=["study"], inplace=True)

df["study_id"] = df["study"].apply(lambda x: re.sub(r' ', '_', x.lower()))

df["harmony_identifier"] = "cls/" + df["study_id"] + "/" + df["sweep-year-age"] + "/" + df["variable_name"]

identifier_counter = Counter()
orig_identifiers = list(df["harmony_identifier"])
for idx in range(len(orig_identifiers)):
    id = orig_identifiers[idx]
    identifier_counter[id] += 1
    orig_identifiers[idx] = f"{id}_{identifier_counter[id]}"
df["harmony_identifier"] = orig_identifiers

# In[50]:


for study_id, subset in df.groupby("study_id"):

    variables = []

    years_found = set()

    topics_seen = set()

    all_question_texts = []

    for idx in range(len(subset)):
        cls_var_identifier = subset.harmony_identifier.iloc[idx]

        question_text = df["harmony_question"].iloc[idx]

        if not pd.isna(question_text):
            alpha_chars_in_question = re_non_alpha.sub("", question_text)
            if len(alpha_chars_in_question) == 0:
                question_text = ""

        if pd.isna(question_text) or question_text == "":
            question_text = df["variable_label"].iloc[idx]
            question_text = re.sub(r'^\d+\:', '', question_text).strip()

        all_question_texts.append(question_text)

        question_no = df["variable_name"].iloc[idx]

        topic = df["research_topic_subtopic"].iloc[idx]
        if not pd.isna(topic) and topic is not None:
            topics = [topic]

            topics_seen.add(topic)

        #         question_options = df["question_options   (added for Harmony)"].iloc[idx]
        #         if pd.isna(question_options):
        #             question_options = []
        #         else:
        #             question_options = [re.sub(r'^(?:\(|\)|\d|-)+', '', x).strip() for x in question_options.split("(")]
        #             if '' in question_options:
        #                 question_options.remove('')

        sweep_id = df["sweep-year-age"].iloc[idx]

        y = get_clean_start_year(re.sub(r'_', ' ', sweep_id))
        if y != 0:
            years_found.add(y)

        variable_name = df["variable_name"].iloc[idx]
        variable_label = df["variable_label"].iloc[idx]

        all_text = question_text
        if all_text == "":
            all_text = variable_name
        if all_text == "":
            all_text = variable_label

        variable = HarmonyResource(
            age_lower=0,
            age_upper=0,
            all_text=question_text,
            # country="",
            country_codes=[],
            data_access="",
            description=variable_label,
            dois=[],
            duration_years=0,
            end_year=0,
            genetic_data_collected=False,
            geographic_coverage="",
            id=cls_var_identifier,
            instruments=[],
            language_codes=[],
            original_content="",
            num_variables=0,
            num_sweeps=0,
            question=question_text,
            resource_type=ResourceType.variable,
            response_options=[],
            sample_size=0,
            sex=Sex.all,
            source="cls",
            start_year=0,
            study_design=[],
            name=variable_name,
            keywords=topics,
            urls=[],
            variables=[],
            extra_data_schema={}
        )

        variables.append(variable)

    start_year = 0
    end_year = 0
    duration_years = 0
    if len(years_found) > 0:
        start_year = min(years_found)
        end_year = min(years_found)
        duration_years = end_year - start_year

    study_name = subset.study.iloc[0]

    all_questions_joined = " ".join(all_question_texts)

    all_text_clean = all_questions_joined

    study_url = get_study_url(study_id)

    resource = HarmonyResource(
        all_text=all_text_clean,
        age_lower=0,
        age_upper=999,
        country_codes=["UK"],
        # country="UK",
        description=all_questions_joined,
        data_access="",
        dois=[],
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=False,
        geographic_coverage="",
        id="cls/" + study_id,
        instruments=[],
        language_codes=["en"],
        original_content="",
        num_variables=0,
        num_sweeps=0,
        question="",
        resource_type=ResourceType.dataset,
        response_options=[],
        sample_size=0,
        sex=Sex.all,
        source="cls",
        start_year=start_year,
        study_design=["longitudinal"],
        name=study_name,
        keywords=list(topics_seen),
        urls=[study_url],
        variables=variables,
        extra_data_schema={
            "includedInDataCatalog": [{
                "@type": "DataCatalog",
                "name": "UCL Centre for Longitudinal Studies",
                "url": "https://cls.ucl.ac.uk/"
            }],
        }
    )

    harmony_resources.append(resource)

save_resources(harmony_resources)

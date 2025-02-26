import json
import re
import sys

sys.path.append("..")

from schema import HarmonyResource, Sex, ResourceType
from util import clean_study_design, clean_sex, get_clean_sample_size, strip_html_tags
from util import get_countries, get_clean_start_year, get_clean_end_year, get_dois, is_natural_language, \
    save_resources
from util import get_is_genetic

harmony_resources = []
with open("intermediate_output/ukds_all_raw_data.json", "r", encoding="utf-8") as f:
    ukds_data = json.loads(f.read())

for item, item_extra_data in ukds_data:

    id = item['id']

    url = f'https://web.www.healthdatagateway.org/dataset/{id}'

    doi = ""

    if item_extra_data is not None and "doi" in item_extra_data and item_extra_data["doi"] is not None:
        doi = item_extra_data["doi"]
        if "/" in doi and not "doi" in doi:
            doi = f"http://dx.doi.org/{doi}"

    if doi == "" and item_extra_data is not None and "persistentIdentifier" in item_extra_data and item_extra_data[
        "persistentIdentifier"] is not None:
        doi = item_extra_data["persistentIdentifier"]
        if "/" in doi and not "doi" in doi:
            doi = f"http://dx.doi.org/{doi}"

    if doi != "":
        dois = [doi]
    else:
        dois = []

    item_json = json.dumps(item)
    is_genetic = get_is_genetic(item_json)

    geo = ""
    if item_extra_data is not None and "geogCover" in item_extra_data:
        geo = item_extra_data.get("geogCover", "")
        if geo is None:
            geo = ""
    if type(geo) is list:
        geo = " ".join(geo)

    desc = item.get("abstract", [""])[0]

    all_text = " ".join([x for x in item.values() if is_natural_language(x)]) + " " + desc

    if geo != "":
        country_codes = get_countries(geo)
    else:
        country_codes = get_countries(all_text)

    if doi == "":
        dois = get_dois(all_text)

    all_text_clean = strip_html_tags(all_text)
    desc = strip_html_tags(desc)

    orig_url = item.get("externalLink", "")
    if orig_url is None:
        orig_url = ""

    topics = []

    if item_extra_data is not None and "keywordHyperlink" in item_extra_data and item_extra_data[
        "keywordHyperlink"] is not None:
        for hl in item_extra_data["keywordHyperlink"]:
            hl = re.sub(r'<a name="', '', hl)
            hl = re.sub(r'".+', '', hl).lower()
            topics.append(hl)

    data_access = ""
    sample_size = 0
    sex = Sex.all
    copyright = ""
    if item_extra_data is not None:
        if "restrictions" in item_extra_data and item_extra_data[
            "restrictions"] is not None:
            data_access = item_extra_data["restrictions"]

        if "catalogueMetadata" in item_extra_data and "access" in item_extra_data["catalogueMetadata"] and \
                item_extra_data[
                    "catalogueMetadata"] is not None and "copyright" in item_extra_data["catalogueMetadata"]["access"]:
            data_access = item_extra_data["catalogueMetadata"]["access"]["copyright"]

        if "collMode" in item_extra_data and item_extra_data["collMode"] is not None:
            sample_size = get_clean_sample_size(item_extra_data["collMode"])

            # if sample_size == 0:
            #    sample_size = get_clean_sample_size(all_text_clean)

            sex = clean_sex(item_extra_data["collMode"])

        if item_extra_data is not None and "copyright" in item_extra_data:
            copyright = ", ".join(item_extra_data["copyright"])

    study_designs = []
    if item_extra_data is not None and "genericMetadata" in item_extra_data and item_extra_data[
        "genericMetadata"] is not None:
        for t in item_extra_data["genericMetadata"]["dataTypeList"]:
            study_designs.append(t["dataTypeValue"])
    study_design = ", ".join(study_designs)

    extra_data_schema = {
        "includedInDataCatalog": [{
            "@type": "DataCatalog",
            "name": "UK Data Service",
            "url": "https://ukdataservice.ac.uk/"
        }],
    }

    if item_extra_data is not None and "sponsor" in item_extra_data:
        extra_data_schema["sponsor"] = [{"@type": "Organization", "name": sponsor_name} for sponsor_name in
                                        item_extra_data["sponsor"]]

    start_year = 0
    end_year = 0
    if item_extra_data is not None and "collDate" in item_extra_data and item_extra_data[
        "collDate"] is not None and len(item_extra_data["collDate"]) > 0:
        start_year = get_clean_start_year(item_extra_data["collDate"][0])
        end_year = get_clean_end_year(item_extra_data["collDate"][-1])

        extra_data_schema["temporalCoverage"] = re.sub(r'T\d.+$', '',
                                                       item_extra_data["collDate"][0]) + "/" + re.sub(r'T\d.+$',
                                                                                                      '',
                                                                                                      item_extra_data[
                                                                                                          "collDate"][
                                                                                                          -1])

    if start_year > 0 and end_year > 0:
        duration_years = end_year - start_year
    else:
        duration_years = 0

    resource = HarmonyResource(
        all_text=all_text_clean,
        age_lower=0,
        age_upper=999,
        country_codes=country_codes,
        # country=geo,
        description=desc,
        data_access=data_access,
        dois=dois,
        duration_years=duration_years,
        end_year=end_year,
        genetic_data_collected=is_genetic,
        geographic_coverage=geo,
        id="ukds/" + id,
        instruments=[],
        language_codes=["en"],
        num_variables=0,
        num_sweeps=0,
        original_content=json.dumps([item, item_extra_data], indent=4),
        question="",
        resource_type=ResourceType.dataset,
        response_options=[],
        sample_size=sample_size,
        sex=sex,
        source="ukds",
        start_year=start_year,
        study_design=clean_study_design(study_design),
        name=item['title'],
        keywords=topics,
        urls=[url, orig_url],
        variables=[],  # I can't see variables on UKDS platform
        extra_data_schema=extra_data_schema
    )

    harmony_resources.append(resource)

save_resources(harmony_resources)

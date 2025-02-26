import json

from tqdm import tqdm

from schema import Sex, HarmonyResource

harmony_resources = []

with open("merged_resources.json", "r", encoding="utf-8") as f:
    for l in f:
        d = json.loads(l)
        r = HarmonyResource(**d)
        r.variables = [HarmonyResource(**v) for v in d["variables"]]
        harmony_resources.append(r)

ctr_parent = 0
ctr_child = 0
with open("all_resources_for_weaviate.json", "w", encoding="utf-8") as f:
    for resource in tqdm(harmony_resources):
        resource_dict = json.loads(resource.model_dump_json())

        # Delete all fields that are in Pydantic but which we don't want in Weaviate (applicable only to Study)
        del resource_dict["variables"]
        del resource_dict["extra_data_schema"]
        # del resource_dict["response_options"]
        # del resource_dict["question"]

        this_schema = {"@context": "https://schema.org/",
                       "@type": "Dataset",
                       "name": resource.name,
                       "description": resource.description,
                       "url": resource.urls,
                       "keywords": resource.keywords,
                       "identifier": resource.dois}

        if len(resource.variables) > 0:
            this_schema["variableMeasured"] = []
        for variable in resource.variables:
            desc = variable.question

            variable_for_schema_org_representation = {"name": variable.name}
            if desc != variable.name and desc != "":
                variable_for_schema_org_representation["description"] = desc  #

            if len(variable.urls) > 0:
                variable_for_schema_org_representation["url"] = variable.urls
            # Make sure no blank values in the representation
            if variable_for_schema_org_representation["name"] == "":
                variable_for_schema_org_representation["name"] = desc
                if "description" in variable_for_schema_org_representation:
                    del variable_for_schema_org_representation["description"]
            this_schema["variableMeasured"].append(variable_for_schema_org_representation)
        if len(resource.dois) > 0:
            this_schema["identifier"] = resource.dois

        for k, v in resource.extra_data_schema.items():
            this_schema[k] = v

        if "temporalCoverage" not in this_schema:
            if resource.start_year > 0 and resource.end_year > 0:
                this_schema["temporalCoverage"] = f"{resource.start_year}..{resource.end_year}"
            elif resource.start_year > 0:
                this_schema["temporalCoverage"] = f"{resource.start_year}.."

        if "size" not in this_schema and resource.sample_size > 0:
            this_schema["size"] = str(resource.sample_size)

        resource_dict["schema"] = json.dumps(this_schema, ensure_ascii=False)

        f.write(json.dumps(resource_dict) + "\n")
        ctr_parent += 1

        # Copy useful values into child variable from study
        for variable in resource.variables:
            if variable.age_lower == 0 and resource.age_lower > 0:
                variable.age_lower = resource.age_lower
            if variable.start_year == 0 and resource.start_year > 0:
                variable.start_year = resource.start_year
            if variable.end_year == 0 and resource.end_year > 0:
                variable.end_year = resource.end_year
            if variable.sample_size == 0 and resource.sample_size > 0:
                variable.sample_size = resource.sample_size
            if variable.duration_years == 0 and resource.duration_years > 0:
                variable.duration_years = resource.duration_years
            if (variable.age_upper == 0 or variable.age_upper == 999) and resource.age_upper > 0:
                variable.age_upper = resource.age_upper
            if variable.country_codes == [] and len(resource.country_codes) > 0:
                variable.country_codes = resource.country_codes
            if variable.geographic_coverage == "" and resource.geographic_coverage != "":
                variable.geographic_coverage = resource.geographic_coverage
            if variable.keywords == [] and len(resource.keywords) > 0:
                variable.keywords = resource.keywords
            # if variable.url == "" and resource.url != "":
            #     variable.url = resource.url
            # if variable.original_source_url == "" and resource.original_source_url != "":
            #     variable.original_source_url = resource.original_source_url
            # if variable.owner == "" and resource.owner != "":
            #     variable.owner = resource.owner
            if variable.sex == Sex.all and resource.sex != Sex.all:
                variable.sex = resource.sex
            if variable.study_design == [] and len(resource.study_design) > 0:
                variable.study_design = resource.study_design
            if variable.num_sweeps == 0 and resource.num_sweeps > 0:
                variable.num_sweeps = resource.num_sweeps
            if variable.num_variables == 0 and resource.num_variables > 0:
                variable.num_variables = resource.num_variables
            if variable.data_access == "" and resource.data_access != "":
                variable.data_access = resource.data_access

        for variable in resource.variables:
            resource_dict = json.loads(variable.model_dump_json())

            del resource_dict["variables"]
            # del resource_dict["dois"]
            # del resource_dict["genetic_data_collected"]
            # del resource_dict["geographic_coverage"]
            # del resource_dict["num_variables"]
            # del resource_dict["urls"]
            del resource_dict["extra_data_schema"]
            # del resource_dict["data_access"]
            # del resource_dict["country"]
            #
            # resource_dict["study_variable_relationship"] = {
            #     "name": "variable",
            #     "parent": resource.id
            # }

            resource_dict["parent_id"] = resource.id

            resource_dict["schema"] = ""

            f.write(json.dumps(resource_dict) + "\n")
            ctr_child += 1
print(f"Saved {ctr_parent} parent entities (i.e. studies) and {ctr_child} child entities (i.e. variables) to disk")

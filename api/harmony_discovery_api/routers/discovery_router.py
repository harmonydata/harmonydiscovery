"""
MIT License

Copyright (c) 2023 Ulster University (https://www.ulster.ac.uk).
Project: Harmony (https://harmonydata.ac.uk)
Maintainer: Thomas Wood (https://fastdatascience.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import json
import os
from enum import Enum
from typing import List

from fastapi import APIRouter, status, Query
# from fastapi_cache.decorator import cache
from weaviate.classes.query import Filter
from weaviate.connect import ConnectionParams

from harmony_discovery_api.schemas.schema import CountryCode, LanguageCode, ResourceType, Sex
from harmony_discovery_api.schemas.search import SearchResponse, SearchResult, MatchType


# @router.on_event("startup")
# async def startup():
#    redis = aioredis.from_url("redis://localhost", encoding="utf8", decode_responses=True)
#    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")


class SortOrder(str, Enum):
    best_match: str = 'best_match'
    recent: str = 'recent'
    age_ascending: str = 'age_ascending'
    age_descending: str = 'age_descending'
    sample_size_ascending: str = 'sample_size_ascending'
    sample_size_descending: str = 'sample_size_descending'
    duration_ascending: str = 'duration_ascending'
    duration_descending: str = 'duration_descending'
    num_variables_ascending: str = 'num_variables_ascending'
    num_variables_descending: str = 'num_variables_descending'
    num_sweeps_ascending: str = 'num_sweeps_ascending'
    num_sweeps_descending: str = 'num_sweeps_descending'


router = APIRouter(prefix="/discover")

# BEGIN ELASTIC SEARCH

# pip install weaviate-client
# pip install weaviate-client-4.11.0b2
# pip install weaviate-client
from weaviate.classes.query import MetadataQuery
from weaviate.classes.init import Auth
import weaviate
from weaviate.collections.classes.grpc import QueryReference
from weaviate.classes.query import Metrics

# Best practice: store your credentials in environment variables
# weaviate_url = os.environ["WEAVIATE_URL"]
# weaviate_api_key = os.environ["WEAVIATE_API_KEY"]
weaviate_url = "https://mkokqxzqf6a0fdxdipgpg.c0.europe-west3.gcp.weaviate.cloud"
weaviate_api_key = "smIylzaaUYCDBSbKckLWZjcpSnOMToeNzdAJ"

es_dict = {"es": None}

# END ELASTIC SEARCH
# BEGIN HUGGING FACE
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

# Connect to Weaviate
client = weaviate.WeaviateClient(
    connection_params=ConnectionParams.from_params(
        http_host="weaviate.fastdatascience.com",
        http_port=443,
        http_secure=True,
        grpc_host="grpc.weaviate.fastdatascience.com",
        grpc_port=50051,
        grpc_secure=True,
    ),
    auth_client_secret=Auth.api_key(os.environ.get("HARMONY_WEAVIATE_API_KEY")),
    skip_init_checks=False
)
client.connect()

print(client.is_ready())

harmony_index = client.collections.get("harmony")


# End connect to Weaviate

def clean_results_dict_and_get_schema(result_data):
    schema = {}
    if "schema" in result_data:
        if result_data["schema"] is not None and result_data["schema"] != "":
            schema = json.loads(result_data["schema"])
        del result_data["schema"]

    # remove fields with dummy values
    for field_name in ["age_lower", "age_upper", "sample_size", "num_sweeps"]:
        if field_name in result_data and (
                result_data[field_name] == 0 or (field_name == "age_upper" and result_data[field_name] == 999)):
            del result_data[field_name]

    if "original_content" in result_data:
        del result_data["original_content"]

    return schema


cache = {}


@router.get(
    path="/aggregate", response_model=SearchResponse, status_code=status.HTTP_200_OK, response_model_exclude_none=True,
)
def aggregate() -> SearchResponse:
    if "aggregate" in cache:
        return cache["aggregate"]

    variables_to_aggregate = ["source", "resource_type", "language_codes", "keywords", "sex", "country_codes",
                              "study_design"]
    numeric_variables_to_aggregate = ["sample_size", "age_lower", "age_upper", "start_year", "end_year",
                                      "duration_years", "num_variables", "num_sweeps"]
    metrics = []
    for variable_to_aggregate in variables_to_aggregate:
        metrics.append(Metrics(variable_to_aggregate).text(
            top_occurrences_count=True,
            top_occurrences_value=True,
            min_occurrences=50
        ))
    for variable_to_aggregate in numeric_variables_to_aggregate:
        metrics.append(Metrics(variable_to_aggregate).integer(
        ))

    response = harmony_index.aggregate.over_all(
        return_metrics=metrics
    )
    aggregations = {}

    for variable_to_aggregate in variables_to_aggregate:
        this_var_aggregations = {}
        for top_occurence in response.properties[variable_to_aggregate].top_occurrences:
            this_var_aggregations[top_occurence.value] = top_occurence.count
        aggregations[variable_to_aggregate] = this_var_aggregations

    for variable_to_aggregate in numeric_variables_to_aggregate:
        this_var_aggregations = response.properties[variable_to_aggregate].__dict__
        aggregations[variable_to_aggregate] = this_var_aggregations

    agg_response = SearchResponse(num_hits=0, aggregations=aggregations)

    cache["aggregate"] = agg_response

    return agg_response


@router.get(
    path="/search", response_model=SearchResponse, status_code=status.HTTP_200_OK, response_model_exclude_none=True,
    response_model_exclude_defaults=True, response_model_exclude_unset=True
)
def search(
        query: List[str] = Query(default=None),
        country_codes: List[CountryCode] = Query(default=None),
        source: List[str] = Query(default=None),
        language_codes: List[LanguageCode] = Query(default=None),
        resource_type: List[ResourceType] = Query(default=None),
        sex: List[Sex] = Query(default=None),
        study_design: List[str] = Query(default=None),
        keywords: List[str] = Query(default=None),
        age_min: int = Query(default=None),
        duration_years_min: int = Query(default=None),
        num_sweeps_min: int = Query(default=None),
        sample_size_min: int = Query(default=None),
        age_max: int = Query(default=None),
        duration_years_max: int = Query(default=None),
        num_sweeps_max: int = Query(default=None),
        sample_size_max: int = Query(default=None),
        num_results: int = Query(default=50),
        offset: int = Query(default=0),
        return_variables_within_parent: bool = Query(default=True)

) -> SearchResponse:
    query_vector = model.encode(sentences=query, convert_to_numpy=True)[0]

    filters_list = []

    property_name_to_property_value = {
        "country_codes": country_codes,
        "source": source,
        "language_codes": language_codes,
        "resource_type": resource_type,
        "sex": sex,
        "study_design": study_design,
        "keywords": keywords
    }

    property_name_to_min_maxes = {
        "age": (age_min, age_max),
        "duration_years": (duration_years_min, duration_years_max),
        "num_sweeps": (num_sweeps_min, num_sweeps_max),
        "sample_size": (sample_size_min, sample_size_max),
    }

    for property_name, property_value in property_name_to_property_value.items():
        if property_value is not None and len(property_value) > 0:
            filters_list.append(Filter.by_property(property_name).contains_any(property_value))

    for property_name, (property_value_min, property_value_max) in property_name_to_min_maxes.items():
        if property_value_min is not None and property_value_min > 0:
            property_name_with_suffix = property_name
            if property_name_with_suffix == "age":
                property_name_with_suffix = "age_lower"
            filters_list.append(Filter.by_property(property_name_with_suffix).greater_or_equal(property_value_min))
        if property_value_max is not None and property_value_max > 0:
            property_name_with_suffix = property_name
            if property_name_with_suffix == "age":
                property_name_with_suffix = "age_upper"
            filters_list.append(Filter.by_property(property_name_with_suffix).less_or_equal(property_value_max))

    if len(filters_list) > 0:
        filters = Filter.all_of(filters_list)
    else:
        filters = None

    harmony_query_response = harmony_index.query.near_vector(
        near_vector=query_vector,
        target_vector=["all_text", "name"],
        filters=filters,
        limit=num_results,
        offset=offset,
        return_metadata=MetadataQuery(distance=True, score=True),
        return_properties=["schema", "age_lower", "age_upper", "country_codes",
                           "study_design", "resource_type", "name", "description"],
        # don't link from study to variable - too many links, it crashes the query.
        return_references=QueryReference(
            link_on="has_parent",
            return_properties=["schema", "age_lower", "age_upper", "country_codes", "study_design"]
        ),
    )

    results = []

    dataset_id_to_position_in_results = {}
    parents_already_in_results = set()
    for top_level_response_object in harmony_query_response.objects:

        if top_level_response_object.properties["resource_type"] == "variable":
            match_type = MatchType.variable
        else:
            match_type = MatchType.study

        if return_variables_within_parent and "has_parent" in top_level_response_object.references:
            variable_result_data = {}
            variable_result_data["name"] = top_level_response_object.properties["name"]
            variable_result_data["description"] = top_level_response_object.properties["description"]
            variable_result_data["uuid"] = top_level_response_object.uuid.hex

            for study_object in top_level_response_object.references["has_parent"].objects:
                result_data = dict(study_object.properties)
                schema = clean_results_dict_and_get_schema(result_data)
                result_data["uuid"] = study_object.uuid.hex

                search_result = SearchResult(extra_data=result_data, dataset_schema=schema,
                                             distance=top_level_response_object.metadata.distance,
                                             cosine_similarity=1 - top_level_response_object.metadata.distance,
                                             score=top_level_response_object.metadata.score, match_type=[match_type],
                                             variables_which_matched=[variable_result_data], parent={})

                if study_object.uuid.hex in dataset_id_to_position_in_results:
                    search_result = results[dataset_id_to_position_in_results[study_object.uuid.hex]]

                    search_result.variables_which_matched.append(variable_result_data)
                    continue

                dataset_id_to_position_in_results[study_object.uuid.hex] = len(results)
                results.append(search_result)
        else:
            result_data = dict(top_level_response_object.properties)
            result_data["uuid"] = top_level_response_object.uuid.hex
            schema = clean_results_dict_and_get_schema(result_data)
            print("results", result_data)

            parent = {}

            if "has_parent" in top_level_response_object.references:
                for study_object in top_level_response_object.references["has_parent"].objects:
                    if study_object.uuid.hex in parents_already_in_results:
                        parent = {"data": {"uuid": study_object.uuid.hex}}
                        break
                    parents_already_in_results.add(study_object.uuid.hex)
                    parent_result_data = dict(study_object.properties)
                    parent_result_data["uuid"] = study_object.uuid.hex
                    parent_schema = clean_results_dict_and_get_schema(parent_result_data)
                    parent = {"extra_data": parent_result_data, "dataset_schema": parent_schema}
                    break

            search_result = SearchResult(extra_data=result_data, dataset_schema=schema,
                                         distance=top_level_response_object.metadata.distance,
                                         cosine_similarity=1 - top_level_response_object.metadata.distance,
                                         score=top_level_response_object.metadata.score, match_type=[match_type],
                                         variables_which_matched=[], parent=parent)

            if top_level_response_object.uuid.hex in dataset_id_to_position_in_results:
                search_result = results[dataset_id_to_position_in_results[top_level_response_object.uuid.hex]]
                if match_type not in search_result.match_type:
                    search_result.match_type.append(match_type)
                continue

            dataset_id_to_position_in_results[top_level_response_object.uuid.hex] = len(results)
            results.append(search_result)

    harmony_api_response = SearchResponse(num_hits=len(results),
                                          results=results,
                                          aggregations={}
                                          )

    return harmony_api_response

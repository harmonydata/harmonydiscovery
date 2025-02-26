from pydantic import BaseModel, Field
from typing import List

from harmony_discovery_api.schemas.schema import HarmonyResource
from enum import Enum
from typing import List

class HarmonyResourceResult(HarmonyResource):
    score: float
    children: List
    parent: dict

class ElasticSearchInfo(BaseModel):
    query_request: dict = Field(default=None,description="The request sent to Elasticsearch")
    secondary_query_request: dict = Field(default=None,
                                          description="Lookup queries only: The second request sent to Elasticsearch, if applicable")
    query_response: dict = Field(default=None,description="The response received from to Elasticsearch")
    secondary_query_response: dict = Field(default=None,
                                           description="Lookup queries only: if we retrieve a study, we retrieve all child variables and put them here. Likewise for the parent study of a variable.")


class LookupResponse(BaseModel):
    num_hits: int
    results: List = Field(default=None, description="The list of results") #  TODO: can we type this as List[HarmonyResourceResult]?
    diagnostic_info: ElasticSearchInfo = Field(default=None, description="Data set to/from Elasticsearch")
    ld_json: dict = Field(default=None,
                          description="Optional extra ld+json entry for this item. This is structured data that we will provide in the HTML page header so that we will be indexed on the internet: https://developers.google.com/search/docs/appearance/structured-data/dataset")


class MatchType(str, Enum):
    study: str = 'study'
    variable: str = 'variable'


class SearchResult(BaseModel):
    dataset_schema: dict = Field(default=None, description="Dataset represented according to schema.org")
    extra_data: dict = Field(default=None, description="Data about this search result which does not fit in the schema.org representation")
    distance: float = Field(default=None)
    cosine_similarity: float = Field(default=None)
    score:float = Field(default=None)
    match_type: List[MatchType] = Field(default=[], description="How was this match made? By the text in the study title and description? Or by the text in one of the variables?")
    variables_which_matched: list = Field(default=[])
    parent: dict = Field(default=None)


class SearchResponse(BaseModel):
    num_hits: int
    results: List[SearchResult] = Field(default=None, description="The list of results") #  TODO: can we type this as List[HarmonyResourceResult]?
    # diagnostic_info: ElasticSearchInfo = Field(default=None, description="Data set to/from Elasticsearch")
    aggregations: dict = Field(description="The breakdowns")




class CountResponse(BaseModel):
    num_documents: int



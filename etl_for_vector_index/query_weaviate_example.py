import os

import weaviate
from sentence_transformers import SentenceTransformer
# pip install weaviate-client-4.11.0b2
# pip install weaviate-client
from weaviate.classes.init import Auth
from weaviate.classes.query import MetadataQuery
from weaviate.collections.classes.grpc import QueryReference

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

weaviate_url = os.environ["WEAVIATE_URL"]
weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

query_vector = model.encode(sentences=["i feel nervous, anxious or afraid"], convert_to_numpy=True)[0]

with weaviate.connect_to_weaviate_cloud(
        cluster_url=weaviate_url,
        auth_credentials=Auth.api_key(weaviate_api_key),
) as client:
    print("is Weaviate ready?", client.is_ready())

    print("Getting variables:")

    variable_index = client.collections.get("harmony_variables")

    query_response = variable_index.query.near_vector(
        near_vector=query_vector,
        target_vector=["all_text", "name"],
        limit=10,
        return_metadata=MetadataQuery(distance=True, score=True),
        return_references=QueryReference(
            link_on="has_parent",
            return_properties=["name"]
        )
    )

    for r in query_response.objects:
        print(r.metadata.distance, r.metadata.score, r.properties["resource_type"], r.properties["name"],
              r.references["has_parent"].objects)  # ,  r.properties["title"], "/", r.properties["all_text"])

    print("Getting studies:")

    study_index = client.collections.get("harmony_studies")

    query_response = study_index.query.near_vector(
        near_vector=query_vector,
        target_vector=["all_text", "name"],
        limit=100,
        return_metadata=MetadataQuery(distance=True, score=True),
        # don't link from study to variable - too many links, it crashes the query.
        # return_references=QueryReference(
        #     link_on="has_child",
        #     return_properties=["title"]
        # )
    )

    for r in query_response.objects:
        c = "no children"
        if r.references is not None and "has_child" in r.references:
            c = r.references["has_child"].objects

        print(r.metadata.distance, r.metadata.score, r.properties["resource_type"], r.properties["name"],
              c)  # ,  r.properties["title"], "/", r.properties["all_text"])

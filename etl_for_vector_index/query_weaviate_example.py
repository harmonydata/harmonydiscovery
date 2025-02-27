import os

import weaviate
from sentence_transformers import SentenceTransformer
# pip install weaviate-client-4.11.0b2
# pip install weaviate-client
from weaviate.classes.init import Auth
from weaviate.classes.query import MetadataQuery
from weaviate.collections.classes.grpc import QueryReference
from weaviate.classes.init import Auth
from weaviate.connect import ConnectionParams

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

# weaviate_url = os.environ["WEAVIATE_URL"]
# weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

query_vector = model.encode(sentences=["i feel nervous, anxious or afraid"], convert_to_numpy=True)[0]

with weaviate.WeaviateClient(
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
) as client:
    print("is Weaviate ready?", client.is_ready())

    print("Getting variables:")

    harmony_index = client.collections.get("harmony")

    query_response = harmony_index.query.near_vector(
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
        print(r.metadata.distance, r.metadata.score, r.properties["resource_type"], r.properties["name"])  # ,  r.properties["title"], "/", r.properties["all_text"])
        if "has_parent" in r.references:
            print ("\tparent:", r.references["has_parent"].objects)

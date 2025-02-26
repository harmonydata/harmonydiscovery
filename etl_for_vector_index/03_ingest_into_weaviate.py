import hashlib
import json
import logging
import uuid

import weaviate
# pip install weaviate-client-4.11.0b2
import weaviate.classes as wvc
from tqdm import tqdm
from weaviate.classes.config import ReferenceProperty
# pip install weaviate-client
from weaviate.classes.init import Auth
from weaviate.connect import ConnectionParams


def create_uuid_from_string(val: str):
    hex_string = hashlib.md5(val.encode("UTF-8")).hexdigest()
    return uuid.UUID(hex=hex_string)


logging.basicConfig(filename='logging.log', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")


def add_vectors(bulk_list):
    texts_to_embed = []
    for idx in range(0, len(bulk_list), 2):
        texts_to_embed.append(bulk_list[idx + 1]["all_text"])
    embeddings = model.encode(sentences=texts_to_embed, convert_to_numpy=True)
    for idx in range(0, len(bulk_list), 2):
        bulk_list[idx + 1]["all_text_vector"] = embeddings[int(idx // 2)]


def batch_ingest(harmony_index, list_of_dicts: list):
    batch_size = 50

    with harmony_index.batch.dynamic() as batch:
        for start in tqdm(range(0, len(list_of_dicts), batch_size)):
            # sleep(1)  # throttle the import
            # print (f"Start of batch: {start} of {len(list_of_dicts)}")
            end = start + batch_size
            if end >= len(list_of_dicts):
                end = len(list_of_dicts)
            this_batch_of_objects_to_ingest = list_of_dicts[start:end]

            title_texts_to_embed = [r["name"] for r in this_batch_of_objects_to_ingest]
            texts_to_embed = [r["all_text"] for r in this_batch_of_objects_to_ingest]
            title_embeddings = model.encode(sentences=title_texts_to_embed, convert_to_numpy=True,
                                            show_progress_bar=False)
            text_embeddings = model.encode(sentences=texts_to_embed, convert_to_numpy=True, show_progress_bar=False)

            for idx_within_batch, item in enumerate(this_batch_of_objects_to_ingest):
                item_preproc = dict(item)
                my_uuid = create_uuid_from_string(item["id"])
                del item_preproc[
                    "id"]  # "id" is a reserved keyword in Weaviate, we must use Weaviate's UUID. But we can keep the Harmony ID and put it in field harmony_id
                item_preproc["harmony_id"] = item["id"]

                references = None
                if "parent_id" in item:
                    if item["parent_id"] is None:
                        del item["parent_id"]
                    else:
                        parent_id_in_harmony_universe = item["parent_id"]
                        parent_uuid = create_uuid_from_string(parent_id_in_harmony_universe)
                        references = {"has_parent": parent_uuid}

                    # print("Parent is", parent_uuid, parent_id_in_harmony_universe)

                # object_uuid = harmony_index.data.insert(properties=item_preproc,
                #                                         # Specify the named vectors, following the collection definition
                #                                         vector={
                #                                             "name": title_embeddings[idx_within_batch],
                #                                             "all_text": text_embeddings[idx_within_batch]
                #                                         },
                #                                         uuid=my_uuid,
                #                                         references=references
                #                                         )

                object_uuid = batch.add_object(properties=item_preproc,
                                               # Specify the named vectors, following the collection definition
                                               vector={
                                                   "name": title_embeddings[idx_within_batch],
                                                   "all_text": text_embeddings[idx_within_batch]
                                               },
                                               uuid=my_uuid,
                                               references=references
                                               )


def read_docs(filepath: str):
    result = []
    with open(filepath, "r", encoding="utf-8") as file:
        for line in file.readlines():
            data = json.loads(line)
            result.append(data)

    # TODO: delete
    # result = [x for x in result if x["source"] == "mhc"]
    #
    # results2 = []
    # parent_ids_allowed = set()
    # for x in result:
    #     if x["resource_type"] != "variable":
    #         results2.append(x)
    #         parent_ids_allowed.add(x["id"])
    # for x in result:
    #     if x["resource_type"] == "variable" and x["parent_id"] in parent_ids_allowed:
    #         results2.append(x)
    #     if len(results2) > 200:
    #         break
    #
    #
    # result = list(results2)
    # end TODO

    print(f"Number of items to insert into Weaviate: {len(result)}")

    return result


def main(file_names):
    # Best practice: store your credentials in environment variables
    # weaviate_url = os.environ["WEAVIATE_URL"]
    # weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

    # Connect to Weaviate Cloud
    with weaviate.WeaviateClient(
            connection_params=ConnectionParams.from_params(
                http_host="weaviate.fastdatascience.com",
                http_port=443,
                http_secure=True,
                grpc_host="grpc.weaviate.fastdatascience.com",
                grpc_port=50051,
                grpc_secure=True,
            ),
            auth_client_secret=Auth.api_key("rnjXYPkl+27UWFDezxw="),
            skip_init_checks=False
    ) as client:
        client.connect()

        print("is Weaviate ready?", client.is_ready())

        client.collections.delete("harmony")
        harmony_collection = client.collections.create(
            name="harmony",
            description="Collection with named vectors",
            properties=[
                wvc.config.Property(name="name", data_type=wvc.config.DataType.TEXT),
                wvc.config.Property(name="description", data_type=wvc.config.DataType.TEXT),
                wvc.config.Property(name="original_content", data_type=wvc.config.DataType.TEXT,
                                    indexFilterable=False,
                                    indexSearchable=False,
                                    description="The original content from the source, often in JSON format. We don't need to search on this but we might need to look inside for diagnostics."),
                wvc.config.Property(name="schema", data_type=wvc.config.DataType.TEXT,
                                    indexFilterable=False,
                                    indexSearchable=False,
                                    description="The schema.org schema for this study"),
            ],
            vectorizer_config=[
                wvc.config.Configure.NamedVectors.none(
                    name="name"
                ),
                wvc.config.Configure.NamedVectors.none(
                    name="all_text"
                )
            ],
            references=[
                ReferenceProperty(
                    name="has_parent", target_collection="harmony"
                )
            ],
        )
        harmony_collection = client.collections.get("harmony")

        for file_name in file_names:
            print(f"Reading {file_name}...")
            list_of_dicts = read_docs(file_name)

            resources_to_put_in_weaviate = []
            for x in list_of_dicts:
                if x["resource_type"] != "variable":
                    # print(x["name"], x["source"])
                    resources_to_put_in_weaviate.append(x)
            print(f"Number of resources (non-variables) found: {len(resources_to_put_in_weaviate)}")

            print(f"Ingesting studies from {file_name} into Weaviate index...")
            batch_ingest(harmony_collection, resources_to_put_in_weaviate)

            resources_to_put_in_weaviate = []
            for x in list_of_dicts:
                if x["resource_type"] == "variable":
                    # print(x["name"], x["source"])
                    resources_to_put_in_weaviate.append(x)

            print(f"Number of variables found: {len(resources_to_put_in_weaviate)}")
            print(f"Ingesting variables from {file_name} into Weaviate index...")
            batch_ingest(harmony_collection, resources_to_put_in_weaviate)

            print(f"Finished ingesting {file_name} into Weaviate index.")


if __name__ == "__main__":
    file_names = ["all_resources_for_weaviate.json"]

    main(file_names)

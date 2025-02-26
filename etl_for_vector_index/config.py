import os

INDEX_NAME = "harmony_discovery"
# FILE_PATH = "all_harmony_resources.json"
################################


URL = "https://elasticsearch.fastdatascience.com:9200/"
ELASTIC_PASSWORD = os.environ["ELASTIC_PASSWORD"]
USERNAME = "elastic"
################################


MAPPING = {
    "mappings": {
        "properties": {
            "all_text": {"type": "text"},
            "age_lower": {"type": "integer"},
            "age_upper": {"type": "integer"},
            "country_codes": {"type": "keyword"},
            "country": {"type": "text"},
            "data_access": {"type": "text"},
            "description": {"type": "text"},
            "doi": {"type": "keyword", "index": False},
            "duration_years": {"type": "integer"},
            "end_year": {"type": "integer"},
            "funders": {"type": "text"},
            "genetic_data_collected": {"type": "boolean"},
            "geographic_coverage": {"type": "text"},
            "id": {"type": "keyword"},
            "institution": {"type": "text"},
            "instruments": {"type": "text"},
            "language_codes": {"type": "keyword"},
            "num_sweeps": {"type": "integer"},
            "num_variables": {"type": "integer"},
            "original_content": {"type": "text", "index": False},
            "original_source_url": {"type": "keyword"},
            "owner": {"type": "text"},
            "question": {"type": "text"},
            "response_options": {"type": "text", "index": False},
            "resource_type": {"type": "keyword"},
            "sample_size": {"type": "integer"},
            "sex": {"type": "keyword"},
            "source": {"type": "keyword"},
            "start_year": {"type": "integer"},
            "study_design": {"type": "keyword"},
            "title": {"type": "text"},
            "topics": {"type": "keyword"},
            "url": {"type": "keyword", "index": False},
            "variable_name": {"type": "text", "index": False},
            "all_text_vector": {
                "type": "dense_vector",
                "dims": 384,
                "index": True,
                "similarity": "cosine",
                "index_options": {
                    "type": "hnsw",
                    "ef_construction": 128,
                    "m": 24
                }
            },
            "study_variable_relationship": {
                "type": "join",
                "relations": {
                    "study": "variable"
                }
            }
        }
    }}

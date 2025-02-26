# Harmony Discovery

## Architecture

Harmony Discovery is designed as a single vector index hosted in Weaviate. It would also be possible to use an alternative such as Elasticsearch or Pinecone.

All studies, datasets, and variables are indexed in the vector index according to the vector representation of their text (item name and item description).

There are also some traditional numeric and keyword based fields in the vector index such as `sample_size`.

## Where is everything?

The Weaviate server is running at `weaviate.fastdatascience.com`. This is the vector index. At present it's hosted on Azure but in future we can move to a different hosting provider or use Weaviate Serverless. The Weaviate index is defined as a single Docker compose file under [vector_index/docker-compose.yml](vector_index/docker-compose.yml).

The Harmony Discovery API is currently at https://harmonydiscovery.fastdatascience.com/docs

## Architecture

### Index creation

An ingest code reads the data sources, converts them to a unified structure following the schema.org Dataset definition, and then vectorises the text fields using HuggingFace and ingests them into the Weaviate index.

### Retrieval

Discovery API converts incoming texts to vectors using HuggingFace and then connects to Weaviate.


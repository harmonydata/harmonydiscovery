# Harmony Discovery

## Architecture

Harmony Discovery is designed as a single vector index hosted in Weaviate. It would also be possible to use an alternative such as Elasticsearch or Pinecone.

All studies, datasets, and variables are indexed in the vector index according to the vector representation of their text (item name and item description).

There are also some traditional numeric and keyword based fields in the vector index such as `sample_size`.

## Where is everything?

The Weaviate server is running at `weaviate.fastdatascience.com` and `grpc.weaviate.fastdatascience.com` (Azure VM running on IP `20.39.218.72`). This is the vector index. At present it's hosted on Azure but in future we can move to a different hosting provider or use Weaviate Serverless. The Weaviate index is defined as a single Docker compose file under [vector_index/docker-compose.yml](vector_index/docker-compose.yml).

The Harmony Discovery API is currently at https://harmonydiscovery.fastdatascience.com/docs

## Architecture

### Index creation

An ingest code reads the data sources, converts them to a unified structure following the schema.org Dataset definition, and then vectorises the text fields using HuggingFace and ingests them into the Weaviate index.

### Retrieval

Discovery API converts incoming texts to vectors using HuggingFace and then connects to Weaviate.



## Example API calls

```
curl 'https://harmonydiscoveryapi.fastdatascience.com/discover/search?query=adhd&num_results=100&resource_type=study&study_design=twin'


curl 'https://harmonydiscoveryapi.fastdatascience.com/discover/search?query=anxiety&num_results=10&return_variables_within_parent=true'
```

## Description of the API

There is an optional parameter `return_variables_within_parent` which defaults to `true` .  If it is true, then all the results will be a study/dataset - if a variable is found, it is wrapped within its containing study/dataset.

The main data is inside a property called `schema` which is the Schema.org Dataset schema design. So everything is compliant with schema.org.

There is also pagination. You can send `&offset=10` to go from result no. 10, for example.

If a study is returned more than once in the result sequence, the second time it is referred to only by its UUID, to make the response JSON smaller.

The `/discover/aggregate` endpoint is added. I am not generating a histogram as Weaviate does not do that, so for numeric vars it gives me median, mean, etc. But every indexed variable is aggregated by the `aggregate` endpoint.

If something like `age_lower` is set to 0, it is now not returned. So what you get from the API should all be displayable, you shouldn't need to do any cleaning.

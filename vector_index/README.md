# Vector index for Harmony Discovery

The Docker compose file defines the Weaviate index. The Weaviate index does not invoke HuggingFace - the API which is calling Weaviate should handle HuggingFace.

## Where is it running?

The vector index is running on an Azure VM with 16 GB RAM at IP `20.39.218.72`.

The Weaviate index is defined as a single Docker compose file at [docker-compose.yml](docker-compose.yml).

At present it's hosted on Azure but in future we can move to a different hosting provider or use Weaviate Serverless.

The Azure VM runs on an IP address and its domain/subdomain is defined by two A records in the DNS:

![arecords](docs/arecords.png)

## Certificates

Since Azure VM by default does not have SSL certificates, we have added these manually

TODO

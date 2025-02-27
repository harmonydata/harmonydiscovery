# Harmony scripts to upload to Weaviate

## How this is structured

There are individual subfolders for each of the data sources:

* adruk 
* closer
* cls
* hdruk
* matilda
* mhc
* ukds
* ukllc

In each subfolder, you can run the scripts in numerical order. The first scripts download the data from external data sources and the final script (`99_prepare_for_vector_index.py`) prepares it in the format that Harmony Discovery will index it, where everything is oriented around schema.org `Dataset` objects.

When all data sources have been processed, you can run the scripts in the base directory.

* `01_deduplicate_entries.py` - this identifies any studies or datasets that appeared in more than one source, and consolidates them.
* `02_prepare_for_vector_index.py` - this prepares everything for the format that Weaviate will ingest it
* `03_ingest_into_weaviate.py` - this sends it to the Weaviate server in dynamic batches
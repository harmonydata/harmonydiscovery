import requests
import json
import os
# define API key
API_KEY = os.environ['UKLLC_FASTAPI_KEY']

# get all datasets
url = "https://metadata-api-4a09f2833a54.herokuapp.com/all-datasets"
r = requests.get(url, headers={'access_token': API_KEY})
print(r.status_code)
all_datasets = r.json()

with open("intermediate_output/datasets.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(all_datasets, indent=4))

# test endpoint with filter and token
url = "https://metadata-api-4a09f2833a54.herokuapp.com/all-sources"
r = requests.get(url, headers={'access_token': API_KEY})
with open("intermediate_output/sources.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(r.json(), indent=4))

# empty dictionary to store all labelling metadata
all_labels = {}
# define base url
url1 = "https://metadata-api-4a09f2833a54.herokuapp.com/datasets/{source_and_table_name}?"
# for each data source / dataset pair build url and request
for i in all_datasets:
    # format source and dataset
    source = i['source'].lower()
    dataset = i['table'].lower()
    url2 = "source={}&table_name={}".format(source, dataset)
    # full URL
    url = url1 + url2
    # make request
    r = requests.get(url, headers={'access_token': API_KEY})
    print(r.status_code)
    # convert to text
    data = r.text
    # if data found
    if r.status_code == 200:
        # convert to json
        pj = json.loads(data)
        # build dict key incl source and dataset
        k = '_'.join([source, dataset])
        # build dict of all outputs
        all_labels[k] = pj


with open("intermediate_output/all_labels.json", "w", encoding="utf-8") as f:
    f.write(json.dumps(all_labels, indent=4))

import bz2
import os
import pathlib
import pickle as pkl
from pathlib import Path

import pandas as pd
import sqlalchemy
from tqdm import tqdm

output_folder = os.path.join(pathlib.Path(__file__).parent.resolve(), "intermediate_output")
Path(output_folder).mkdir(parents=True, exist_ok=True)

db_str = os.environ['DATABASE_URL'].replace("postgres", "postgresql+psycopg2", 1)

cnxn = sqlalchemy.create_engine(db_str).connect()

df_tables = pd.read_sql("SELECT * from pg_catalog.pg_tables", cnxn)

df_tables.to_pickle("intermediate_output/tables.pkl.bz2")

df_datasets = pd.read_sql("SELECT * from dataset", cnxn)

df_datasets.to_pickle("intermediate_output/datasets.pkl.bz2")

df_sources = pd.read_sql("SELECT * from source_info", cnxn)

df_sources.to_pickle("intermediate_output/sources.pkl.bz2")

df_dataset_ages = pd.read_sql("SELECT * from dataset_ages", cnxn)

df_dataset_ages.to_pickle("intermediate_output/dataset_ages.pkl.bz2")

print("Downloading table metadata...")

all_table_metadata = {}
for table_name in tqdm(set(df_tables.tablename)):
    if table_name.startswith("metadata_"):
        df_metadata = pd.read_sql(f"SELECT * from {table_name}", cnxn)

        all_table_metadata[table_name] = df_metadata

with bz2.open("intermediate_output/table_metadata.pkl.bz2", "w") as f:
    pkl.dump(all_table_metadata, f)

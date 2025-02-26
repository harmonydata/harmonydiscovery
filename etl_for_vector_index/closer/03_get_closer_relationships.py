import os

pw = os.environ["CLOSER_PW"]
username = os.environ["CLOSER_USERNAME"]

import json
from time import sleep

import pandas as pd
from colectica_api import ColecticaObject
from tqdm import tqdm

C = ColecticaObject("discovery.closer.ac.uk", username, pw)

with open("intermediate_output/closer_items.json", "r", encoding="utf-8") as f:
    closer_items = json.loads(f.read())

dfs = []
for qg_idx, study in enumerate(tqdm(closer_items["Study"]['Results'])):
    df, X = C.item_info_set(AgencyId=study["AgencyId"],
                            Identifier=study["Identifier"])
    df["source_agency"] = study["AgencyId"]
    df["source_identifier"] = study["Identifier"]
    dfs.append(df)
    sleep(1)
    # if len(dfs)> 100:
    #    break
    if len(dfs) % 1000 == 5:
        df_study_relationships = pd.concat(dfs)
        df_study_relationships.to_csv(f"intermediate_output/closer_study_relationships_{qg_idx}.csv", index=False)
        del df_study_relationships

df_study_relationships = pd.concat(dfs)
df_study_relationships.to_csv("intermediate_output/closer_study_relationships.csv", index=False)

exit()

dfs = []
for qg_idx, question_group in enumerate(tqdm(closer_items["Question Group"]['Results'])):
    df, X = C.item_info_set(AgencyId=question_group["AgencyId"],
                            Identifier=question_group["Identifier"])
    df["source_agency"] = question_group["AgencyId"]
    df["source_identifier"] = question_group["Identifier"]
    dfs.append(df)
    sleep(1)
    # if len(dfs)> 100:
    #    break
    if len(dfs) % 1000 == 5:
        df_question_group_relationships = pd.concat(dfs)
        df_question_group_relationships.to_csv(f"intermediate_output/closer_question_group_relationships_{qg_idx}.csv", index=False)
        del df_question_group_relationships

df_question_group_relationships = pd.concat(dfs)
df_question_group_relationships.to_csv("intermediate_output/closer_question_group_relationships.csv", index=False)

dfs = []
for i_idq, instrument in enumerate(tqdm(closer_items["Instrument"]['Results'])):
    df, X = C.item_info_set(AgencyId=instrument["AgencyId"],
                            Identifier=instrument["Identifier"])
    df["source_agency"] = instrument["AgencyId"]
    df["source_identifier"] = instrument["Identifier"]
    dfs.append(df)
    sleep(1)
    if len(dfs) % 1000 == 5:
        df_instrument_relationships = pd.concat(dfs)
        df_instrument_relationships.to_csv(f"intermediate_output/closer_instrument_relationships_{i_idq}.csv", index=False)
        del df_instrument_relationships

df_instrument_relationships = pd.concat(dfs)
df_instrument_relationships.to_csv("intermediate_output/closer_instrument_relationships.csv", index=False)

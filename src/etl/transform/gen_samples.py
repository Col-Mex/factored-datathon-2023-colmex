import joblib
import gzip
import json
import numpy as np
import random
import pandas as pd
import os

jsons = True
run = False

def chunk_creator(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def extract_sample_from_compressed(list_files, path_files, percentage_sample, output_path, random_seed=5):
    random.seed(random_seed)
    random_sample_rev_1p = random.sample(list_files, int(len(list_files)*percentage_sample))
    dataframes = []
    for file in random_sample_rev_1p:
        with gzip.open(os.path.join(path_files,file), 'r') as fin:
            decompressed_data = fin.read().decode('utf-8')
        json_objects = [json.loads(line) for line in decompressed_data.splitlines() if line]
        df = pd.DataFrame(json_objects)
        dataframes.append(df)
    all_data = pd.concat(dataframes)

    # Save the DataFrame to a .parquet file
    all_data.to_parquet(output_path)

def extract_sample_from_jsons(list_files, path_files, percentage_sample, output_path, random_seed=5, if_random=True):
    if if_random:
        random.seed(random_seed)
        random_sample_rev_1p = random.sample(list_files, int(len(list_files)*percentage_sample))
    else:
        random_sample_rev_1p = list_files
    dataframes = []
    for file in random_sample_rev_1p:
        with open(os.path.join(path_files,file), 'r') as f:
            data = f.readlines()
        json_objects = [json.loads(line) for line in data if line]
        df = pd.DataFrame(json_objects)
        dataframes.append(df)
    all_data = pd.concat(dataframes)

    # Save the DataFrame to a .parquet file
    all_data.to_parquet(output_path)
    
    return random_sample_rev_1p


if run:
    if jsons:
        list_reviews = os.listdir("sample/review_data/")
        list_metadata = os.listdir("sample/review_metadata/")
        
        extract_sample_from_jsons(list_reviews, "sample/review_data/", 0.20, "sample/review_data_sample/reviews_sample_20p.parquet")
        extract_sample_from_jsons(list_metadata, "sample/review_metadata/", 0.20, "sample/review_metadata_sample/metadata_sample_20p.parquet")
    else:
        list_reviews = joblib.load("src/etl/extract/downloaded_reviews.joblib")
        list_metadata = joblib.load("src/etl/extract/downloaded_metadata.joblib")
        
        extract_sample_from_compressed(list_reviews, "src/etl/extract/", 0.01, "src/etl/transform/reviews_sample_1p.parquet")
        extract_sample_from_compressed(list_metadata, "src/etl/extract/", 0.01, "src/etl/transform/metadata_sample_1p.parquet")

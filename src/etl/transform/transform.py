import pandas as pd
import json
from gen_samples import *

import os

percentage_of_run = 0.05

elements_running = ["metadata", "data"]

list_metadata = os.listdir("sample/review_metadata/")
list_data = os.listdir("sample/review_data/")


for idx, element in enumerate([list_metadata, list_data]):
    len_element = len(element)
    batch_size = int(len_element*percentage_of_run)
    chunks = list(chunk_creator(element, batch_size))
    
    counter = 0
    for chunk in chunks:
        print("running chunk number {} for {}".format(counter, elements_running[idx]))
        proccessed = extract_sample_from_jsons(chunk, "sample/review_{}/".format(elements_running[idx]), 0.05, f"sample/review_{elements_running[idx]}_sample/partitions/{elements_running[idx]}_sample_5p_{counter}.parquet", if_random=False)
        counter += 1
        print("done.")



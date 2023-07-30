from src.etl.transform.filter import *

import os


# get ASINS and Category of metadata
list_of_metadata_partitions = os.listdir('sample/review_metadata_sample/partitions/')
list_of_data_partitions = os.listdir('sample/review_data_sample/partitions/')
list_of_data_filtered = os.listdir('sample/review_data_sample/filtered/')
files = ["asins", "categories", "main_cat"]

asins, categories, main_cat = get_asins(list_of_metadata_partitions)
filtered_asins, filtered_categories = filter_asins(asins, categories, main_cat, term = "Software")
filter_data(filtered_asins, list_of_data_partitions)

join_filter(list_of_data_filtered)

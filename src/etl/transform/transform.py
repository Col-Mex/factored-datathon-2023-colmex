import pandas as pd
import json

main_path = ""
sample_path = main_path + "sample/"

reviews_path = sample_path + "reviews0.json"
metadata_path = sample_path + "metadata0.json"

reviews_df_path = sample_path + "reviews.csv"
metadata_df_path = sample_path + "metadata.csv"

with open(reviews_path, 'r') as f:
    data = f.readlines()

list_of_data = [json.loads(x) for x in data]

colnames = ['asin', 'overall', 'reviewText', 'reviewerID', 'summary', 'unixReviewTime', 'verified']
reviews = pd.DataFrame(data=[list(list_of_data[0].values())], columns=colnames)
reviews.to_csv(reviews_df_path, index=False, sep="|")

for element in list_of_data[1:]:
    review_temp = pd.DataFrame(data=[list(element.values())], columns=list(element.keys()))
    review_temp = review_temp.replace(r'\n',' ', regex=True)
    if set(list(review_temp.columns)) == set(colnames):
        review_temp = review_temp[colnames]
        review_temp.to_csv(reviews_df_path, mode="a", header=False, index=False, sep="|")


with open(metadata_path, 'r') as f:
    metadata = f.readlines()

list_of_metadata = [json.loads(x) for x in metadata]

colnames_metadata = ['also_buy', 'also_view', 'asin', 'brand', 'category', 'date', 'description', 'details', 'feature', 'fit', 'image', 'main_cat', 'price', 'rank', 'similar_item', 'tech1', 'tech2', 'title']
metadata = pd.DataFrame(data=[list(list_of_metadata[0].values())], columns=colnames_metadata)
metadata.to_csv(metadata_df_path, index=False, sep="|")

for element in list_of_metadata[1:]:
    metadata_temp = pd.DataFrame(data=[list(element.values())], columns=list(element.keys()))
    metadata_temp = metadata_temp.replace(r'\n',' ', regex=True)
    if set(list(metadata_temp.columns)) == set(colnames_metadata):
        metadata_temp = metadata_temp[colnames_metadata]
        metadata_temp.to_csv(metadata_df_path, mode="a", header=False, index=False, sep="|")





import pandas as pd
import json

main_path = ""
sample_path = main_path + "sample/"

reviews_path = sample_path + "reviews0.json"
metadata_path = sample_path + "metadata0.json"

reviews_df_path = sample_path + "reviews.csv"
metadata_df_path = sample_path + "reviews.csv"

with open(reviews_path, 'r') as f:
    data = f.readlines()

list_of_data = [json.loads(x) for x in data]

colnames = ['asin', 'overall', 'reviewText', 'reviewerID', 'reviewerName', 'summary', 'unixReviewTime', 'verified']
reviews = pd.DataFrame(data=[list(list_of_data[0].values())], columns=colnames)
reviews.to_csv(reviews_df_path, index=False, sep="|")

for element in list_of_data[1:]:
    review_temp = pd.DataFrame(data=[list(element.values())], columns=list(element.keys()))
    review_temp = review_temp.replace(r'\n',' ', regex=True)
    if set(list(review_temp.columns)) == set(colnames):
        review_temp = review_temp[colnames]
        review_temp.to_csv(reviews_df_path, mode="a", header=False, index=False, sep="|")


import pyarrow.parquet as pq
from src.etl.transform.transform import model
import multiprocessing
import pandas as pd


reviews_sample = pq.read_table('sample/data_music.parquet')
metadata_sample = pq.read_table('sample/metadata_music.parquet')

metadata_sample = metadata_sample.to_pandas()
metadata_sample = metadata_sample.drop_duplicates("asin")

first_columns = list(reviews_sample.columns)

reviews_sample = reviews_sample.to_pandas()
reviews_sample.reset_index(drop=True, inplace=True)
reviews_sample["counter"] = list(reviews_sample.index)

reviews_merged = reviews_sample.merge(metadata_sample, how="left", on="asin")
top_10_brands = list(reviews_merged.groupby("brand").size().sort_values(ascending=False).head(10).index)

reviews_merged = reviews_merged[reviews_merged["brand"].isin(top_10_brands)]

# reviews_sample = reviews_sample.head(10)
reviews_and_summary = reviews_merged[["reviewText", "summary"]].values.tolist()

sentimenter = model()
sentimenter.load_model()

pool_obj = multiprocessing.Pool(processes=10)
table = pool_obj.map(sentimenter.apply_model_to_data, reviews_and_summary)
pool_obj.close()

sentiment = [i[0][0]for i in table]
emotion = [i[1][0]for i in table]


reviews_merged["sentiment"] = sentiment
reviews_merged["emotion"] = emotion


reviews_merged['reviewID'] = reviews_merged['reviewerID'] + "-" + reviews_merged['asin'] + "-" + reviews_merged['unixReviewTime']
reviews_merged['dateReview'] = pd.to_datetime(reviews_merged['unixReviewTime'].astype('int'), unit='s')


expected_columns = ["reviewID",
                    "asin",
                    "overall",
                    "reviewText",
                    "reviewerID",
                    "reviewerName",
                    "summary",
                    "unixReviewTime",
                    "dateReview",
                    "verified",
                    "vote"]


reviews_merged = reviews_merged[expected_columns]

reviews_merged.to_parquet("sample/data_music_labeled.parquet")


##### Create Metadata table

music_metadata = metadata_sample[metadata_sample["brand"].isin(top_10_brands)]

metadata_expected_columns = ["as_in", "brand", "main_cat", "title"]

music_metadata.rename(columns={"asin" : "as_in"}, inplace = True)

music_metadata[metadata_expected_columns]

music_metadata.to_parquet("sample/metadata_music_brands.parquet")


#### Manage stream data
with open('jsons/asins_music_instruments.txt', 'r') as f:
    lines = f.readlines()

lines = [l.strip() for l in lines]

stream_sample = pq.read_table('sample/stream_data_music_43424.parquet')
stream_sample = stream_sample.to_pandas()

stream_sample = stream_sample[stream_sample["asin"].isin(lines)]

industry_asins = lines

streaming_data = stream_sample.copy()

stream_sample[expected_columns]


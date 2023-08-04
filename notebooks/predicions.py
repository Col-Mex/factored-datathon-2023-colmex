import pyarrow.parquet as pq
from src.etl.transform.transform import data_filtering


model = data_filtering()

reviews_sample = pq.read_table('sample/data_music.parquet')
reviews_sample = reviews_sample.to_pandas()

reviews_sample.head(10)

reviews_sample = model.apply_model_to_data(reviews_sample)
reviews_sample.to_parquet("sample/data_music.parquet")

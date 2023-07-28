import pyarrow.parquet as pq

reviews_sample = pq.read_table('sample/review_data_sample/reviews_sample_1p.parquet')
metadata_sample = pq.read_table('sample/review_metadata_sample/metadata_sample_1p.parquet')

reviews_sample.to_pandas()
metadata_sample.to_pandas()


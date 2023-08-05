from src.etl.transform.transform import *



percentage_of_run = 0.05
elements_running = ["metadata", "data"]
list_metadata = os.listdir("sample/review_metadata/")
list_data = os.listdir("sample/review_data/")


# get ASINS and Category of metadata
# list_of_metadata_partitions = os.listdir('sample/review_metadata_sample/partitions/')
# list_of_data_partitions = os.listdir('sample/review_data_sample/partitions/')
# list_of_data_filtered = os.listdir('sample/review_data_sample/filtered/')
# files = ["asins", "categories", "main_cat"]

filtered = data_filtering()

def execute_transform(data_or_metadata="metadata"):
    asins, brands, main_cat = filtered.get_asins()
    industry_asins, industry_brands = filtered.filter_asins(term = "Musical Instruments")
    filtered.save_list(industry_asins, "metadata", "asins_music_instruments")
    filtered.save_list(industry_brands, "metadata", "brands_music_instruments")
    data, metadata = filtered.filter_data(industry_asins)
    data = filtered.join_filter(data)
    metadata = filtered.join_filter(metadata)
    
    data.to_parquet("sample/data_music.parquet")
    metadata.to_parquet("sample/metadata_music.parquet")


if __name__ == "__main__":
    execute_transform()




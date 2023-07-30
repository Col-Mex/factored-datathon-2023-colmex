from src.etl.transform.transform import *



percentage_of_run = 0.05
elements_running = ["metadata", "data"]
list_metadata = os.listdir("sample/review_metadata/")
list_data = os.listdir("sample/review_data/")


# get ASINS and Category of metadata
list_of_metadata_partitions = os.listdir('sample/review_metadata_sample/partitions/')
list_of_data_partitions = os.listdir('sample/review_data_sample/partitions/')
list_of_data_filtered = os.listdir('sample/review_data_sample/filtered/')
files = ["asins", "categories", "main_cat"]

extractor = data_extraction()
filtered = data_filtering()

def execute_transform(data_or_metadata="metadata"):
    filtered.get_asins(list_of_metadata_partitions)
    industry_asins = filtered.filter_asins(term = "Software")
    filtered.save_list(industry_asins, 'metadata', 'asins_industry')
    filtered.filter_data(list_of_metadata_partitions, types=data_or_metadata)
    filtered.join_filter(list_of_metadata_partitions, types=data_or_metadata)


if __name__ == "__main__":
    extractor.extract_data(percentage_of_run, elements_running, list_metadata, list_data)
    for element in elements_running:
        execute_transform(data_or_metadata=element)


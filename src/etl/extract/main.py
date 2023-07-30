from extract import *


folder_metadata = "amazon_metadata"
folder_data = "amazon_reviews"

path_metadata_to_save = "sample/review_metadata"
path_data_to_save = "sample/review_data"

metadata_joblib = "downloaded_metadata.joblib"
data_joblib = "downloaded_data.joblib"

downloader = data_downloader()

def download_data():
    for folder_name, path_to_save, joblib_file in [[folder_data, path_data_to_save, data_joblib], [folder_metadata, path_metadata_to_save, metadata_joblib]]:
        downloader.download_compressed_data(folder_name, path_to_save, joblib_file)


if __name__ == "__main__":
   download_data()

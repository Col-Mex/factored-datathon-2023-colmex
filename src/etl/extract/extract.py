import gzip
import json
import joblib
import os
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient
from azure.core.credentials import AzureSasCredential
import time

import pandas as pd



class data_downloader():
    
    def __init__(self):
        storage_account_name = "safactoreddatathon"
        container_name = "source-files"
        sas_token = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=sas_token)
        self.file_system_client = service_client.get_file_system_client(file_system=container_name)    

    def download_compressed_data(self, folder_to_download, path_to_save, name_joblib_downloaded):
        print(f"Started downloading from {folder_to_download}")
        start_time = time.time()
        paths = self.file_system_client.get_paths(folder_to_download)

        counter = 0
        # Check if joblib exists with a list of names of files already downloaded. If not, create an empty list
        joblib_dreviews = os.path.join(path_to_save, name_joblib_downloaded)
        if os.path.exists(joblib_dreviews):
            downloaded_data = joblib.load(joblib_dreviews)
        else:
            downloaded_data = []

        for path in paths:
            path_name = path.name
            total_path = os.path.join(path_to_save, path_name)
            if path_name.endswith('.json.gz')  and path_name not in downloaded_data:
                file_client = self.file_system_client.get_file_client(path_name)
                os.makedirs(os.path.dirname(total_path), exist_ok=True)
                with open(total_path, "wb") as download_file:
                        data = file_client.download_file().readall()
                        download_file.write(data)
                        downloaded_data.append(path_name)
                        joblib.dump(downloaded_data, joblib_dreviews)
                counter += 1
                if counter % 500 == 0:
                    print(f"Downloaded {counter} files until now. Took {time.time() - start_time} seconds")
        hours, rem = divmod(time.time() - start_time, 3600)
        minutes, seconds = divmod(rem, 60)
        print(f"{counter} files downloaded in {int(hours)}:{int(minutes)}:{seconds} (h:m:s)")

    def descompress_data(self, list_files, path_files, output_path):
        sample_rev_1p = list_files
        dataframes = []
        for file in sample_rev_1p:
            with gzip.open(os.path.join(path_files,file), 'r') as fin:
                decompressed_data = fin.read().decode('utf-8')
            json_objects = [json.loads(line) for line in decompressed_data.splitlines() if line]
            df = pd.DataFrame(json_objects)
            dataframes.append(df)
        all_data = pd.concat(dataframes)

        # Save the DataFrame to a .parquet file
        all_data.to_parquet(output_path)
        


# download_compressed_data("amazon_metadata", "src/etl/extract", "downloaded_metadata.joblib")
# download_compressed_data("amazon_reviews", "src/etl/extract", "downloaded_reviews.joblib")
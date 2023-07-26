import gzip
import json
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient
from azure.core.credentials import AzureSasCredential


storage_account_name = "safactoreddatathon"
container_name = "source-files"
sas_token = "sp=rle&st=2023-07-25T18:12:36Z&se=2023-08-13T02:12:36Z&sv=2022-11-02&sr=c&sig=l2TCTwPWN8LSM922lR%2Fw78mZWQK2ErEOQDUaCJosIaw%3D"
file_path = "amazon_reviews"
service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
    "https", storage_account_name), credential=sas_token)
file_system_client = service_client.get_file_system_client(file_system=container_name)

# List all the files in the directory
paths = file_system_client.get_paths(file_path)

counter = 0
all_data = []

for path in paths:
    if not path.is_directory and path.name.endswith('.json.gz'):
        file_client = file_system_client.get_file_client(path.name)
        file_content = file_client.download_file().readall()
        
        # Decompress the gzip file and decode the bytes to string
        decompressed_data = gzip.decompress(file_content).decode('utf-8')
        
        # Each line contains a separate JSON object
        json_objects = [json.loads(line) for line in decompressed_data.splitlines() if line]

        # Save the json data to a local file
        file_name = 'reviews' + str(counter) + '.json'
        with open(file_name, 'w') as json_file:
            for json_obj in json_objects:
                json.dump(json_obj, json_file)
                json_file.write('\n')
        counter += 1
        if counter >=1:
            break

file_path = "amazon_metadata"
# List all the files in the directory
paths = file_system_client.get_paths(file_path)

counter = 0
all_data = []

for path in paths:
    if not path.is_directory and path.name.endswith('.json.gz'):
        file_client = file_system_client.get_file_client(path.name)
        file_content = file_client.download_file().readall()
        
        # Decompress the gzip file and decode the bytes to string
        decompressed_data = gzip.decompress(file_content).decode('utf-8')
        
        # Each line contains a separate JSON object
        json_objects = [json.loads(line) for line in decompressed_data.splitlines() if line]

        # Save the json data to a local file
        file_name = 'metadata' + str(counter) + '.json'
        with open(file_name, 'w') as json_file:
            for json_obj in json_objects:
                json.dump(json_obj, json_file)
                json_file.write('\n')
        counter += 1
        if counter >=1:
            break
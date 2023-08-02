import pyarrow.parquet as pq

import joblib
import gzip
import json
import numpy as np
import random
import pandas as pd
import os
from sqlalchemy import create_engine
import urllib
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# reviews_sample = pq.read_table('sample/review_data_sample/data_industry.parquet')
# metadata_sample = pq.read_table('sample/review_metadata_sample/metadata_industry.parquet')

# reviews_sample.to_pandas()
# metadata_sample.to_pandas()

## Login to Azure Key Vault

# replace 'my-key-vault-url' with the URL of your Azure Key Vault
VAULT_URL = 'https://kv-colmex.vault.azure.net/'

# DefaultAzureCredential will use the credentials of your logged-in Azure account



# list_reviews = joblib.load("../extract/downloaded_reviews.joblib")
# ejemplo = list_reviews[0]
# with gzip.open(os.path.join("../extract/",ejemplo), 'r') as fin:
#     decompressed_data = fin.read().decode('utf-8')
# json_objects = [json.loads(line) for line in decompressed_data.splitlines() if line]
# df = pd.DataFrame(json_objects)

# df = df.sample(10)
# df = df.reset_index().rename(columns={"index": "reviewID"}).drop(columns=["image", "style"])


class load_data():
    def __init__(self) -> None:
        self.credential = DefaultAzureCredential()
        
    def set_table_to_load(self, table_to_load):
        self.table_to_load = table_to_load
    
    def connect_to_database(self, table_name = "reviews_test"):
        # create a SecretClient
        client = SecretClient(vault_url=VAULT_URL, credential=self.credential)
        print(client.get_secret('db-server').value)
        print(client.get_secret('database').value)

        # parameters
        server = client.get_secret('db-server').value
        database = client.get_secret('database').value
        username = client.get_secret('db-username').value
        password = client.get_secret('db-password').value
        driver = '{ODBC Driver 17 for SQL Server}'
        self.table = table_name

        # create the connection string
        params = urllib.parse.quote_plus(
            f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')

        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    def load_data(self, if_exists="append", index=False):
        self.table_to_load.to_sql(f'{self.table}', con=self.engine, if_exists=if_exists, index=index)


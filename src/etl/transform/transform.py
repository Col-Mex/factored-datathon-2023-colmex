
from src.etl.transform.gen_samples import *

import os
import pyarrow.parquet as pq
import pandas as pd
import os


# percentage_of_run = 0.05

# elements_running = ["metadata", "data"]

# list_metadata = os.listdir("sample/review_metadata/")
# list_data = os.listdir("sample/review_data/")



# # get ASINS and Category of metadata
# list_of_metadata_partitions = os.listdir('sample/review_metadata_sample/partitions/')
# list_of_data_partitions = os.listdir('sample/review_data_sample/partitions/')
# list_of_data_filtered = os.listdir('sample/review_data_sample/filtered/')
# files = ["asins", "categories", "main_cat"]

# 1. Convert to jsons
# 2. Convert to pandas?
# 3. Filter by asins
# 4. export


class data_sampling_from_local():
    def __init__(self) -> None:
        pass
    
    def extract_data(self, percentage_of_run, elements_running, list_metadata, list_data):
        for idx, element in enumerate([list_metadata, list_data]):
            len_element = len(element)
            batch_size = int(len_element*percentage_of_run)
            chunks = list(chunk_creator(element, batch_size))
            
            counter = 0
            for chunk in chunks:
                print("running chunk number {} for {}".format(counter, elements_running[idx]))
                extract_sample_from_jsons(chunk, "sample/review_{}/".format(elements_running[idx]), 0.05, f"sample/review_{elements_running[idx]}_sample/partitions/{elements_running[idx]}_sample_5p_{counter}.parquet", if_random=False)
                counter += 1
                print("done.")

class data_filtering():
    def __init__(self, stream=False):
        self.list_of_metadata_partitions = os.listdir('sample/review_metadata_sample/partitions/')
        self.list_of_data_partitions = os.listdir('sample/review_data_sample/partitions/')
        self.stream = stream
    
    def __extract_asins(self, metadata_df):
        """_summary_

        Args:
            metadata_df (pd DataFrame): metadata dataframe

        Returns:
            _type_: _description_
        """
        
        asins = metadata_df["asin"].tolist()
        category = metadata_df["category"].tolist()  
        category = [list(cat) for cat in category]  
        main_cat_temp = metadata_df["main_cat"].tolist()
        main_cat = []
        sub1 = "alt="
        sub2 = "/>"
        for element in main_cat_temp:
            try:
                idx1 = element.index(sub1)
                idx2 = element.index(sub2)
                res = ''
                # getting elements in between
                for idx in range(idx1 + len(sub1) + 1, idx2):
                    res = res + element[idx]
                main_cat.append(res)
            except:
                main_cat.append(element)
        
        return asins, category, main_cat        
    
    def get_asins(self, list_of_partitions):
        """_summary_

        Args:
            list_of_partitions (list): List of paritions in metadata
        """
        asins = []
        categories = []
        main_cat = []
        for partition in list_of_partitions:
            print("running partition {}".format(partition))
            metadata_sample = pq.read_table(os.path.join('sample/review_metadata_sample/partitions/', partition))
            metadata_sample = metadata_sample.to_pandas()
            asins_temp, category_temp, main_cat_list = self.__extract_asins(metadata_sample)
        
            main_cat = main_cat + main_cat_list
            asins = asins + asins_temp
            categories = categories + category_temp
            print("done.")
            
        self.main_cat = main_cat
        self.asins = asins
        self.categories = categories
        
        return asins, main_cat

    def filter_asins(self, term = "Software"):
        filtered_asins = []
        filtered_categories = []
        
        for idx, c in enumerate(self.main_cat):
            if c == term:
                filtered_asins.append(self.asins[idx])
                filtered_categories.append(self.categories[idx])
        
        self.filtered_asins = filtered_asins
        self.filtered_categories = filtered_categories
        
        return filtered_asins


    def filter_data(self, industry_asins, list_of_reviewsdf=None):
        """_summary_

        Args:
            industry_asins (_type_): _description_
            list_of_reviewsdf (_type_, optional): _description_. Defaults to None.
        """
        data = []
        metadata = []
        if self.stream:
            for review_df in list_of_reviewsdf:
                data_sample = review_df[review_df["asin"].isin(industry_asins)]
                data.append(data_sample)
        
        else:
            partitions_for_both = {
                "data" : self.list_of_data_partitions,
                "metadata" : self.list_of_metadata_partitions
            }
            for idx, list_of_partitions in enumerate(list(partitions_for_both.values())):
                for partition in list_of_partitions:
                    type_of_data = list(partitions_for_both.keys())[idx]
                    data_sample = pq.read_table(os.path.join(f'sample/review_{type_of_data}_sample/partitions/', partition))
                    data_sample = data_sample.to_pandas()
                    data_sample = data_sample[data_sample["asin"].isin(industry_asins)]
                    if type_of_data == "data":                    
                        data.append(data_sample)
                    else:
                        metadata.append(data_sample)

        return data, metadata


    def save_list(self, element_to_save, metadata_or_data, element_name):
        file = open(F'sample/review_{metadata_or_data}_sample/{element_name}.txt','w')
        
        for item in element_to_save:
            file.write(str(item)+"\n")

        file.close()
        
    def load_list_of_asis(self, path):
        """load list of asins

        Args:
            path (str): path where is located the list of asins

        Returns:
            asins: list of asins of the industry
        """
        with open(path) as f:
            asins = f.readlines()

        return asins

    def join_filter(self, list_of_dfs):
        """join a list of dfs into a single df

        Args:
            list_of_dfs_filtered (_type_): _description_

        Returns:
            _type_: _description_
        """
        tables = []
        for df in list_of_dfs:
            tables.append(df)

        data = pd.DataFrame(columns = list(tables[0].columns))
        for table in tables:
            data = pd.concat([data, table], axis=0)

        data.to_parquet("sample/review_data_sample/data_industry.parquet")        
        
        return data

# asins, categories, main_cat = get_asins(list_of_metadata_partitions)
# filtered_asins, filtered_categories = filter_asins(asins, categories, main_cat, term = "Software")
# filter_data(filtered_asins, list_of_data_partitions)

# join_filter(list_of_data_filtered)

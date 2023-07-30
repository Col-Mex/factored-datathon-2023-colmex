
from gen_samples import *

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



class data_extraction():
    def __init__(self) -> None:
        pass
    
    def extract_data(self,percentage_of_run, elements_running, list_metadata, list_data):
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
    def __init__(self) -> None:
        pass    

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
            asins_temp = metadata_sample["asin"].tolist()
            category_temp = metadata_sample["category"].tolist()  
            category_temp = [list(cat) for cat in category_temp]  
            main_cat_temp = metadata_sample["main_cat"].tolist()
            main_cat_list = []
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
                    main_cat_list.append(res)
                except:
                    main_cat_list.append(element)
            self.main_cat = main_cat + main_cat_list
            self.asins = asins + asins_temp
            self.categories = categories + category_temp
            print("done.")



    def filter_asins(self, term = "Software"):
        filtered_asins = []
        filtered_categories = []
        
        for idx, c in enumerate(self.main_cat):
            if c == term:
                filtered_asins.append(self.asins[idx])
                filtered_categories.append(self.categories[idx])
        
        self.filtered_asins = filtered_asins
        self.filtered_categories = filtered_categories


    def filter_data(self, list_of_partitions):
        """_summary_

        Args:
            list_of_partitions (list): List of paritions in data
        """
        for partition in list_of_partitions:
            # Load parquet
            data_sample = pq.read_table(os.path.join('sample/review_data_sample/partitions/', partition))
            # Convert to Pandas
            data_sample = data_sample.to_pandas()
            # Filter by list of asins
            data_sample = data_sample[data_sample["asin"].isin(filtered_asins)]
            # Save Parquet
            data_sample.to_parquet(os.path.join('sample/review_data_sample/filtered', partition))

    def save_list(self, element_to_save, metadata_or_data, element_name):
        file = open(F'sample/review_{metadata_or_data}_sample/{element_name}.txt','w')
        
        for item in element_to_save:
            file.write(str(item)+"\n")

        file.close()


    def join_filter(self, list_of_filtered):
        tables = []
        for filtered in list_of_filtered:
            data_sample = pq.read_table(os.path.join('sample/review_data_sample/filtered/', filtered))
            data_sample = data_sample.to_pandas()
            tables.append(data_sample)

        filtered_data = pd.DataFrame(columns = list(tables[0].columns))
        for table in tables:
            filtered_data = pd.concat([filtered_data, table], axis=0)
        
        filtered_data.to_parquet("sample/review_data_sample/data_industry.parquet")



# asins, categories, main_cat = get_asins(list_of_metadata_partitions)
# filtered_asins, filtered_categories = filter_asins(asins, categories, main_cat, term = "Software")
# filter_data(filtered_asins, list_of_data_partitions)

# join_filter(list_of_data_filtered)

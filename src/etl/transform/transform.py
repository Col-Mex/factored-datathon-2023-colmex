
from src.etl.transform.gen_samples import *

import os
import pyarrow.parquet as pq
import pandas as pd
import json
import os

from transformers import pipeline

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

    def load_asins(self, path):
        with open(path) as f:
            asins = json.load(f)
        
        return asins
    
    def convert_strlist_to_dataframe(self, list_of_strings):
        
        try:
            json_objects = [json.loads(str(line)) for line in list_of_strings if line]
        except:
            json_objects = list_of_strings
        
        data = pd.DataFrame(json_objects)

        return data

    def filter_data(self, industry_asins, list_of_reviewsdf=None):
        """_summary_

        Args:
            industry_asins (_type_): _description_
            list_of_reviewsdf (_type_, optional): _description_. Defaults to None.
        Returns;
            data (list): list of dataframes of reviews
            metadata (list): list of dataframes of metadata
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
        
    # def load_list_of_asis(self, path):
    #     """load list of asins

    #     Args:
    #         path (str): path where is located the list of asins

    #     Returns:
    #         asins: list of asins of the industry
    #     """
    #     with open(path) as f:
    #         asins = f.readlines()

    #     return asins

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

        #data.to_parquet("sample/review_data_sample/data_industry.parquet")        
        
        return data
    
    def select_columns(self, data):
        # Create column of review ID 
        data['reviewID'] = data['reviewerID'] + "-" + data['asin'] + "-" + data['unixReviewTime']
        # Create date column based on column unixReviewTime
        data['dateReview'] = pd.to_datetime(data['unixReviewTime'].astype('int'), unit='s')
        # Select only necessary columns
        data = data[['asin', 'overall', 'reviewText', 'reviewerID', 'reviewerName', 'summary', 'unixReviewTime', 'verified', 'vote']]
        
        return data
    
    def apply_model_to_data(self, data):
        
        self.__load_model()
        
        sentiment = data.apply(self.__safe_sentiment_task, axis=1)
        emotion = data.apply(self.__safe_emotion_task, axis=1)
        
        data[['sentiment', 'sentiment_score']] = pd.DataFrame(sentiment.to_list(), index=data.index)
        data[['emotion', 'emotion_score']] = pd.DataFrame(emotion.to_list(), index=data.index)
        
        # Drop columns of scores
        data.drop(columns=['sentiment_score', 'emotion_score'], inplace=True)
        
        expected_columns = ['asin', 'overall', 'reviewText', 'reviewerID', 'reviewerName',
                'summary', 'unixReviewTime', 'verified', 'vote', 'reviewID',
                'dateReview', 'sentiment', 'emotion']

        data = data[expected_columns]
        
        # Drop duplicates
        data.drop_duplicates(subset=['reviewID'], inplace=True)
        
        return data

    def __load_model(self):
        # Load ML models for sentiment and emotion recognition on base of text
        model_sentiment = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        self.sentiment_task = pipeline("sentiment-analysis", model=model_sentiment, tokenizer=model_sentiment)
        self.model_sentiment = model_sentiment

        model_emotion = "j-hartmann/emotion-english-distilroberta-base"
        self.emotion_task = pipeline("text-classification", model=model_emotion, return_all_scores=False)
        self.model_emotion = model_emotion
   
    def __safe_sentiment_task(self, row):
        
         # In case the review text is too long, use the summary to detect sentiment
        try:
            return tuple(self.sentiment_task(row['reviewText'])[0].values())
        except RuntimeError:
            try:
                return tuple(self.sentiment_task(row['summary'], **{"truncation": True, "max_length": 512})[0].values())
            except (RuntimeError, IndexError):
                    return tuple('neutral', 0)


    def __safe_emotion_task(self, row):
        
        # The same for emotion
        try:
            return tuple(self.emotion_task(row['reviewText'])[0].values())
        except RuntimeError:
            try:
                return tuple(self.emotion_task(row['summary'], **{"truncation": True, "max_length": 512})[0].values())
            except (RuntimeError, IndexError):
                    return tuple('neutral', 0)
        
        

# asins, categories, main_cat = get_asins(list_of_metadata_partitions)
# filtered_asins, filtered_categories = filter_asins(asins, categories, main_cat, term = "Software")
# filter_data(filtered_asins, list_of_data_partitions)

# join_filter(list_of_data_filtered)

from src.etl.extract.extract_stream import extract
from src.etl.transform.transform import data_filtering, model
from src.etl.load.load import load_data

import multiprocessing

data_filter = data_filtering(stream=True)
loader = load_data()

sentimenter = model()
sentimenter.load_model()


def run_model(data):
    reviews_and_summary = data[["reviewText", "summary"]].values.tolist()
    pool_obj = multiprocessing.Pool(processes=10)
    table = pool_obj.map(sentimenter.apply_model_to_data, reviews_and_summary)
    pool_obj.close()
    sentiment = [i[0][0]for i in table]
    emotion = [i[1][0]for i in table]
    data["sentiment"] = sentiment
    data["emotion"] = emotion
    return data


def execute_pipeline():
    #Extractiong data
    print("Extract data")
    streaming_data = extract()
    
    # Transforming data
    print("Transform data")
    data = data_filter.convert_strlist_to_dataframe(streaming_data)
    
    industry_asins = data_filter.load_asins("jsons/asins_music_instruments.txt")
    data, __ = data_filter.filter_data(industry_asins, [data])
    data = data_filter.join_filter(data)
    data = data_filter.select_columns(data)
    
    ## Run model

    data = run_model(data)

    print("Load data")
    #Loading data
    loader.connect_to_database(table_name = "reviews_test")
    loader.set_table_to_load(data)
    loader.load_data(if_exists="append", index=False)

if __name__ == "__main__":
    execute_pipeline()

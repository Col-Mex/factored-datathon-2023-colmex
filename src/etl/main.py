from extract.extract_stream import extract
from transform.transform import data_filtering, model
from load.load import load_data

import multiprocessing

# Run this code from main path of repo (above src folder) with the following command "python src/etl/main.py"

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

# 4311954456
def execute_pipeline():
    #Extractiong data
    print("Extract data")
    streaming_data = extract()
    
    # Transforming data
    print("Transform data")
    data = data_filter.convert_strlist_to_dataframe(streaming_data)
    
    industry_asins = data_filter.load_asins("jsons/asins_music_instruments.txt")
    industry_brands = data_filter.load_asins("jsons/brands_music_instruments.txt")
    selected_brands = data_filter.load_asins("jsons/brands.txt")

    data, __ = data_filter.filter_data(industry_asins, [data])
    data = data_filter.filter_brands(selected_brands, industry_brands, industry_asins, data[0])
    
    data = data_filter.join_filter([data])
    data = data_filter.select_columns(data)
    
    ## Run model

    data = run_model(data)

    print("Load data")
    #Loading data
    #loader.connect_to_database(table_name = "reviews_test")
    #loader.set_table_to_load(data)
    #loader.load_data(if_exists="append", index=False)
    print("Done.")

if __name__ == "__main__":
    execute_pipeline()

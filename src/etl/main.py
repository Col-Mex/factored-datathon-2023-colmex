from src.etl.extract.extract_stream import extract
from src.etl.transform.transform import data_filtering
from src.etl.load.load import load_data

data_filter = data_filtering(stream=True)
loader = load_data()


def execute_pipeline():
    #Extractiong data
    print("Extract data")
    streaming_data = extract()
    
    # Transforming data
    print("Transform data")
    data = data_filter.convert_strlist_to_dataframe(streaming_data)
    
    industry_asins = data_filter.load_asins("jsons/software_asins.json")
    data, __ = data_filter.filter_data(industry_asins, [data])
    data = data_filter.join_filter(data)
    data = data_filter.select_columns(data)
    data = data_filter.apply_model_to_data(data)

    print("Load data")
    #Loading data
    loader.connect_to_database(table_name = "reviews_test")
    loader.set_table_to_load(data)
    loader.load_data(if_exists="append", index=False)

if __name__ == "__main__":
    execute_pipeline()

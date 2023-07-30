import pyarrow.parquet as pq
import pandas as pd
import os



def get_asins(list_of_partitions):

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
        main_cat = main_cat + main_cat_list
        asins = asins + asins_temp
        categories = categories + category_temp
        print("done.")
        
    return asins, categories, main_cat


def filter_asins(asins, categories, main_cat, term = "Software"):
    filtered_asins = []
    filtered_categories = []
    
    for idx, c in enumerate(main_cat):
        if c == term:
            filtered_asins.append(asins[idx])
            filtered_categories.append(categories[idx])
    
    return filtered_asins, filtered_categories

def filter_data(filtered_asins, list_of_partitions):
    for partition in list_of_partitions:
        # Load parquet
        data_sample = pq.read_table(os.path.join('sample/review_data_sample/partitions/', partition))
        # Convert to Pandas
        data_sample = data_sample.to_pandas()
        # Filter by list of asins
        data_sample = data_sample[data_sample["asin"].isin(filtered_asins)]
        # Save Parquet
        data_sample.to_parquet(os.path.join('sample/review_data_sample/filtered', partition))

def save_list(element_to_save, metadata_or_data, element_name):
    file = open(F'sample/review_{metadata_or_data}_sample/{element_name}.txt','w')
    
    for item in element_to_save:
        file.write(str(item)+"\n")

    file.close()


def join_filter(list_of_filtered):
    tables = []
    for filtered in list_of_filtered:
        data_sample = pq.read_table(os.path.join('sample/review_data_sample/filtered/', filtered))
        data_sample = data_sample.to_pandas()
        tables.append(data_sample)

    filtered_data = pd.DataFrame(columns = list(tables[0].columns))
    for table in tables:
        filtered_data = pd.concat([filtered_data, table], axis=0)
    
    filtered_data.to_parquet("sample/review_data_sample/data_industry.parquet")


"""
file = open('sample/review_metadata_sample/categories.txt','w')
for item in categories:
	file.write(str(item)+"\n")

file.close()

file = open('sample/review_metadata_sample/main_cat.txt','w')
for item in main_cat:
	file.write(str(item)+"\n")

file.close()
"""

"""
partition = list_of_partitions[0]

#reviews_sample = pq.read_table('sample/review_data_sample/reviews_sample_5p.parquet')
metadata_sample = metadata_sample = pq.read_table(os.path.join('sample/review_metadata_sample/partitions/', partition))

#reviews_sample = reviews_sample.to_pandas()
metadata_sample = metadata_sample.to_pandas()

main_cat_temp = metadata_sample["main_cat"].tolist()

sub1 = "alt="
sub2 = "/>"

main_cat_list = []
for element in main_cat_temp:
    try:
        idx1 = element.index("alt=")
        idx2 = element.index("/>")
        res = ''
        # getting elements in between
        for idx in range(idx1 + len(sub1) + 1, idx2):
            res = res + element[idx]
        main_cat_list.append(res)
    except:
        main_cat_list.append(element)


main_cat_list[:70]
set(main_cat_list)

#reviews_sample["asin"]
asins_temp = metadata_sample["asin"].tolist()
category_temp = metadata_sample["category"].tolist()
"""

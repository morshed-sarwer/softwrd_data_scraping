from pymongo import MongoClient
import pandas as pd
def upload_to_mongo(file_name, collection_name):
    
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['softwrd']
        collection = db[collection_name]
        csv = "data/"+file_name
        df = pd.read_csv(csv)
        data_dict_list = df.to_dict(orient='records')
        for data_dict in data_dict_list:
            if not collection.find_one(data_dict):
                collection.insert_one(data_dict)
        client.close()
        # print(csv)
    except Exception as e:
        print(f"error {e}")
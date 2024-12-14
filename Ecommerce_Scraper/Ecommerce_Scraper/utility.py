import pymongo
from pymongo import UpdateOne
from Ecommerce_Scraper.settings import MONGO_DATABASE, MONGO_URI
# from settings import MONGO_DATABASE, MONGO_URI


class MongoDBHandler:
    def __init__(self, mongo_uri, mongo_db):
        """
        Initialize the MongoDBHandler with connection details.
        """
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None

    def connect(self):
        """
        Establish a connection to the MongoDB server.
        """
        if not self.client:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
    
    def close(self):
        """
        Close the connection to the MongoDB server.
        """
        if self.client:
            self.client.close()
            self.client = None
            self.db = None

    def insert_one(self, collection_name, data):
        """
        Insert a single document into the specified collection.
        """
        self.connect()
        return self.db[collection_name].insert_one(data)

    def insert_many(self, collection_name, data_list):
        """
        Insert multiple documents into the specified collection.
        """
        self.connect()
        return self.db[collection_name].insert_many(data_list)

    def update_one(self, collection_name, query, update, upsert=False):
        """
        Update/Insert a single document in the specified collection.
        """
        self.connect()
        return self.db[collection_name].update_one(query, {"$set": update}, upsert=upsert)
    
    def bulk_upsert(self, collection_name, df, filter_cols:list=['_id'], upsert=True):
        """
        Update/Insert many documents in the specified collection.
        """
        self.connect()
        operations = [
            UpdateOne(
                {col:row[col] for col in filter_cols},
                {'$set':row.to_dict()},
                upsert=upsert
            )
            for _, row in df.iterrows()
        ]
        return self.db[collection_name].bulk_write(operations)

    def find(self, collection_name, query=None, projection=None):
        """
        Retrieve documents from the specified collection.
        """
        self.connect()
        return self.db[collection_name].find(query or {}, projection)

    def delete_one(self, collection_name, query):
        """
        Delete a single document from the specified collection.
        """
        self.connect()
        return self.db[collection_name].delete_one(query)

    def delete_many(self, collection_name, query):
        """
        Delete multiple documents from the specified collection.
        """
        self.connect()
        return self.db[collection_name].delete_many(query)


def getCategoryUrls():
    mongo_handler = MongoDBHandler(MONGO_URI, MONGO_DATABASE)
    results_category = mongo_handler.find(
        "amz__product_category", 
        {'IsActive':True},
        {'Category':1, 'SubCategory':{'$literal':''}, 'Url':1, '_id':0}
    )

    results_subCategory = mongo_handler.find(
        "amz__product_subcategory", 
        {'IsActive':True},
        {'Category':1, 'SubCategory':1, 'Url':1, '_id':0}
    )
    # print(list(results_category),list(results_subCategory))
    results = list(results_category)+list(results_subCategory)
    return results if results else []

def remove_suffixes(df, *args):
    for i in args:
        df = df.rename(columns={col: col.replace(i,'') for col in df.columns})
    return df

def add_suffixes(df, suffix):
    return df.rename(columns={col: col+suffix for col in df.columns})
import pymongo

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
        Update a single document in the specified collection.
        """
        self.connect()
        return self.db[collection_name].update_one(query, {"$set": update}, upsert=upsert)

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

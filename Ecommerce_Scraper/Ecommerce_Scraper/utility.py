# import pymongo
# from pymongo import UpdateOne
import psycopg2
from scrapy.utils.project import get_project_settings
from psycopg2.extras import RealDictCursor, execute_values
import re

# from settings import MONGO_DATABASE, MONGO_URI


# class MongoDBHandler:
#     def __init__(self, mongo_uri, mongo_db):
#         """
#         Initialize the MongoDBHandler with connection details.
#         """
#         self.mongo_uri = mongo_uri
#         self.mongo_db = mongo_db
#         self.client = None
#         self.db = None

#     def connect(self):
#         """
#         Establish a connection to the MongoDB server.
#         """
#         if not self.client:
#             self.client = pymongo.MongoClient(self.mongo_uri)
#             self.db = self.client[self.mongo_db]
    
#     def close(self):
#         """
#         Close the connection to the MongoDB server.
#         """
#         if self.client:
#             self.client.close()
#             self.client = None
#             self.db = None

#     def insert_one(self, collection_name, data):
#         """
#         Insert a single document into the specified collection.
#         """
#         self.connect()
#         return self.db[collection_name].insert_one(data)

#     def insert_many(self, collection_name, data_list):
#         """
#         Insert multiple documents into the specified collection.
#         """
#         self.connect()
#         return self.db[collection_name].insert_many(data_list)

#     def update_one(self, collection_name, query, update, upsert=False):
#         """
#         Update/Insert a single document in the specified collection.
#         """
#         self.connect()
#         return self.db[collection_name].update_one(query, {"$set": update}, upsert=upsert)
    
#     def bulk_upsert(self, collection_name, df, filter_cols:list=['_id'], upsert=True):
#         """
#         Update/Insert many documents in the specified collection.
#         """
#         self.connect()
#         operations = [
#             UpdateOne(
#                 {col:row[col] for col in filter_cols},
#                 {'$set':{k:v for k,v in row.to_dict().items() if k not in filter_cols}},
#                 upsert=upsert
#             )
#             for _, row in df.iterrows()
#         ]
#         return self.db[collection_name].bulk_write(operations)

#     def find(self, collection_name, query=None, projection=None):
#         """
#         Retrieve documents from the specified collection.
#         """
#         self.connect()
#         return self.db[collection_name].find(query or {}, projection)

#     def delete_one(self, collection_name, query):
#         """
#         Delete a single document from the specified collection.
#         """
#         self.connect()
#         return self.db[collection_name].delete_one(query)

#     def delete_many(self, collection_name, query):
#         """
#         Delete multiple documents from the specified collection.
#         """
#         self.connect()
#         return self.db[collection_name].delete_many(query)

class PostgresDBHandler:
    def __init__(self, host, database, user, password, port=5432):
        """Initialize connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()

    def insert(self, table, data):
        """
        Insert a record into the specified table.
        
        Args:
            table (str): Name of the table.
            data (dict): Dictionary of column-value pairs to insert.
        """
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({values});"  # RETURNING id
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
                self.connection.commit()
                # return cursor.fetchone()[0]
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error inserting into {table}: {e}")
            raise

    def read(self, table, columns='*', conditions=None):
        """
        Read records from the specified table.
        
        Args:
            table (str): Name of the table.
            columns (str or list): Columns to retrieve (default is '*').
            conditions (str): WHERE clause conditions (optional).
        
        Returns:
            list[dict]: List of retrieved records as dictionaries.
        """
        if isinstance(columns, list):
            columns = ', '.join(columns)
        query = f"SELECT {columns} FROM {table}"
        if conditions:
            query += f" WHERE {conditions}"
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error reading from {table}: {e}")
            raise

    def update(self, table, data, conditions):
        """
        Update records in the specified table.
        
        Args:
            table (str): Name of the table.
            data (dict): Dictionary of column-value pairs to update.
            conditions (str): WHERE clause conditions.
        """
        set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {conditions}"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
                self.connection.commit()
                return cursor.rowcount  # Number of rows updated
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error updating {table}: {e}")
            raise

    def bulk_upsert(self, table, data, conflict_columns, update_columns):
        """
        Perform a bulk upsert operation.

        Args:
            table (str): Name of the table.
            data (list[dict]): List of dictionaries, where keys are column names, and values are the row data.
            conflict_columns (list[str]): List of columns to check for conflicts (e.g., primary or unique keys).
            update_columns (list[str]): List of columns to update on conflict.
        """
        # Ensure data is provided
        if not data:
            raise ValueError("No data provided for upsert.")

        # Generate the SQL parts dynamically
        columns = data[0].keys()  # Assume all rows have the same keys
        column_list = ", ".join(columns)
        value_placeholders = ", ".join([f"%({col})s" for col in columns])

        conflict_clause = ", ".join(conflict_columns)
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])

        query = f"""
            INSERT INTO {table} ({column_list})
            VALUES ({value_placeholders})
            ON CONFLICT ({conflict_clause})
            DO UPDATE SET
            {update_clause};
        """

        try:
            with self.connection.cursor() as cursor:
                # Use execute_values for efficiency
                execute_values(cursor, query, data)
                self.connection.commit()
                print(f"Bulk upsert completed for {len(data)} rows.")
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error during bulk upsert: {e}")
            raise
    
    # def execute_stored_procedure(self, procedure_name):
    #     """
    #     Execute a stored procedure in PostgreSQL.

    #     Args:
    #         procedure_name (str): The name of the stored procedure, including the schema (e.g., 'processed.sp_update_master_tables').
    #     """
    #     try:
    #         with self.connection.cursor() as cursor:
    #             # Execute the stored procedure
    #             cursor.execute(f"CALL {procedure_name}();")
    #             self.connection.commit()
    #             print(f"Stored procedure {procedure_name} executed successfully.")
    #     except psycopg2.Error as e:
    #         self.connection.rollback()
    #         print(f"Error executing stored procedure {procedure_name}: {e}")
    #         raise
    
    def execute(self, query, type=None):
        """
        Execute a query in PostgreSQL.

        Args:
            query (str): The whole sql query to be executed (e.g. select * from processed.amz__product_category)
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Execute the stored procedure
                cursor.execute(query)
                self.connection.commit()
                print(f"Query executed successfully.")
                if type is None:
                    return cursor.fetchall()
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error executing query {query}: {e}")
            raise
    
    def delete(self, table, conditions):
        """
        Delete records from the specified table.
        
        Args:
            table (str): Name of the table.
            conditions (str): WHERE clause conditions.
        """
        query = f"DELETE FROM {table} WHERE {conditions}"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
                return cursor.rowcount  # Number of rows deleted
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error deleting from {table}: {e}")
            raise

def getCategoryUrls(db='mongo'):
    settings = get_project_settings()
    if db == 'mongo':
        mongo_handler = MongoDBHandler(
            settings.get('MONGO_URI'),
            settings.get('MONGO_DATABASE'))
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
    elif db == 'postgres':
        postgres_handler = PostgresDBHandler(
                settings.get('POSTGRES_HOST'),
                settings.get('POSTGRES_DATABASE'),
                settings.get('POSTGRES_USERNAME'),
                settings.get('POSTGRES_PASSWORD'),
                settings.get('POSTGRES_PORT'),
            )
        postgres_handler.connect()
        if settings.get('BEST_SELLER_LOAD_TYPE') == 'INCREMENTAL':
            return postgres_handler.execute(
                """
                SELECT category, '' as sub_category, url FROM curated.amz__product_category 
                WHERE is_active = true AND cast(last_refreshed_timestamp as date) < CURRENT_DATE
                UNION
                SELECT category, sub_category, url FROM curated.amz__product_subcategory 
                WHERE is_active = true AND cast(last_refreshed_timestamp as date) < CURRENT_DATE
                """
            )
        elif settings.get('BEST_SELLER_LOAD_TYPE') == 'FULLREFRESH':
            return postgres_handler.execute(
                """
                select category, '' as sub_category, url from curated.amz__product_category WHERE is_active = true
                union 
                select category, sub_category, url from curated.amz__product_subcategory WHERE is_active = true
                """
            )   
        
def remove_suffixes(df, *args):
    for i in args:
        df = df.rename(columns={col: col.replace(i,'') for col in df.columns})
    return df

def add_suffixes(df, suffix):
    return df.rename(columns={col: col+suffix for col in df.columns})

def getUrlToScrap(db:'mongo | postgres'='mongo'):
    settings = get_project_settings()
    if db == 'mongo':
        mongo_handler = MongoDBHandler(
            settings.get('MONGO_URI'),
            settings.get('MONGO_DATABASE'))
        urls = mongo_handler.find(
            "trf_amz__best_sellers", 
            {'isLatest':True},
            {'productUrl':1, '_id':0}
        )
        return list(urls)
    elif db == 'postgres':
        postgres_handler = PostgresDBHandler(
                settings.get('POSTGRES_HOST'),
                settings.get('POSTGRES_DATABASE'),
                settings.get('POSTGRES_USERNAME'),
                settings.get('POSTGRES_PASSWORD'),
                settings.get('POSTGRES_PORT'),
            )
        postgres_handler.connect()
        if settings.get('PRODUCT_LOAD_TYPE') == 'INCREMENTAL':
            return postgres_handler.execute(
                """
                select distinct product_url 
                from curated.amz__best_sellers 
                where asin not in (select distinct asin from curated.amz__product_details where is_latest is True)
                """
            )
        elif settings.get('PRODUCT_LOAD_TYPE') == 'FULLREFRESH':
            return postgres_handler.execute(
                """
                select distinct product_url 
                from curated.amz__best_sellers 
                """
            )

def extract_numeric_part(value):
    if isinstance(value, str):
        # extract the numeric part
        x = [i for i in value.split(' ') if re.search(r'\d', i)]
        if len(x) > 1:
            raise Exception(f"Expected One Numeric Part, got {len(x)}:{x}")
        # remove special characters, exclude: decimal and alphanumeric letters
        if x:
            x = re.sub(r'[^a-zA-Z0-9\s\.]', '', x[0]).lower()
            if x[-1] == 'k':
                x = float(x.replace('k',''))*1000
            elif x[-1] == 'l':
                x = float(x.replace('l',''))*1_000_000
            else:
                x = float(x)
            return x
        return None
    
def safe_strip(value):
    return value.strip() if isinstance(value, str) else value
import traceback
import pyodbc
from dotenv import load_dotenv
import os


def printArgs(*args):
    for i in args:
        print(i)
     
def safe_get_element_value(driver, by, value, mode="text", attribute=None, default=None):
    try:
        element = driver.find_element(by, value)
        if mode == "text":
            element = element.text.strip()
        elif mode == "attribute" and attribute:
            element = element.get_attribute(attribute)
        else:
            raise ValueError("Invalid mode or attribute not specified.")
        element = element if element else default
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e)
        if 'NoSuchElementException' in tb.exc_type_str:
            element = default
        else:
            raise Exception (e)
    finally:
        return element

class SQLServerExpress:
    def __init__(self, estd_conn = False):
        """
        Initialize the connection to SQL Server Express.

        :param server: Server name or IP (e.g., "localhost\\SQLEXPRESS").
        :param database: Database name.
        :param username: Username for SQL authentication.
        :param password: Password for SQL authentication.
        """
        # Load .env file
        load_dotenv()

        # Access environment variables        
        self.server = os.getenv('SQL_SERVER_INSTANCE')
        self.database = os.getenv('SQL_DATABASE_NAME')
        self.username = os.getenv('DATABASE_USERNAME')
        self.password = os.getenv('DATABASE_PASSWORD')
        # print(self.server, self.database, self.username, self.password, sep='\n')
        
        if estd_conn:
            self.connect()
        else:
            self.connection = None

    def connect(self):
        """Establish a connection to the SQL Server Express database."""
        try:
            self.connection = pyodbc.connect(
                f"""DRIVER={{ODBC Driver 17 for SQL Server}};
                SERVER={self.server};
                DATABASE={self.database};
                UID={self.username};
                PWD={self.password}"""
            )
            print("Connection established successfully.")
        except pyodbc.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Connection closed.")

    def execute_query(self, query, params=None):
        """
        Execute a query (INSERT, UPDATE, DELETE).

        :param query: SQL query string with placeholders for parameters.
        :param params: Parameters for the query as a tuple (optional).
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            self.connection.commit()
            print("Query executed successfully.")
            cursor.close()
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            raise
        
    def execute_many_query(self, query, params=None):
        """
        Execute a query (INSERT, UPDATE, DELETE).

        :param query: SQL query string with placeholders for parameters.
        :param params: Parameters for the query as a tuple (optional).
        """
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, params or ())
            cursor.commit()
            print("Query executed successfully.")
            cursor.close()
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            raise

    def fetch_all(self, query, params=None):
        """
        Execute a SELECT query and fetch all results.

        :param query: SQL SELECT query string with placeholders for parameters.
        :param params: Parameters for the query as a tuple (optional).
        :return: List of results.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            cursor.close()
            return results
        except pyodbc.Error as e:
            print(f"Error fetching data: {e}")
            raise

    def fetch_one(self, query, params=None):
        """
        Execute a SELECT query and fetch a single result.

        :param query: SQL SELECT query string with placeholders for parameters.
        :param params: Parameters for the query as a tuple (optional).
        :return: Single result row.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            result = cursor.fetchone()
            cursor.close()
            return result
        except pyodbc.Error as e:
            print(f"Error fetching data: {e}")
            raise

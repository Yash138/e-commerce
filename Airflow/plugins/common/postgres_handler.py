import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

class PostgresDBHandler:
    def __init__(self, host, database, user, password, port=5432):
        """Initialize connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None # Ensure connection is initialized to None

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            # Only connect if there's no existing connection or it's closed
            if self.connection is None or self.connection.closed != 0: # psycopg2.connection.closed is 0 for open, 1 for closed
                self.connection = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    port=self.port
                )
                return self.connection
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise # Re-raise the exception after printing

    def close(self):
        """Close the database connection."""
        if self.connection and self.connection.closed == 0: # Only close if connection exists and is open
            self.connection.close()
            self.connection = None # Set to None after closing

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

    def read(self, table=None, columns='*', conditions=None, query = None):
        """
        Read records from the specified table.
        
        Args:
            table (str): Name of the table.
            columns (str or list): Columns to retrieve (default is '*').
            conditions (str): WHERE clause conditions (optional).
            query (str): Custom SQL query to execute (optional).
        Returns:
            list[dict]: List of retrieved records as dictionaries.
        """
        if not query:
            if isinstance(columns, list):
                columns = ', '.join(columns)
            query = f"SELECT {columns} FROM {table}"
            if conditions:
                query += f" WHERE {conditions}"
        else:
            query = query.strip()
            table = query.split(' ')[query.lower().split(' ').index('from') + 1]
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error reading from {table}: {e}")
            raise
    
    def stream_read(self, table=None, columns='*', conditions=None, query=None, batch_size=1000):
        """
        Stream large result sets from the database in chunks.
        
        Args:
            table (str): Name of the table.
            columns (str or list): Columns to retrieve (default is '*').
            conditions (str): WHERE clause conditions (optional).
            batch_size (int): Number of rows to fetch per iteration (default is 1000).
        
        Yields:
            dict: A row from the result set.
        """
        if not query:
            if isinstance(columns, list):
                columns = ', '.join(columns)
            query = f"SELECT {columns} FROM {table}"
            if conditions:
                query += f" WHERE {conditions}"
        else:
            query = query.strip()
            table = query.split(' ')[query.lower().split(' ').index('from') + 1]
        
        try:
            with self.connection.cursor(name="stream_cursor", cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        yield row
        except psycopg2.Error as e:
            print(f"Error streaming from {table}: {e}")
            raise

    def update(self, table, data, conditions):
        """
        Update records in the specified table.
        
        Args:
            table (str): Name of the table.
            data (dict): Dictionary of column-value pairs to update.
            conditions (dict): WHERE conditions.
        """
        set_clause = ', '.join([f"{col} = %s" for col in data.keys()])
        conditions_clause = ' AND '.join([f"{col} = '{value}'" for col, value in conditions.items()])
        query = f"UPDATE {table} SET {set_clause} WHERE {conditions_clause}"
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, tuple(data.values()))
                self.connection.commit()
                return cursor.rowcount  # Number of rows updated
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error updating {table}: {e}")
            raise

    def bulk_upsert(self, table, data, conflict_columns, update_columns=None):
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
        values = [tuple(record[col] for col in columns) for record in data]
        column_list = ", ".join(columns)
        # value_placeholders = ", ".join([f"%({col})s" for col in columns])
        # value_placeholders = ", ".join([f"{col}" for col in columns])

        conflict_clause = ", ".join(conflict_columns)
        
        if update_columns:
            update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        else:
            # Update all columns except the conflict columns
            update_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns]
            )
        
        query = f"""
            INSERT INTO {table} ({column_list})
            VALUES %s 
            ON CONFLICT ({conflict_clause})
            DO UPDATE SET {update_clause};
        """
        
        try:
            with self.connection.cursor() as cursor:
                # Use execute_values for efficiency
                execute_values(cursor, query, values)
                self.connection.commit()
                print(f"Bulk upsert completed for {len(data)} rows.")
        except psycopg2.Error as e:
            self.connection.rollback()
            raise Exception(f"Error during bulk upsert: {e}")
            
    
    def execute(self, query):
        """
        Execute CRUD statements in PostgreSQL.

        Args:
            query (str): The whole sql query to be executed (e.g. select * from processed.amz__product_category)
        """
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Execute the stored procedure
                cursor.execute(query)
                self.connection.commit()
                print(f"Query executed successfully.")
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
    
    # --- ADD THESE TWO METHODS TO MAKE IT A CONTEXT MANAGER ---
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
import asyncpg
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

class AsyncPostgresDBHandler:
    def __init__(self, host, database, user, password, port=5432):
        """Initialize connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None

    async def connect(self):
        """Establish an asynchronous connection to the PostgreSQL database."""
        try:
            self.connection = await asyncpg.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
        except asyncpg.PostgresError as e:
            print(f"Error connecting to PostgreSQL database: {e}")
            raise

    async def close(self):
        """Close the database connection."""
        if self.connection:
            await self.connection.close()

    async def insert(self, table, data):
        """
        Insert a record into the specified table.
        
        Args:
            table (str): Name of the table.
            data (dict): Dictionary of column-value pairs to insert.
        """
        columns = ', '.join(data.keys())
        # asyncpg uses $1, $2, ... as placeholders
        placeholders = ', '.join(f'${i}' for i in range(1, len(data) + 1))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
        try:
            await self.connection.execute(query, *tuple(data.values()))
        except asyncpg.PostgresError as e:
            print(f"Error inserting into {table}: {e}")
            raise

    async def read(self, table=None, columns='*', conditions=None, query=None):
        """
        Read records from the specified table.
        
        Args:
            table (str): Name of the table.
            columns (str or list): Columns to retrieve (default is '*').
            conditions (str): WHERE clause conditions (optional).
        
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
            records = await self.connection.fetch(query)
            # Convert asyncpg.Record objects to dicts
            return [dict(record) for record in records]
        except asyncpg.PostgresError as e:
            print(f"Error reading from {table}: {e}")
            raise
    
    async def stream_read(self, query):
        """
        Read records from the specified table in a streaming fashion.
        
        Args:
            query (str): The full SQL query to execute.
        
        Returns:
            asyncpg.Record: An asyncpg Record object.
        """
        try:
            async for record in self.connection.transaction():
                async for record in self.connection.cursor(query):
                    yield record
        except asyncpg.PostgresError as e:
            print(f"Error streaming records: {e}")
            raise
    
    async def update(self, table, data, conditions):
        """
        Update records in the specified table.
        
        Args:
            table (str): Name of the table.
            data (dict): Dictionary of column-value pairs to update.
            conditions (str): WHERE clause conditions.
        
        Returns:
            int: Number of rows updated.
        """
        # Build the SET clause with placeholders
        set_clause = ', '.join([f"{col} = ${i}" for i, col in enumerate(data.keys(), start=1)])
        query = f"UPDATE {table} SET {set_clause} WHERE {conditions}"
        try:
            result = await self.connection.execute(query, *tuple(data.values()))
            # asyncpg returns a command status like "UPDATE 1"; extract the affected row count.
            rowcount = int(result.split()[-1])
            return rowcount
        except asyncpg.PostgresError as e:
            print(f"Error updating {table}: {e}")
            raise

    async def bulk_upsert(self, table, data, conflict_columns, update_columns=None):
        """
        Perform a bulk upsert operation.
        
        Args:
            table (str): Name of the table.
            data (list[dict]): List of dictionaries containing row data.
            conflict_columns (list[str]): Columns to check for conflicts.
            update_columns (list[str]): Columns to update on conflict.
        """
        if not data:
            raise ValueError("No data provided for upsert.")
        
        # Assume all rows have the same keys
        columns = list(data[0].keys())
        column_list = ", ".join(columns)
        placeholders = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
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
            VALUES ({placeholders})
            ON CONFLICT ({conflict_clause})
            DO UPDATE SET {update_clause};
        """
        try:
            # Prepare a list of tuples corresponding to each row's values in order.
            values_list = [tuple(item[col] for col in columns) for item in data]
            # Execute the statement for each tuple
            await self.connection.executemany(query, values_list)
            print(f"Bulk upsert completed for {len(data)} rows.")
        except asyncpg.PostgresError as e:
            print(f"Error during bulk upsert: {e}")
            raise

    async def execute(self, query, type=None):
        """
        Execute a query in PostgreSQL.
        
        Args:
            query (str): The full SQL query to execute.
            type: If None, returns fetched results; otherwise returns execution status.
        
        Returns:
            list[dict] or str: Fetched rows as a list of dictionaries, or the command status.
        """
        try:
            if type is None:
                records = await self.connection.fetch(query)
                return [dict(record) for record in records]
            else:
                result = await self.connection.execute(query)
                return result
        except asyncpg.PostgresError as e:
            print(f"Error executing query {query}: {e}")
            raise

    async def delete(self, table, conditions):
        """
        Delete records from the specified table.
        
        Args:
            table (str): Name of the table.
            conditions (str): WHERE clause conditions.
        
        Returns:
            int: Number of rows deleted.
        """
        query = f"DELETE FROM {table} WHERE {conditions}"
        try:
            result = await self.connection.execute(query)
            rowcount = int(result.split()[-1])
            return rowcount
        except asyncpg.PostgresError as e:
            print(f"Error deleting from {table}: {e}")
            raise

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

    def read(self, table=None, columns='*', conditions=None, query = None):
        """
        Read records from the specified table.
        
        Args:
            table (str): Name of the table.
            columns (str or list): Columns to retrieve (default is '*').
            conditions (str): WHERE clause conditions (optional).
        
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
        column_list = ", ".join(columns)
        value_placeholders = ", ".join([f"%({col})s" for col in columns])

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

if __name__ == "__main__":
    async def main():
        db = AsyncPostgresDBHandler('localhost', 'ecommerce', 'postgres', 'hsV6.sfi2', '5432')
        await db.connect()
        rows = await db.read("curated.amz__product_category")
        print(rows)
        await db.close()

    asyncio.run(main())

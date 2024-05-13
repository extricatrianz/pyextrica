from typing import List
from sqlalchemy import create_engine as _sqlalchemy_engine
from sqlalchemy import text as _sqlalchemy_text

class PyExtricaFunctions:
    @staticmethod
    def extrica_engine(*args, **kwargs):
        """
        Create a new engine instance optimized for Trino queries with PyExtrica.
        
        This function initializes a connection engine for PyExtrica, a powerful Python library designed specifically for seamless interaction with Trino, a distributed SQL query engine. PyExtrica simplifies and enhances the Trino querying experience by providing intuitive interfaces, efficient query execution, and comprehensive support for Trino-specific functionalities.

        :param \*args: Positional arguments that will be passed to
            :func:`sqlalchemy.create_engine`.
        :param \*\*kwargs: Keyword arguments that will be passed to
            :func:`sqlalchemy.create_engine`.
        
        .. seealso::
            :func:`sqlalchemy.create_engine` - Arguments and parameters accepted by the create_engine function.
        """
        return _sqlalchemy_engine(*args, **kwargs)

    @staticmethod
    def extrica_text(*args, **kwargs):
        """
        Create a text clause instance tailored for Trino queries with PyExtrica.
        
        This function returns a TextClause instance constructed using the provided SQL expression, optimized for compatibility with PyExtrica. PyExtrica is a Python library designed specifically for seamless interaction with Trino, a distributed SQL query engine. By using extrica_text, users can construct SQL expressions efficiently and effectively, leveraging the full power of Trino through PyExtrica's intuitive interfaces and comprehensive support for Trino-specific functionalities.
        
        :param \*args: Positional arguments that will be passed to
            :func:`sqlalchemy.text`.
        :param \*\*kwargs: Keyword arguments that will be passed to
            :func:`sqlalchemy.text`.
        
        .. seealso::
            :func:`sqlalchemy.text` - Arguments and parameters accepted by the text function.
        """
        return _sqlalchemy_text(*args, **kwargs)
    
    @staticmethod
    def get_catalog_names(engine) -> List[str]:
        """
        Retrieve catalog names using PyExtrica.

        This method simplifies the process of retrieving catalog names by wrapping the
        engine.dialect.get_catalog_names method and handling the connection management automatically.

        :param engine: The engine instance.
        :return: A list of catalog names.
        :raises Exception: If there is an error retrieving catalog names.
        """
        try:
            # Establish a connection using the provided engine
            with engine.connect() as connection:
                # Retrieve catalog names using the engine dialect method
                catalogs = engine.dialect.get_catalog_names(connection)
            return catalogs
        except Exception as e:
            # Raise an exception if there is an error
            raise Exception(f"Error retrieving catalog names. Please check your credentials and platform. Error: {str(e)}")
    
    @staticmethod
    def get_schema_names(engine) -> List[str]:
        """
        Retrieve schema names using PyExtrica.

        This method simplifies the process of retrieving schema names by wrapping the
        engine.dialect.get_schema_names method and handling the connection management automatically.

        :param engine: The SQLAlchemy engine instance.
        :return: A list of schema names.
        :raises Exception: If there is an error retrieving schema names.
        """
        try:
            # Establish a connection using the provided engine
            with engine.connect() as connection:
                # Retrieve schema names using the engine dialect method
                schemas = engine.dialect.get_schema_names(connection)
            return schemas
        except Exception as e:
            # Raise an exception if there is an error
            raise Exception(f"Error retrieving schema names. Please check your credentials and platform. Error: {str(e)}")
    
    @staticmethod
    def get_table_names(engine, schema: str = None) -> List[str]:
        """
        Retrieve table names using PyExtrica.

        This method simplifies the process of retrieving table names by wrapping the
        engine.dialect.get_table_names method and handling the connection management automatically.

        :param engine: The SQLAlchemy engine instance.
        :param schema: Optional. The schema to filter table names.
        :return: A list of table names.
        :raises Exception: If there is an error retrieving table names.
        """
        try:
            # Establish a connection using the provided engine
            with engine.connect() as connection:
                # Retrieve table names using the engine dialect method
                tables = engine.dialect.get_table_names(connection, schema=schema)
            return tables
        except Exception as e:
            # Raise an exception if there is an error
            raise Exception(f"Error retrieving table names. Please check your credentials and platform. Error: {str(e)}")
    
    @staticmethod
    def get_table_columns(engine, schema: str = None, table_name: str = None, **kwargs):
        """
        Retrieve columns information for a specified table using PyExtrica.
        
        This method wraps the engine.dialect.get_columns method, providing a more intuitive and
        simplified interface for retrieving columns information.
        
        :param engine: The engine instance.
        :param schema: The schema to which the specified table belongs.
        :param table_name: The name of the table for which columns information is to be retrieved.
        :param \*\*kwargs: Additional keyword arguments.
        :return: A list containing information about columns, including their names and data types.
        """
        try:
            with engine.connect() as connection:
                catalogs = engine.dialect.get_columns(connection, schema=schema, table_name=table_name, **kwargs)
                return catalogs
        except Exception as e:
            # Raise an exception if there is an error
            raise Exception(f"Error retrieving catalog names. Please check your credentials and platform. Error: {str(e)}")
        
    @staticmethod
    def execute_sql_query(engine, sql_query):
        """
        Execute a SQL query on Extrica.

        This method allows executing a SQL query using the provided SQLAlchemy engine instance,
        which is specifically configured for use with Extrica, a distributed SQL query engine.
        It handles the connection management automatically.

        :param engine: The SQLAlchemy engine instance optimized for Extrica.
        :param sql_query: The SQL query to execute.
        :return: The result of executing the SQL query.
        :raises ValueError: If the SQL query is empty.
        :raises Exception: If there is an error executing the SQL query.
        """
        # Check if the SQL query is empty
        if not sql_query.strip():
            raise ValueError("SQL query cannot be empty.")

        try:
            # Convert SQL query string to sqlalchemy.text object
            text_query = PyExtricaFunctions.extrica_text(sql_query)
            if sql_query.strip().lower().startswith('select'):
                # Execute the SELECT SQL query using the provided engine instance
                with engine.connect() as connection:
                    result = connection.execute(text_query)
                return result
            else:
                # For other DML operations, execute the query and return a success message
                with engine.connect() as connection:
                    connection.execute(text_query)
                
                # Check if the query is a DML operation (INSERT, UPDATE, DELETE)
                dml_operations = ['insert', 'update', 'delete', 'alter', 'drop', 'truncate','create']
                for operation in dml_operations:
                    if operation in sql_query.strip().lower():
                        return f"Successfully executed {operation.upper()} operation."
                
                # If it's not a recognized DML operation, return a generic success message
                return "Query executed successfully."
        except Exception as e:
            raise Exception(f"Error executing SQL query. Please check your credentials and SQL query. Error: {str(e)}")

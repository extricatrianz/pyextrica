import json
from textwrap import dedent
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import unquote_plus

from sqlalchemy import exc, sql
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import sqltypes

from pyextrica import dbapi as trino_dbapi
from pyextrica import logging
from pyextrica.auth import (
    ExtricaAuthentication
)
from pyextrica.dbapi import Cursor
from pyextrica.sqlalchemy import compiler, datatype, error
from pyextrica.extrica_rest_handler import ExtricaHTTPHandler
from .datatype import JSONIndexType, JSONPathType

logger = logging.get_logger(__name__)

colspecs = {
    sqltypes.JSON.JSONIndexType: JSONIndexType,
    sqltypes.JSON.JSONPathType: JSONPathType,
}


class TrinoDialect(DefaultDialect):
    def __init__(self,
                 json_serializer=None,
                 json_deserializer=None,
                 **kwargs):
        DefaultDialect.__init__(self, **kwargs)
        self._json_serializer = json_serializer
        self._json_deserializer = json_deserializer

    name = "pyextrica"
    driver = "rest"

    statement_compiler = compiler.TrinoSQLCompiler
    ddl_compiler = compiler.TrinoDDLCompiler
    type_compiler = compiler.TrinoTypeCompiler
    preparer = compiler.TrinoIdentifierPreparer

    # Data Type
    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True

    # Column options
    supports_sequences = False
    supports_comments = True
    inline_comments = True
    supports_default_values = False

    # DDL
    supports_alter = True

    # DML
    # Queries of the form `INSERT () VALUES ()` is not supported by Trino.
    supports_empty_insert = False
    supports_multivalues_insert = True
    postfetch_lastrowid = False

    # Caching
    # Warnings are generated by SQLAlchmey if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = False

    # Support proper ordering of CTEs in regard to an INSERT statement
    cte_follows_insert = True
    colspecs = colspecs

    @classmethod
    def dbapi(cls):
        """
        ref: https://www.python.org/dev/peps/pep-0249/#module-interface
        """
        return trino_dbapi

    @classmethod
    def import_dbapi(cls):
        """
        ref: https://www.python.org/dev/peps/pep-0249/#module-interface
        """
        return trino_dbapi

    def create_connect_args(self, url: URL) -> Tuple[Sequence[Any], Mapping[str, Any]]:
        args: Sequence[Any] = list()
        kwargs: Dict[str, Any] = dict(host=url.host)
        if url.port:
            kwargs["port"] = url.port

        db_parts = (url.database or "system").split("/")
        if len(db_parts) == 1:
            kwargs["catalog"] = unquote_plus(db_parts[0])
        elif len(db_parts) == 2:
            kwargs["catalog"] = unquote_plus(db_parts[0])
            kwargs["schema"] = unquote_plus(db_parts[1])
        else:
            raise ValueError(f"Unexpected database format {url.database}")

        if url.username:
            kwargs["user"] = unquote_plus(url.username)

        if url.password:
            if not url.username:
                raise ValueError("Username is required when specify password in connection URL")
            kwargs["http_scheme"] = "https"
            token = ExtricaHTTPHandler._generate_token(url.username, url.password,url.host)
            kwargs["auth"] = ExtricaAuthentication(token)
        platform = url.query.get('platform')
        if platform:
            kwargs["platform"] = platform
        schema = url.query.get('schema')
        if schema:
            kwargs["schema"] = schema
        if "access_token" in url.query:
            kwargs["http_scheme"] = "https"
            kwargs["auth"] = ExtricaAuthentication(unquote_plus(url.query["access_token"]))
        return args, kwargs

    def get_columns(self, connection: Connection, table_name: str, schema: str = None, **kw) -> List[Dict[str, Any]]:
        """
        Retrieve columns information for a specified table using REST API calls.

        This method first determines the platform (Data Products or Data Sources) associated with the provided connection.
        It then retrieves authentication details, such as token and host, from the connection.
        Based on the platform, it calls the appropriate method to fetch columns information using REST API calls.

        Parameters:
            connection (Connection): The connection object containing necessary information for authentication and endpoint.
            table_name (str): The name of the table for which columns information is to be retrieved.
            schema (str, optional): The schema to which the specified table belongs.
            **kw: Additional keyword arguments.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing information about columns, including their names and data types.
        """
        
        platform=self._get_default_platform(connection)
        if platform is None:
            raise ValueError("Please provide a platform.")
        auth = self._get_default_auth(connection)
        token = auth.token
        host=self._get_default_host(connection)
        user=self._get_default_user(connection)
        table=table_name   
        if platform == 'data_products':
            columns = ExtricaHTTPHandler._get_columns_dp(user,token, host,table)
            return columns
        else:
            if schema is None:
                schema = self._get_default_schema_name(connection)
            if schema is None:
                raise ValueError("Please provide a schema.")
            catalog=self._get_default_catalog_name(connection)
            columns = ExtricaHTTPHandler._get_columns_ds(user,token, host,catalog,schema,table)
            return columns
            

    def get_catalog_names(self, connection: Connection, **kw) -> List[str]:
        """
        Retrieve catalog names from the specified platform using REST API calls.

        This method first determines the platform (Data Products or Data Sources) associated with the provided connection.
        It then retrieves authentication details, such as token and host, from the connection.
        Based on the platform, it calls the appropriate method to fetch catalog names using REST API calls.

        Parameters:
            connection (Connection): The connection object containing necessary information for authentication and endpoint.
            **kw: Additional keyword arguments.

        Returns:
            List[str]: A list of catalog names.
        """
        
        platform=self._get_default_platform(connection)
        if not platform:
            raise ValueError("Please provide a platform.")
        auth = self._get_default_auth(connection)
        token = auth.token
        host=self._get_default_host(connection)
        user=self._get_default_user(connection)
        if platform == 'data_products':
            catalogs = ExtricaHTTPHandler._get_catalogs_dp(host, user, token)
            return catalogs
        else:
            catalogs = ExtricaHTTPHandler._get_catalogs_ds(host, user, token)
            return catalogs

    def get_schema_names(self, connection: Connection, **kw) -> List[str]:
        
        """
        Retrieve schema names from the specified platform using REST API calls.

        This method first determines the platform (Data Products or Data Sources) associated with the provided connection.
        It also retrieves the default catalog name from the connection.
        It then validates the platform and catalog names.
        If valid, it retrieves authentication details such as token and host from the connection.
        Based on the platform, it calls the appropriate method to fetch schema names using REST API calls.

        Parameters:
            connection (Connection): The connection object containing necessary information for authentication, endpoint, and catalog.
            **kw: Additional keyword arguments.

        Returns:
            List[str]: A list of schema names.
        """
        
        platform=self._get_default_platform(connection)
        catalog=self._get_default_catalog_name(connection)
        if not platform or not catalog:
            if not platform:
                raise ValueError("Please provide a platform.")
            if not catalog:
                raise ValueError("Please provide a catalog name.")
        auth = self._get_default_auth(connection)
        token = auth.token
        host=self._get_default_host(connection)
        user=self._get_default_user(connection)
        if platform == 'data_products':
            schemas = ExtricaHTTPHandler._get_schemas_dp(user, catalog, token, host)
            return schemas
        else:
            schemas = ExtricaHTTPHandler._get_schemas_ds(user, catalog, token, host)
            return schemas


    def get_table_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        """
        Retrieve table names from the specified platform and schema using REST API calls.

        This method first determines the platform (Data Products or Data Sources) associated with the provided connection.
        If schema is not provided, it retrieves the default schema name from the connection.
        It then validates the platform, schema, and catalog names.
        If valid, it retrieves authentication details such as token and host from the connection.
        Based on the platform, it calls the appropriate method to fetch table names using REST API calls.

        Parameters:
            connection (Connection): The connection object containing necessary information for authentication, endpoint, and schema.
            schema (str, optional): The schema for which table names are to be retrieved. If not provided, the default schema is used.
            **kw: Additional keyword arguments.

        Returns:
            List[str]: A list of table names.
        """
        
        platform=self._get_default_platform(connection)
        if schema is None:
            schema = self._get_default_schema_name(connection)
        if schema is None:
            raise ValueError("Please provide a schema.")
        if platform is None:
            raise ValueError("Please provide a platform.")
        auth = self._get_default_auth(connection)
        token = auth.token
        host=self._get_default_host(connection)
        catalog=self._get_default_catalog_name(connection)
        user=self._get_default_user(connection)
        schema=schema
        if platform == 'data_products':
            tables = ExtricaHTTPHandler._get_tables_dp(user, catalog, token, host, schema)
            return tables
        else:
            tables = ExtricaHTTPHandler._get_tables_ds(user, catalog, token, host, schema)
            return tables

    def _raw_connection(self, connection: Union[Engine, Connection]) -> trino_dbapi.Connection:
        if isinstance(connection, Engine):
            return connection.raw_connection()
        return connection.connection

    def _get_default_catalog_name(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.catalog

    def _get_default_schema_name(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.schema
    
    def _get_default_auth(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.auth
    
    def _get_default_host(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.host
    
    def _get_default_platform(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.platform
    
    def _get_default_user(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.user
    
    def _get_default_table_name(self, connection: Connection) -> Optional[str]:
        dbapi_connection: trino_dbapi.Connection = self._raw_connection(connection)
        return dbapi_connection.table_name
    
    def do_execute(
        self, cursor: Cursor, statement: str, parameters: Tuple[Any, ...], context: DefaultExecutionContext = None
    ):
        cursor.execute(statement, parameters)

    def do_rollback(self, dbapi_connection: trino_dbapi.Connection):
        if dbapi_connection.transaction is not None:
            dbapi_connection.rollback()

    def set_isolation_level(self, dbapi_conn: trino_dbapi.Connection, level: str) -> None:
        dbapi_conn._isolation_level = trino_dbapi.IsolationLevel[level]

    def get_isolation_level(self, dbapi_conn: trino_dbapi.Connection) -> str:
        return dbapi_conn.isolation_level.name

    def get_default_isolation_level(self, dbapi_conn: trino_dbapi.Connection) -> str:
        return trino_dbapi.IsolationLevel.AUTOCOMMIT.name

    def _get_full_table(self, table_name: str, schema: str = None, quote: bool = True) -> str:
        table_part = self.identifier_preparer.quote_identifier(table_name) if quote else table_name
        if schema:
            schema_part = self.identifier_preparer.quote_identifier(schema) if quote else schema
            return f"{schema_part}.{table_part}"

        return table_part

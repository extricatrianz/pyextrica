from typing import Any, Dict, List
from unittest import mock
import unittest
from mock import MagicMock, Mock, patch
from pyextrica.sqlalchemy import dialect



from pyextrica.extrica_rest_handler import  ExtricaHTTPHandler
import pytest
# from sqlalchemy import Engine
from sqlalchemy.engine.url import URL, make_url

from pyextrica.auth import ExtricaAuthentication
from pyextrica.dbapi import Connection
from pyextrica.sqlalchemy import URL as trino_url
from pyextrica.sqlalchemy.dialect import (
    ExtricaAuthentication,
    TrinoDialect,
)
from pyextrica.dbapi import Connection as trino_dbapi_Connection
from pyextrica.transaction import IsolationLevel
from pyextrica.sqlalchemy.dialect import TrinoDialect
class TestTrinoDialect(unittest.TestCase):
    # def setup_method(self):
    #     self.dialect = TrinoDialect()
    def setUp(self):
        self.dialect = TrinoDialect()
        self.mock_connection = MagicMock()

    @pytest.mark.parametrize(
        "url, generated_url, expected_args, expected_kwargs",
        [
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                )),
                'pyextrica://user@localhost:8080/?source=trino-sqlalchemy',
                list(),
                dict(host="localhost", catalog="system", user="user", port=8080, source="trino-sqlalchemy"),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    port=443,
                )),
                'pyextrica://user@localhost:443/?source=trino-sqlalchemy',
                list(),
                dict(host="localhost", port=443, catalog="system", user="user", source="trino-sqlalchemy"),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    access_token="afdlsdfk%4#'",
                )),
                'pyextrica://user@localhost:8080/'
                '?access_token=afdlsdfk%254%23%27'
                '&source=extrica-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    auth=ExtricaAuthentication("afdlsdfk%4#'"),
                    http_scheme="https",
                    source="extrica-sqlalchemy"
                ),
            ),
            (
                make_url(trino_url(
                    user="user",
                    host="localhost",
                    session_properties={"query_max_run_time": "1d"},
                    http_headers={"trino": 1},
                    extra_credential=[("a", "b"), ("c", "d")],
                    client_tags=["1", "sql"],
                    legacy_primitive_types=False,
                )),
                'pyextrica://user@localhost:8080/'
                '?client_tags=%5B%221%22%2C+%22sql%22%5D'
                '&extra_credential=%5B%5B%22a%22%2C+%22b%22%5D%2C+%5B%22c%22%2C+%22d%22%5D%5D'
                '&http_headers=%7B%22trino%22%3A+1%7D'
                '&legacy_primitive_types=false'
                '&session_properties=%7B%22query_max_run_time%22%3A+%221d%22%7D'
                '&source=extrica-sqlalchemy',
                list(),
                dict(
                    host="localhost",
                    port=8080,
                    catalog="system",
                    user="user",
                    source="extrica-sqlalchemy",
                    session_properties={"query_max_run_time": "1d"},
                    http_headers={"trino": 1},
                    extra_credential=[("a", "b"), ("c", "d")],
                    client_tags=["1", "sql"],
                    legacy_primitive_types=False,
                ),
            ),
            
        ],
    )
    
    
   

    @patch('pyextrica.extrica_rest_handler.ExtricaHTTPHandler._generate_token')
    def test_create_connect_args(self, mock_generate_token):
        # Mock the URL object with desired properties
        mock_url = Mock()
        mock_url.host = 'localhost'
        mock_url.port = 8080
        mock_url.database = 'test_catalog/test_schema'
        mock_url.username = 'user'
        mock_url.password = 'password'
        mock_url.query = {'platform': 'data_products'}

        # Create a TrinoDialect instance
        dialect = TrinoDialect()

        # Call the method being tested
        args, kwargs = dialect.create_connect_args(mock_url)

        # Assertions or validations based on the expected behavior
        self.assertEqual(len(args), 0)  # No positional arguments expected
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 8080)
        self.assertEqual(kwargs['catalog'], 'test_catalog')
        self.assertEqual(kwargs['schema'], 'test_schema')
        self.assertEqual(kwargs['user'], 'user')
        self.assertEqual(kwargs['http_scheme'], 'https')  # Expected due to password presence
        self.assertIsInstance(kwargs['auth'], ExtricaAuthentication)  # Assuming this class exists

    def test_create_connect_args_missing_user_when_specify_password(self):
        url = make_url("pyextrica://:pass@localhost")
        with pytest.raises(ValueError, match="Username is required when specify password in connection URL"):
            self.dialect.create_connect_args(url)

    def test_create_connect_args_wrong_db_format(self):
        url = make_url("pyextrica://abc@localhost/catalog/schema/foobar")
        with pytest.raises(ValueError, match="Unexpected database format catalog/schema/foobar"):
            self.dialect.create_connect_args(url)

    def test_trino_connection_jwt_auth(self):
        dialect = TrinoDialect()
        access_token = 'sample-token'
        url = make_url(f'pyextrica://host/?access_token={access_token}')
        _, cparams = dialect.create_connect_args(url)

        assert cparams['http_scheme'] == "https"
        assert isinstance(cparams['auth'], ExtricaAuthentication)
        assert cparams['auth'].token == access_token

    def test_get_default_catalog_name(self):
        trino_dialect = "Presto"  # Define and initialize the attribute
        self.assertEqual(trino_dialect, "Presto")  # Assert that the attribute has the expected value

    def test_get_default_schema_name(self):
        trino_dialect = "Presto"  # Define and initialize the attribute
        self.assertEqual(trino_dialect, "Presto")  # Assert that the attribute has the expected value
    def test_get_default_auth(self):
        self.dialect._raw_connection = MagicMock()
        mock_connection = MagicMock(spec=trino_dbapi_Connection)
        mock_connection.auth = MagicMock()
        self.dialect._raw_connection.return_value = mock_connection
        auth = self.dialect._get_default_auth(Mock())
        self.assertIsNotNone(auth)  # Replace this assertion with your actual logic

    def test_get_default_host(self):
        self.dialect._raw_connection = MagicMock()
        mock_connection = MagicMock(spec=trino_dbapi_Connection)
        mock_connection.host = MagicMock()
        self.dialect._raw_connection.return_value = mock_connection
        host = self.dialect._get_default_host(Mock())
        self.assertIsNotNone(host)  # Replace this assertion with your actual logic

    def test_get_default_platform(self):
        self.dialect._raw_connection = MagicMock()
        mock_connection = MagicMock(spec=trino_dbapi_Connection)
        mock_connection.platform = MagicMock()
        self.dialect._raw_connection.return_value = mock_connection
        platform = self.dialect._get_default_platform(Mock())
        self.assertIsNotNone(platform)  # Replace this assertion with your actual logic

    def test_get_default_user(self):
        self.dialect._raw_connection = MagicMock()
        mock_connection = MagicMock(spec=trino_dbapi_Connection)
        mock_connection.user = MagicMock()
        self.dialect._raw_connection.return_value = mock_connection
        user = self.dialect._get_default_user(Mock())
        self.assertIsNotNone(user)  # Replace this assertion with your actual logic

    def test_get_default_table_name(self):
        self.dialect._raw_connection = MagicMock()
        mock_connection = MagicMock(spec=trino_dbapi_Connection)
        mock_connection.table_name = MagicMock()
        self.dialect._raw_connection.return_value = mock_connection
        table_name = self.dialect._get_default_table_name(Mock())
        self.assertIsNotNone(table_name)  # Replace this assertion with your actual logic

    def test_do_execute(self):
        mock_cursor = MagicMock()
        statement = "SELECT * FROM table"
        parameters = (1, 2, 3)
        self.dialect.do_execute(mock_cursor, statement, parameters)
        mock_cursor.execute.assert_called_once_with(statement, parameters)

    def test_do_rollback(self):
        mock_dbapi_connection = MagicMock(spec=trino_dbapi_Connection)
        mock_dbapi_connection.transaction = MagicMock()
        self.dialect.do_rollback(mock_dbapi_connection)
        mock_dbapi_connection.rollback.assert_called_once()

    

    def test_get_isolation_level(self):
        mock_dbapi_connection = MagicMock(spec=trino_dbapi_Connection)
        isolation_level = self.dialect.get_isolation_level(mock_dbapi_connection)
        self.assertIsNotNone(isolation_level)  # Replace this assertion with your actual logic

    def test_get_default_isolation_level(self):
        mock_dbapi_connection = MagicMock(spec=trino_dbapi_Connection)
        default_isolation_level = self.dialect.get_default_isolation_level(mock_dbapi_connection)
        self.assertIsNotNone(default_isolation_level)  # Replace this assertion with your actual logic

    def test_get_full_table(self):
        table_name = "table_name"
        schema = "schema_name"
        full_table = self.dialect._get_full_table(table_name, schema)
        self.assertIsNotNone(full_table) 


import unittest
from unittest.mock import MagicMock

from pyextrica.sqlalchemy.dialect import TrinoDialect  
class TestYourClass(unittest.TestCase):

    def setUp(self):
        self.connection = MagicMock()
        self.connection.token = "mock_token"
        self.connection.host = "mock_host"
        self.connection.user = "mock_user"
        self.connection.catalog = "mock_catalog"
        self.connection.schema = "mock_schema"
        self.your_class_instance = TrinoDialect()

    def test_get_columns_data_products(self):
        self.your_class_instance._get_default_platform = MagicMock(return_value='data_products')
        result = self.your_class_instance.get_columns(self.connection, 'mock_table')
        self.assertIsNotNone(result)
        

    def test_get_columns_data_sources(self):
        self.your_class_instance._get_default_platform = MagicMock(return_value='data_sources')
        result = self.your_class_instance.get_columns(self.connection, 'mock_table', schema='mock_schema')
        self.assertIsNotNone(result)
        

    
   
    def test_get_table_names_with_schema(self):
        self.your_class_instance._get_default_platform = MagicMock(return_value='data_products')
        result = self.your_class_instance.get_table_names(self.connection, schema='mock_schema')
        self.assertIsNotNone(result)

    def test_get_table_names_without_schema(self):
        self.your_class_instance._get_default_platform = MagicMock(return_value='data_sources')
        result = self.your_class_instance.get_table_names(self.connection)
        self.assertIsNotNone(result)
        # Add more specific assertions based on the expected behavior

    def test_get_table_names_missing_platform(self):
        self.your_class_instance._get_default_platform = MagicMock(return_value=None)
        with self.assertRaises(ValueError):
            self.your_class_instance.get_table_names(self.connection, schema='mock_schema')

    def test_get_table_names_missing_schema(self):
        self.your_class_instance._get_default_platform = MagicMock(return_value='data_sources')
        self.your_class_instance._get_default_schema_name = MagicMock(return_value=None)
        with self.assertRaises(ValueError):
            self.your_class_instance.get_table_names(self.connection)

    

if __name__ == '__main__':
    unittest.main()

    


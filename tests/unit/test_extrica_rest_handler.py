import unittest
from unittest.mock import patch, MagicMock
from pyextrica.extrica_rest_handler import ExtricaHTTPHandler  # Import your class here

import unittest
from unittest.mock import patch, Mock

import requests


class TestExtricaHTTPHandler(unittest.TestCase):

    def setUp(self):
        self.base_url = 'https://example.com'
        self.access_token = 'sample_token'
        self.handler = ExtricaHTTPHandler(self.base_url, self.access_token)




   

    @patch('requests.get')
    def test_get(self, mock_get):
        endpoint = '/test-endpoint'
        params = {'param1': 'value1', 'param2': 'value2'}
        expected_url = f"{self.base_url}{endpoint}?param1=value1&param2=value2"
        expected_headers = {'Authorization': 'Bearer ' + self.access_token}
        expected_response = MagicMock()

        mock_get.return_value = expected_response

        response = self.handler._get(endpoint, params)

        mock_get.assert_called_once_with(url=expected_url, headers=expected_headers)
        self.assertEqual(response, expected_response)

    @patch('requests.post')
    def test_get_modified_query(self, mock_post):
        email = 'test@example.com'
        sql = 'SELECT * FROM table'
        access_token = 'sample_token'
        expected_endpoint = f"/query-engine/dqe/getModifiedQuery/{email}"
        expected_url = f"{self.base_url}{expected_endpoint}"
        expected_payload = {"inputQuerySql": sql, "isPaginatedResultset": False}
        expected_headers = {'Authorization': 'Bearer ' + access_token, 'Content-Type': 'application/json'}
        expected_response = MagicMock()

        mock_post.return_value = expected_response

        response = self.handler._get_modified_query(email, sql, access_token)

        mock_post.assert_called_once_with(url=expected_url, headers=expected_headers, json=expected_payload)
        self.assertEqual(response, expected_response)

    @patch('requests.post')
    def test_generate_token(self, mock_post):
        username = 'test_user'
        password = 'test_password'
        host = 'example.com'
        expected_payload = {"email": username, "password": password, "host": host}
        expected_url = f"https://{host}/iam/security/signin"
        expected_headers = {'Content-Type': 'application/json'}
        expected_response = MagicMock()
        expected_response.status_code = 200
        expected_response.json.return_value = {'accessToken': 'sample_token'}

        mock_post.return_value = expected_response

        token = ExtricaHTTPHandler._generate_token(username, password, host)

        mock_post.assert_called_once_with(url=expected_url, headers=expected_headers, json=expected_payload)
        self.assertEqual(token, 'sample_token')
    
    @patch('requests.get')
    def test_get_catalogs_dp(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'domain': 'domain1'}, {'domain': 'domain2'}]
        mock_get.return_value = mock_response

        result = ExtricaHTTPHandler._get_catalogs_dp('example.com', 'test@example.com', 'token')

        self.assertEqual(result, ['domain1', 'domain2'])

    @patch('requests.get')
    def test_get_schemas_dp(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'domain': 'domain1'}, {'domain': 'domain2'}]
        mock_get.return_value = mock_response

        result = ExtricaHTTPHandler._get_schemas_dp('test@example.com', 'example_domain', 'token', 'example.com')

        self.assertEqual(result, ['domain1', 'domain2'])

    @patch('requests.get')
    def test_get_tables_dp(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'key1': ['table1', 'table2'], 'key2': ['table3']}
        mock_get.return_value = mock_response

        result = ExtricaHTTPHandler._get_tables_dp('test@example.com', 'example_domain', 'token', 'example.com', 'subdomain')

        self.assertEqual(result, ['table1', 'table2', 'table3'])
   

   

    @patch('pyextrica.extrica_rest_handler.requests.get')
    def test_get_columns_dp(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'dataproduct': {'columns': [{'name': 'Column1', 'type': 'String'}, {'name': 'Column2', 'type': 'Integer'}]}}
        mock_get.return_value = mock_response

        columns = ExtricaHTTPHandler._get_columns_dp('user@example.com', 'token123', 'example.com', 'DataProduct1')
       # self.assertEqual(columns,  [{'name': 'Column1', 'type': NullType(), 'nullable': 'YES'}, {'[51 chars]ES'}])

    @patch('pyextrica.extrica_rest_handler.requests.get')
    def test_get_catalogs_ds(self, mock_get):
        # Mocking the response from the API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'name': 'Catalog1'}, {'name': 'Catalog2'}]
        mock_get.return_value = mock_response

        # Calling the function with mock parameters
        catalogs = ExtricaHTTPHandler._get_catalogs_ds('example.com', 'user@example.com', 'token123')

        # Asserting the returned value
        self.assertEqual(catalogs, ['Catalog1', 'Catalog2'])

    @patch('pyextrica.extrica_rest_handler.requests.get')
    def test_get_schemas_ds(self, mock_get):
        # Mocking the response from the API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'schemaName': 'Schema1'}, {'schemaName': 'Schema2'}]
        mock_get.return_value = mock_response

        # Calling the function with mock parameters
        schemas = ExtricaHTTPHandler._get_schemas_ds('user@example.com', 'Catalog1', 'token123', 'example.com')

        # Asserting the returned value
        self.assertEqual(schemas, ['Schema1', 'Schema2'])

    @patch('pyextrica.extrica_rest_handler.requests.get')
    def test_get_tables_ds(self, mock_get):
        # Mocking the response from the API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'tableName': 'Table1'}, {'tableName': 'Table2'}]
        mock_get.return_value = mock_response

        # Calling the function with mock parameters
        tables = ExtricaHTTPHandler._get_tables_ds('user@example.com', 'Catalog1', 'token123', 'example.com', 'Schema1')

        # Asserting the returned value
        self.assertEqual(tables, ['Table1', 'Table2'])

    @patch('pyextrica.extrica_rest_handler.requests.get')
    def test_get_columns_ds(self, mock_get):
        # Mocking the response from the API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'name': 'Column1', 'dataType': 'Type1'}, {'name': 'Column2', 'dataType': 'Type2'}]
        mock_get.return_value = mock_response

        # Calling the function with mock parameters
        columns = ExtricaHTTPHandler._get_columns_ds('user@example.com', 'token123', 'example.com', 'Catalog1', 'Schema1', 'Table1')

        # Asserting the returned value
        #self.assertEqual(columns, [{'name': 'Column1', 'dataType': 'Type1'}, {'name': 'Column2', 'dataType': 'Type2'}])

    





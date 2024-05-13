import json
from urllib.parse import quote
import requests

__all__ = ["ExtricaHTTPHandler"]

class ExtricaHTTPHandler:

    def __init__(self, base_url, access_token):
        self._base_url = base_url
        self._access_token = access_token

    def _get(self,  endpoint, params=None):
        """
        Perform a GET request to the specified endpoint with optional parameters.

        This method constructs a URL with the given endpoint and parameters, then sends a GET request
        to that URL with the provided access token in the headers.

        :param endpoint: The endpoint to send the GET request to.
        :param params: Optional. A dictionary containing query parameters.
        :return: The response object returned by the GET request.
        """
        encoded_params = "&".join([f"{key}={quote(str(value))}" for key, value in params.items()])
        url_with_params = f"{self._base_url}{endpoint}?{encoded_params}"
        headers = {'Authorization': 'Bearer '+self._access_token}
        response = requests.get(url=url_with_params,headers=headers)
        return response
    
    def _get_modified_query(self, email, sql,access_token):
        """
        Get the modified SQL query for a given email and input SQL.

        This method sends a POST request to the specified endpoint with the provided email,
        input SQL, and access token in the request body. It returns the response object.

        :param email: The email address associated with the modified query.
        :param sql: The input SQL query to be modified.
        :param access_token: The access token for authorization.
        :return: The response object returned by the POST request.
        """
        payload = {"inputQuerySql": sql, "isPaginatedResultset": False}
        endpoint = f"/query-engine/dqe/getModifiedQuery/{email}"
        url_with_params = f"{self._base_url}{endpoint}"
        headers = {'Authorization': 'Bearer '+access_token, 'Content-Type': 'application/json'}
        response = requests.post(url=url_with_params,headers=headers, json=payload)
        return response
    
    def _generate_token(username, password,host) -> str:
        """
        Function to generate authentication token by calling an Extrica API.
        
        This function sends a request to the Extrica API with provided username and password to
        obtain an authentication token.
        
        Parameters:
            username (str): The username for authentication.
            password (str): The password for authentication.
            host (str): The host URL of the Extrica API.
        
        Returns:
            str: The authentication token obtained from the Extrica API.
        """
        payload = {"email": username, "password": password,"host":host}
        url = f"https://{host}/iam/security/signin"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url=url, headers=headers, json=payload)
        if response.status_code == 200:
            token = response.json().get("accessToken")
            return token
        else:
            return None

    def _get_catalogs_dp(host, userEmail, token):
        """
        Function to retrieve data product catalogs.
        
        This function makes a request to the specified host with provided user email and token
        to fetch data product catalogs.
        
        Parameters:
            host (str): The host URL for accessing the data product catalogs.
            userEmail (str): The email address of the user.
            token (str): The authentication token for accessing the data product catalogs.
        
        Returns:
            list: A list of domain names extracted from the extrica platform.
        """

        url = f"https://{host}/core/datasets/{userEmail}"
        headers = {"Authorization": f"Bearer {token}"}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            domains = [item['domain'] for item in data]
            return domains
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None 
    
    def _get_schemas_dp(userEmail, domain, token, host):
        """
        Function to retrieve schemas of a specified domain from the Extrica Data Product platform.
        
        This function makes a request to the specified host with provided user email, domain, and token
        to fetch schemas of the specified domain from the Extrica Data Product platform.
        
        Parameters:
            userEmail (str): The email address of the user.
            domain (str): The domain for which schemas are to be retrieved.
            token (str): The authentication token for accessing the Extrica platform.
            host (str): The host URL for accessing the Extrica Data Product platform.
        
        Returns:
            list: A list of schemas belonging to the specified domain.
        """
        
        url = f"https://{host}/core/datasets/{userEmail}/domains/{domain}"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            schemas = [item['domain'] for item in data]
            return schemas
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching schemas: {e}")
            return []
        
    def _get_tables_dp(userEmail, domainName, token, host, subDomainName):
        """
        Function to retrieve data product names from the Extrica Data Product platform.
        
        This function makes a request to the specified host with provided user email, domain name, token, and subdomain name
        to fetch data product names from the Extrica Data Product platform.
        
        Parameters:
            userEmail (str): The email address of the user.
            domainName (str): The name of the domain for which data product names are to be retrieved.
            token (str): The authentication token for accessing the Extrica platform.
            host (str): The host URL for accessing the Extrica Data Product platform.
            subDomainName (str): The name of the subdomain within the specified domain.
        
        Returns:
            list: A list of data product names belonging to the specified domain and subdomain.
        """
        
        url = f"https://{host}/core/datasets/{userEmail}/domains/{domainName}/subDomains/{subDomainName}"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            tables = []
            for key in data:
                tables.extend(data[key])
            return tables
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching tables: {e}")
            return []
        
    def _get_columns_dp(userEmail, token, host, dataproduct):
        """
        Function to retrieve columns of a data product from the Extrica Data Product platform and return as a dictionary.
        
        This function makes a request to the specified host with provided user email, token, and data product
        to fetch columns from the Extrica Data Product platform.
        
        Parameters:
            userEmail (str): The email address of the user.
            token (str): The authentication token for accessing the Extrica platform.
            host (str): The host URL for accessing the Extrica Data Product platform.
            dataproduct (str): The name of the data product for which columns are to be retrieved.
        
        Returns:
            list: A list of dictionary mapping column names to their types.
        """
        
        columns_list = []
        try:
            url = f"https://{host}/core/datasets/{dataproduct}/{userEmail}/columns"
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers)
            data = response.json()
            dataproduct = data.get('dataproduct', {})
            columns = dataproduct.get('columns', [])
            for column in columns:
                name = column.get('name')
                column_type = column.get('type')
                if name and column_type:
                    columns_list.append({'name': name, 'dataType': column_type})
        except Exception as e:
            print(f"Error occurred while extracting columns: {e}")
        return columns_list
    
    def _get_catalogs_ds(host, userEmail, token):
        """
        Function to retrieve catalogs of data sources from the Extrica platform.
        
        This function makes a request to the specified host with provided user email and token
        to fetch catalogs of data sources.
        
        Parameters:
            host (str): The host URL for accessing the Extrica platform's data sources catalogs.
            userEmail (str): The email address of the user.
            token (str): The authentication token for accessing the Extrica platform.
        
        Returns:
            list: A list of catalogs of data sources.
        """

        url = f"https://{host}/core/datasource/jdbcDatasources/list/{userEmail}"
        headers = {"Authorization": f"Bearer {token}"}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            catalogs = [item['name'] for item in data]
            return catalogs
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return None 
    
    def _get_schemas_ds(userEmail, datasource, token, host):
        """
        Function to retrieve schemas of a specified datasource from the Extrica Data Source platform.
        
        This function makes a request to the specified host with provided user email, datasource (catalog), and token
        to fetch schemas of the specified datasource from the Extrica Data Source platform.
        
        Parameters:
            userEmail (str): The email address of the user.
            datasource (str): The datasource (catalog) for which schemas are to be retrieved.
            token (str): The authentication token for accessing the Extrica platform.
            host (str): The host URL for accessing the Extrica Data Source platform.
        
        Returns:
            list: A list of schemas belonging to the specified datasource (catalog).
        """
        
        url = f"https://{host}/core/datasource/{datasource}/jdbcSchemas"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            schemas = [schema['schemaName'] for schema in data]
            return schemas
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching schemas: {e}")
            return []
        
    def _get_tables_ds(userEmail, catalog, token, host, schema):
        """
        Function to retrieve tables from the Extrica Data Source platform.
        
        This function makes a request to the specified host with provided user email, catalog, token, and schema
        to fetch tables from the Extrica Data Source platform.
        
        Parameters:
            userEmail (str): The email address of the user.
            catalog (str): The catalog for which tables are to be retrieved.
            token (str): The authentication token for accessing the Extrica platform.
            host (str): The host URL for accessing the Extrica Data Source platform.
            schema (str): The schema within the specified catalog.
        
        Returns:
            list: A list of tables belonging to the specified catalog and schema.
        """
        
        url = f"https://{host}/core/datasource/jdbcTables/{catalog}/{schema}"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            table_names = [table['tableName'] for table in data]
            return table_names
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while fetching tables: {e}")
            return []
        
    def _get_columns_ds(userEmail, token, host,catalog,schema,table):
        """
        Function to retrieve columns from the Extrica Data Source platform.
        
        This function makes a request to the specified host with provided user email, token, catalog, schema, and table
        to fetch columns from the Extrica Data Source platform.
        
        Parameters:
            userEmail (str): The email address of the user.
            token (str): The authentication token for accessing the Extrica platform.
            host (str): The host URL for accessing the Extrica Data Source platform.
            catalog (str): The catalog containing the specified schema and table.
            schema (str): The schema containing the specified table.
            table (str): The table for which columns are to be retrieved.
        
        Returns:
            list: A list of dictionaries containing information about columns, including their names and data types.
        """
         
        url = f"https://{host}/core/datasource/jdbcTableColumns/{catalog}/{schema}/{table}"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            columns_info = []
            response = requests.get(url, headers=headers)
            data = response.json()
            for column_data in data:
                column_name = column_data.get('name')
                data_type = column_data.get('dataType')
                if column_name and data_type:
                    column_info = {'name': column_name, 'dataType': data_type}
                    columns_info.append(column_info)
            return columns_info
        except Exception as e:
            print(f"Error occurred while extracting columns: {e}")
            return []



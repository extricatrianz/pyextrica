


# import unittest
# from unittest.mock import MagicMock
# from mock import patch
# from pyextrica.pyextrica_functions import PyExtricaFunctions

# class TestPyExtricaFunctions(unittest.TestCase):

#     def setUp(self):
#         # Mock the SQLAlchemy engine for testing purposes
#         self.engine = MagicMock()
#   # Import the class containing the static method
#   # Import the module where PyExtricaFunctions is defined


  

#     def test_get_catalog_names(self):
#         # Mock the engine.dialect.get_catalog_names method
#         self.engine.dialect.get_catalog_names = MagicMock(return_value=['catalog1', 'catalog2'])
#         catalogs = PyExtricaFunctions.get_catalog_names(self.engine)
#         self.assertEqual(catalogs, ['catalog1', 'catalog2'])

#     def test_get_schema_names(self):
#         # Mock the engine.dialect.get_schema_names method
#         self.engine.dialect.get_schema_names = MagicMock(return_value=['schema1', 'schema2'])
#         schemas = PyExtricaFunctions.get_schema_names(self.engine)
#         self.assertEqual(schemas, ['schema1', 'schema2'])

#     def test_get_table_names(self):
#         # Mock the engine.dialect.get_table_names method
#         self.engine.dialect.get_table_names = MagicMock(return_value=['table1', 'table2'])
#         tables = PyExtricaFunctions.get_table_names(self.engine)
#         self.assertEqual(tables, ['table1', 'table2'])

#     def test_get_table_columns(self):
#         # Mock the engine.dialect.get_columns method
#         self.engine.dialect.get_columns = MagicMock(return_value=[('column1', 'type1'), ('column2', 'type2')])
#         columns = PyExtricaFunctions.get_table_columns(self.engine, schema='schema1', table_name='table1')
#         self.assertEqual(columns, [('column1', 'type1'), ('column2', 'type2')])

#     def test_execute_sql_query(self):
#         # Mock the connection.execute method
#         mock_result = MagicMock()
#         mock_result.fetchall = MagicMock(return_value=[(1, 'John'), (2, 'Doe')])
#         self.engine.connect.return_value.__enter__.return_value.execute.return_value = mock_result

#         result = PyExtricaFunctions.execute_sql_query(self.engine, 'SELECT * FROM users')
#         self.assertEqual(result.fetchall(), [(1, 'John'), (2, 'Doe')])

#     # Add more test cases as needed

# if __name__ == '__main__':
#     unittest.main()




import unittest
from unittest.mock import MagicMock, patch
from pyextrica.pyextrica_functions import PyExtricaFunctions

class TestPyExtricaFunctions(unittest.TestCase):

    def setUp(self):
        # Mock the SQLAlchemy engine for testing purposes
        self.engine = MagicMock()

    def test_get_catalog_names(self):
        # Mock the engine.dialect.get_catalog_names method
        self.engine.dialect.get_catalog_names = MagicMock(return_value=['catalog1', 'catalog2'])
        catalogs = PyExtricaFunctions.get_catalog_names(self.engine)
        self.assertEqual(catalogs, ['catalog1', 'catalog2'])

    def test_get_schema_names(self):
        # Mock the engine.dialect.get_schema_names method
        self.engine.dialect.get_schema_names = MagicMock(return_value=['schema1', 'schema2'])
        schemas = PyExtricaFunctions.get_schema_names(self.engine)
        self.assertEqual(schemas, ['schema1', 'schema2'])

    def test_get_table_names(self):
        # Mock the engine.dialect.get_table_names method
        self.engine.dialect.get_table_names = MagicMock(return_value=['table1', 'table2'])
        tables = PyExtricaFunctions.get_table_names(self.engine)
        self.assertEqual(tables, ['table1', 'table2'])

    def test_get_table_columns(self):
        # Mock the engine.dialect.get_columns method
        self.engine.dialect.get_columns = MagicMock(return_value=[('column1', 'type1'), ('column2', 'type2')])
        columns = PyExtricaFunctions.get_table_columns(self.engine, schema='schema1', table_name='table1')
        self.assertEqual(columns, [('column1', 'type1'), ('column2', 'type2')])

    def test_execute_sql_query(self):
        # Mock the connection.execute method
        mock_result = MagicMock()
        mock_result.fetchall = MagicMock(return_value=[(1, 'John'), (2, 'Doe')])
        self.engine.connect.return_value.__enter__.return_value.execute.return_value = mock_result

        result = PyExtricaFunctions.execute_sql_query(self.engine, 'SELECT * FROM users')
        self.assertEqual(result.fetchall(), [(1, 'John'), (2, 'Doe')])

    

    

if __name__ == '__main__':
    unittest.main()


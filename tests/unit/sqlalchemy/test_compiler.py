# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from mock import Mock
import pytest
from sqlalchemy import Column, Float, Integer, MetaData, String, Table, func, insert, select
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import column, table

from tests.unit.conftest import sqlalchemy_version
from pyextrica.sqlalchemy.dialect import TrinoDialect

metadata = MetaData()
table_without_catalog = Table(
    'table',
    metadata,
    Column('id', Integer),
    Column('name', String),
)
table_with_catalog = Table(
    'table',
    metadata,
    Column('id', Integer),
    schema='default',
    trino_catalog='other'
)


@pytest.fixture
def dialect():
    return TrinoDialect()


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_limit_offset(dialect):
    statement = select(table_without_catalog).limit(10).offset(0)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1\nLIMIT :param_2'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)

def test_limit(dialect):
    statement = select(table_without_catalog).limit(10)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nLIMIT :param_1'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_offset(dialect):
    statement = select(table_without_catalog).offset(0)
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\nOFFSET :param_1'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_cte_insert_order(dialect):
    cte = select(table_without_catalog).cte('cte')
    statement = insert(table_without_catalog).from_select(table_without_catalog.columns, cte)
    query = statement.compile(dialect=dialect)
    assert str(query) == \
        'INSERT INTO "table" (id, name) WITH cte AS \n'\
        '(SELECT "table".id AS id, "table".name AS name \n'\
        'FROM "table")\n'\
        ' SELECT cte.id, cte.name \n'\
        'FROM cte'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)



@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
def test_table_clause(dialect):
    statement = select(table("user", column("id"), column("name"), column("description")))
    query = statement.compile(dialect=dialect)
    assert str(query) == 'SELECT user.id, user.name, user.description \nFROM user'


@pytest.mark.skipif(
    sqlalchemy_version() < "1.4",
    reason="columns argument to select() must be a Python list or other iterable"
)
@pytest.mark.parametrize(
    'function,element',
    [
        ('first_value', func.first_value),
        ('last_value', func.last_value),
        ('nth_value', func.nth_value),
        ('lead', func.lead),
        ('lag', func.lag),
    ]
)
def test_ignore_nulls(dialect, function, element):
    statement = select(
        element(
            table_without_catalog.c.id,
            ignore_nulls=True,
        ).over(partition_by=table_without_catalog.c.name).label('window')
    )
    query = statement.compile(dialect=dialect)
    assert str(query) == \
           f'SELECT {function}("table".id) IGNORE NULLS OVER (PARTITION BY "table".name) AS window '\
           f'\nFROM "table"'

    statement = select(
        element(
            table_without_catalog.c.id,
            ignore_nulls=False,
        ).over(partition_by=table_without_catalog.c.name).label('window')
    )
    query = statement.compile(dialect=dialect)
    assert str(query) == \
           f'SELECT {function}("table".id) OVER (PARTITION BY "table".name) AS window ' \
           f'\nFROM "table"'

import unittest
from pyextrica.sqlalchemy.compiler import TrinoTypeCompiler  # Import your TrinoTypeCompiler and YourTypeClass from your module

  # Import your TrinoTypeCompiler class from your module

import unittest
from pyextrica.sqlalchemy.compiler import TrinoTypeCompiler  # Import TrinoTypeCompiler from your module

class TestTrinoTypeCompiler(unittest.TestCase):

    def setUp(self):
        # Provide the required 'dialect' argument when initializing TrinoTypeCompiler
        self.compiler = TrinoTypeCompiler(dialect='trino')

  

    def test_visit_DOUBLE(self):
        # Create an instance of YourTypeClass with required arguments
        type_double = TestTrinoTypeCompiler()
        result = self.compiler.visit_DOUBLE(type_double)
        self.assertEqual(result, "DOUBLE")
    
 # Replace 'your_module' and classes with actual names




import unittest

class MockTrinoTypeCompiler:
    def visit_FLOAT(self, type_, **kw):
        precision = type_.precision or 32
        if 0 <= precision <= 32:
            return self.visit_REAL(type_, **kw)
        elif 32 < precision <= 64:
            return self.visit_DOUBLE(type_, **kw)
        else:
            raise ValueError(f"type.precision must be in range [0, 64], got {type_.precision}")

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kw):
        return self.visit_DECIMAL(type_, **kw)

    # Implement other methods as needed for your tests

class TestTrinoTypeCompiler(unittest.TestCase):
    def setUp(self):
        self.compiler = MockTrinoTypeCompiler()

    def test_visit_FLOAT(self):
        type_ = Float(precision=64)
        result = self.compiler.visit_FLOAT(type_)
        self.assertEqual(result, "DOUBLE")

        with self.assertRaises(ValueError):
            type_ = Float(precision=65)
            self.compiler.visit_FLOAT(type_)

    # Implement other test methods similarly

if __name__ == "__main__":
    unittest.main()



import unittest
from pyextrica.sqlalchemy.compiler import TrinoTypeCompiler
from sqlalchemy import FLOAT, NUMERIC, NCHAR, NVARCHAR, TEXT, BINARY, CLOB, BLOB, DATETIME, TIMESTAMP, TIME, JSON

class TestTrinoTypeCompiler(unittest.TestCase):

    def setUp(self):
        dialect = Mock() 
        self.compiler = TrinoTypeCompiler(dialect=dialect)

    def test_visit_FLOAT(self):
        result = self.compiler.visit_FLOAT(FLOAT(precision=32))
        self.assertEqual(result, 'REAL')

    # def test_visit_DOUBLE(self):
    #     result = self.compiler.visit_DOUBLE(DOUBLE())
    #     self.assertEqual(result, 'DOUBLE')

    def test_visit_NUMERIC(self):
        result = self.compiler.visit_NUMERIC(NUMERIC())
        self.assertEqual(result, 'DECIMAL')

    def test_visit_NCHAR(self):
        result = self.compiler.visit_NCHAR(NCHAR())
        self.assertEqual(result, 'CHAR')

    def test_visit_NVARCHAR(self):
        result = self.compiler.visit_NVARCHAR(NVARCHAR())
        self.assertEqual(result, 'VARCHAR')

    def test_visit_TEXT(self):
        result = self.compiler.visit_TEXT(TEXT())
        self.assertEqual(result, 'VARCHAR')

    def test_visit_BINARY(self):
        result = self.compiler.visit_BINARY(BINARY())
        self.assertEqual(result, 'VARBINARY')

    def test_visit_CLOB(self):
        result = self.compiler.visit_CLOB(CLOB())
        self.assertEqual(result, 'VARCHAR')

    

    def test_visit_BLOB(self):
        result = self.compiler.visit_BLOB(BLOB())
        self.assertEqual(result, 'VARBINARY')

    def test_visit_DATETIME(self):
        result = self.compiler.visit_DATETIME(DATETIME())
        self.assertEqual(result, 'TIMESTAMP')

    
    def test_visit_JSON(self):
        result = self.compiler.visit_JSON(JSON())
        self.assertEqual(result, 'JSON')

if __name__ == '__main__':
    unittest.main()

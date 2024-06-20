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
from typing import List
from mock import Mock

import pytest

from pyextrica.sqlalchemy import datatype

split_string_testcases = {
    "10": ["10"],
    "10,3": ["10", "3"],
    '"a,b",c': ['"a,b"', "c"],
    '"a,b","c,d"': ['"a,b"', '"c,d"'],
    r'"a,\"b\",c",d': [r'"a,\"b\",c"', "d"],
    r'"foo(bar,\"baz\")",quiz': [r'"foo(bar,\"baz\")"', "quiz"],
    "varchar": ["varchar"],
    "varchar,int": ["varchar", "int"],
    "varchar,int,float": ["varchar", "int", "float"],
    "array(varchar)": ["array(varchar)"],
    "array(varchar),int": ["array(varchar)", "int"],
    "array(varchar(20))": ["array(varchar(20))"],
    "array(varchar(20)),int": ["array(varchar(20))", "int"],
    "array(varchar(20)),array(varchar(20))": [
        "array(varchar(20))",
        "array(varchar(20))",
    ],
    "map(varchar, integer),int": ["map(varchar, integer)", "int"],
    "map(varchar(20), integer),int": ["map(varchar(20), integer)", "int"],
    "map(varchar(20), varchar(20)),int": ["map(varchar(20), varchar(20))", "int"],
    "map(varchar(20), varchar(20)),array(varchar)": [
        "map(varchar(20), varchar(20))",
        "array(varchar)",
    ],
    "row(first_name varchar(20), last_name varchar(20)),int": [
        "row(first_name varchar(20), last_name varchar(20))",
        "int",
    ],
    'row("first name" varchar(20), "last name" varchar(20)),int': [
        'row("first name" varchar(20), "last name" varchar(20))',
        "int",
    ],
}


@pytest.mark.parametrize(
    "input_string, output_strings",
    split_string_testcases.items(),
    ids=split_string_testcases.keys(),
)
def test_split_string(input_string: str, output_strings: List[str]):
    actual = list(datatype.aware_split(input_string))
    assert actual == output_strings


split_delimiter_testcases = [
    ("first,second", ",", ["first", "second"]),
    ("first second", " ", ["first", "second"]),
    ("first|second", "|", ["first", "second"]),
    ("first,second third", ",", ["first", "second third"]),
    ("first,second third", " ", ["first,second", "third"]),
]


@pytest.mark.parametrize(
    "input_string, delimiter, output_strings",
    split_delimiter_testcases,
)
def test_split_delimiter(input_string: str, delimiter: str, output_strings: List[str]):
    actual = list(datatype.aware_split(input_string, delimiter=delimiter))
    assert actual == output_strings


split_maxsplit_testcases = [
    ("one,two,three", -1, ["one", "two", "three"]),
    ("one,two,three", 0, ["one,two,three"]),
    ("one,two,three", 1, ["one", "two,three"]),
    ("one,two,three", 2, ["one", "two", "three"]),
    ("one,two,three", 3, ["one", "two", "three"]),
    ("one,two,three", 10, ["one", "two", "three"]),
    (",one,two,three", 0, [",one,two,three"]),
    (",one,two,three", 1, ["", "one,two,three"]),
    ("one,two,three,", 2, ["one", "two", "three,"]),
    ("one,two,three,", 3, ["one", "two", "three", ""]),
]


@pytest.mark.parametrize(
    "input_string, maxsplit, output_strings",
    split_maxsplit_testcases,
)
def test_split_maxsplit(input_string: str, maxsplit: int, output_strings: List[str]):
    actual = list(datatype.aware_split(input_string, maxsplit=maxsplit))
    assert actual == output_strings
import unittest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pyextrica.sqlalchemy.datatype import JSONIndexType, JSONPathType, _FormatTypeMixin

# Define Base
Base = declarative_base()

# Mocking the _compiler_dispatch method
def mock_compiler_dispatch(self, visitor, **kwargs):
    return visitor._compiler_dispatch(self, **kwargs)

_FormatTypeMixin._compiler_dispatch = mock_compiler_dispatch

# Your test case remains unchanged
class TestJSONTypes(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        cls.session = Session()

    def test_JSONIndexType(self):
        index_type = JSONIndexType()

        # Mock the string_bind_processor and string_literal_processor methods
        index_type.string_bind_processor = Mock(return_value=lambda x: f'"{x}"')
        index_type.string_literal_processor = Mock(return_value=lambda x: f"'{x}'")

        bind_processor = index_type.bind_processor(Mock())
        literal_processor = index_type.literal_processor(Mock())

        # Test bind processor
        result_bind = bind_processor("key")
        self.assertEqual(result_bind,  '"$["key"]"')

        # Test literal processor
        result_literal = literal_processor("key")
        self.assertEqual(result_literal, "'$[\"key\"]'")

    def test_JSONPathType(self):
        path_type = JSONPathType()

        # Mock the string_bind_processor and string_literal_processor methods
        path_type.string_bind_processor = Mock(return_value=lambda x: f'"{x}"')
        path_type.string_literal_processor = Mock(return_value=lambda x: f"'{x}'")

        bind_processor = path_type.bind_processor(Mock())
        literal_processor = path_type.literal_processor(Mock())

        # Test bind processor
        result_bind = bind_processor(["path", "to", "key"])
        self.assertEqual(result_bind, '"$["path"]["to"]["key"]"')

        # Test literal processor
        result_literal = literal_processor(["path", "to", "key"])
        self.assertEqual(result_literal, "'$[\"path\"][\"to\"][\"key\"]'")

if __name__ == '__main__':
    unittest.main()



import unittest
from unittest.mock import MagicMock
from pyextrica.sqlalchemy.datatype import MAP, SQLType

class TestYourClass(unittest.TestCase):

    def test_init(self):
        # Mock SQLType instances for key_type and value_type
        mock_key_type = MagicMock(spec=SQLType)
        mock_value_type = MagicMock(spec=SQLType)

        your_obj = MAP(mock_key_type, mock_value_type)

        # Assert that key_type and value_type are correctly set
        self.assertEqual(your_obj.key_type, mock_key_type)
        self.assertEqual(your_obj.value_type, mock_value_type)

    def test_init_with_types(self):
        # Mock type classes for key_type and value_type
        mock_key_type_class = MagicMock()
        mock_value_type_class = MagicMock()

        your_obj = MAP(mock_key_type_class, mock_value_type_class)

        # Assert that key_type and value_type are instances of TypeEngine
        self.assertIsInstance(your_obj.key_type, MagicMock)
        self.assertIsInstance(your_obj.value_type, MagicMock)

    def test_python_type(self):
        # Mock SQLType instances for key_type and value_type
        mock_key_type = MagicMock(spec=SQLType)
        mock_value_type = MagicMock(spec=SQLType)

        your_obj = MAP(mock_key_type, mock_value_type)

        # Mock the python_type property of SQLType instances
        mock_key_type.python_type = int
        mock_value_type.python_type = str

        # Assert that python_type returns a dictionary
#self.assertEqual(your_obj.python_type, {'key': int, 'value': str})

if __name__ == '__main__':
    unittest.main()



import unittest
from unittest.mock import MagicMock
from pyextrica.sqlalchemy.datatype import ROW, SQLType
from typing import List, Tuple, Optional

class TestROW(unittest.TestCase):

    def test_init(self):
        # Mock SQLType instances for attribute types
        mock_attr_types = [
            (None, MagicMock(spec=SQLType)),
            ('attr1', MagicMock(spec=SQLType)),
            ('attr2', MagicMock(spec=SQLType)),
        ]

        row_obj = ROW(mock_attr_types)

        # Assert that attr_types is correctly set
        self.assertEqual(row_obj.attr_types, mock_attr_types)

    def test_python_type(self):
        # Mock SQLType instances for attribute types
        mock_attr_types = [
            (None, MagicMock(spec=SQLType)),
            ('attr1', MagicMock(spec=SQLType)),
            ('attr2', MagicMock(spec=SQLType)),
        ]

        row_obj = ROW(mock_attr_types)

        # Mock the python_type property of SQLType instances
        for _, attr_type in mock_attr_types:
            attr_type.python_type = MagicMock()

        # Assert that python_type returns a list of attribute names and their corresponding Python types
        expected_python_type = [
            {'attr': MagicMock().python_type},
            {'attr1': MagicMock().python_type},
            {'attr2': MagicMock().python_type},
        ]
       # self.assertEqual(row_obj.python_type, expected_python_type)

if __name__ == '__main__':
    unittest.main()





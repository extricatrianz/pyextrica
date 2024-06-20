# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from asyncio import constants
from mock import Mock, patch
import pyextrica


import pytest

from pyextrica.transaction import IsolationLevel,Transaction



def test_isolation_level_levels() -> None:
    levels = {
        "AUTOCOMMIT",
        "READ_UNCOMMITTED",
        "READ_COMMITTED",
        "REPEATABLE_READ",
        "SERIALIZABLE",
    }

    assert IsolationLevel.levels() == levels


def test_isolation_level_values() -> None:
    values = {
        0, 1, 2, 3, 4
    }

    assert IsolationLevel.values() == values


def test_isolation_level_check_match() -> None:
    assert IsolationLevel.check(3) == 3


def test_isolation_level_check_mismatch() -> None:
    with pytest.raises(ValueError):
        IsolationLevel.check(-1)


from unittest.mock import Mock
import unittest
import unittest
from unittest.mock import MagicMock, patch
from pyextrica.transaction import Transaction, pyextrica
# Assuming your Transaction class is in a module named your_module
from pyextrica.transaction import Transaction

from pyextrica.transaction import Transaction, constants, NO_TRANSACTION

class TestTransaction(unittest.TestCase):
    def setUp(self):
        # Create a mock TrinoRequest object for testing
        self.mock_request = Mock()
        self.mock_request = MagicMock()
        self.transaction = Transaction(self.mock_request)

    def test_transaction_initialization(self):
        # Create a Transaction object with the mock request
        transaction = Transaction(self.mock_request)

        # Assert that the transaction ID is initialized to NO_TRANSACTION
        self.assertEqual(transaction.id, "NONE")

     

    


    def test_transaction_begin_failure(self):
        # Create a Transaction object with the mock request
        transaction = Transaction(self.mock_request)

        # Mock a failed response from TrinoRequest.post
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 500
        self.mock_request.post.return_value = mock_response

        # Assert that DatabaseError is raised when begin fails
        with self.assertRaises(pyextrica.exceptions.DatabaseError):
            transaction.begin()

  

    

        
    def test_commit(self):
        mock_query = MagicMock()
        self.mock_request.transaction_id = 'mock_transaction_id'
        mock_execute_result = [1, 2, 3]  # Mocking execute result
        mock_query.execute.return_value = mock_execute_result

        with patch.object(pyextrica.client, 'TrinoQuery', return_value=mock_query):
            self.transaction.commit()

        # Check if transaction ID is reset after commit
        self.assertEqual(self.transaction.id, 'NONE')
        self.assertEqual(self.mock_request.transaction_id, self.transaction.id)

    def test_rollback(self):
        mock_query = MagicMock()
        self.mock_request.transaction_id = 'mock_transaction_id'
        mock_execute_result = [4, 5, 6]  # Mocking execute result
        mock_query.execute.return_value = mock_execute_result

        with patch.object(pyextrica.client, 'TrinoQuery', return_value=mock_query):
            self.transaction.rollback()

        # Check if transaction ID is reset after rollback
        self.assertEqual(self.transaction.id, 'NONE')
        self.assertEqual(self.mock_request.transaction_id, self.transaction.id)




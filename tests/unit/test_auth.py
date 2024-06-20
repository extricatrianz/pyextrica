



# import unittest
# from unittest.mock import MagicMock
# from requests import Session
# from pyextrica.auth import ExtricaAuthentication, _BearerAuth


# class TestExtricaAuthentication(unittest.TestCase):
#     def test_set_http_session(self):
#         token = "dummy_token"
#         auth = ExtricaAuthentication(token)
#         http_session = Session()
#         http_session.auth = MagicMock()

#         result_session = auth.set_http_session(http_session)

#         self.assertEqual(http_session.auth.token, token)
#         self.assertEqual(result_session, http_session)

#     def test_get_exceptions(self):
#         auth = ExtricaAuthentication("dummy_token")
#         exceptions = auth.get_exceptions()

#         self.assertEqual(exceptions, ())

#     def test_equality(self):
#         token1 = "token1"
#         token2 = "token2"
#         auth1 = ExtricaAuthentication(token1)
#         auth2 = ExtricaAuthentication(token2)
#         auth3 = ExtricaAuthentication(token1)

#         self.assertNotEqual(auth1, auth2)
#         self.assertEqual(auth1, auth3)


# if __name__ == "__main__":
#     unittest.main()


import unittest
from unittest.mock import MagicMock
from pyextrica.auth import ExtricaAuthentication, _BearerAuth
#from pyextrica.auth import _BearerAuth-code.pyextrica.auth import ExtricaAuthentication
from requests import PreparedRequest, Session
from pyextrica.auth import Authentication


class TestAuthentication(unittest.TestCase):
    def test_set_http_session(self):
        class MockAuth(Authentication):
            def set_http_session(self, http_session: Session) -> Session:
                return http_session

        auth = MockAuth()
        http_session = Session()

        result_session = auth.set_http_session(http_session)

        self.assertEqual(result_session, http_session)

    def test_get_exceptions(self):
        class MockAuth(Authentication):
            def set_http_session(self, http_session: Session) -> Session:
                return http_session

        auth = MockAuth()

        exceptions = auth.get_exceptions()

        self.assertEqual(exceptions, ())

class TestExtricaAuthentication(unittest.TestCase):
    

    def test_get_exceptions(self):
        # Create an instance of ExtricaAuthentication with a token
        auth = ExtricaAuthentication("dummy_token")

        # Call get_exceptions method and assert the return value
        exceptions = auth.get_exceptions()
        self.assertEqual(exceptions, ())

    def test_equality(self):
        # Create instances of ExtricaAuthentication with different tokens
        token1 = "token1"
        token2 = "token2"
        auth1 = ExtricaAuthentication(token1)
        auth2 = ExtricaAuthentication(token2)
        auth3 = ExtricaAuthentication(token1)

        # Assert that equality works correctly
        self.assertNotEqual(auth1, auth2)
        self.assertEqual(auth1, auth3)





class TestBearerAuth(unittest.TestCase):
    def test_init(self):
        
        token = "dummy_token"
        auth = _BearerAuth(token)

        # Assert that the token was set correctly
        self.assertEqual(auth.token, token)

    def test_call(self):
        # Create an instance of _BearerAuth with a token
        token = "dummy_token"
        auth = _BearerAuth(token)

        # Create a mock PreparedRequest object
        request = MagicMock(spec=PreparedRequest)
        request.headers = {}

        # Call the __call__ method of _BearerAuth with the mock request
        result_request = auth(request)

        # Assert that the Authorization header was set correctly
        self.assertEqual(request.headers["Authorization"], "Bearer " + token)
        self.assertEqual(result_request, request)


if __name__ == "__main__":
    unittest.main()


import unittest
from urllib.parse import quote_plus
from pyextrica.sqlalchemy.util import _url



from urllib.parse import quote_plus


# class TestURLFunction(unittest.TestCase):
#     def test_valid_url_generation(self):
#         host = 'example.com'
#         port = 8080
#         user = 'user'
#         password = 'password'
#         catalog = 'catalog'
#         schema = 'schema'
#         source = 'source'
#         session_properties = {'key': 'value'}
#         http_headers = {'header': 'value'}
#         extra_credential = [('key1', 'value1'), ('key2', 'value2')]
#         client_tags = ['tag1', 'tag2']
#         legacy_primitive_types = True
#         legacy_prepared_statements = False
#         access_token = 'token'
#         cert = 'cert_data'
#         key = 'key_data'
#         verify = True
#         roles = {'role1': 'user1', 'role2': 'user2'}
#         platform = 'data_products'

#         generated_url = _url(
#             host=host,
#             port=port,
#             user=user,
#             password=password,
#             catalog=catalog,
#             schema=schema,
#             source=source,
#             session_properties=session_properties,
#             http_headers=http_headers,
#             extra_credential=extra_credential,
#             client_tags=client_tags,
#             legacy_primitive_types=legacy_primitive_types,
#             legacy_prepared_statements=legacy_prepared_statements,
#             access_token=access_token,
#             cert=cert,
#             key=key,
#             verify=verify,
#             roles=roles,
#             platform=platform
#         )

#         self.assertIn(user, generated_url)
#         self.assertIn(password, generated_url)
#         self.assertIn(host, generated_url)
#         self.assertIn(str(port), generated_url)
#         self.assertIn(quote_plus(catalog), generated_url)
#         self.assertIn(quote_plus(schema), generated_url)
#         self.assertIn(source, generated_url)
#         self.assertIn(quote_plus(str(session_properties)), generated_url)
#         self.assertIn(quote_plus(str(http_headers)), generated_url)
#         self.assertIn(quote_plus(str(extra_credential)), generated_url)
#         self.assertIn(quote_plus(str(client_tags)), generated_url)
#         self.assertIn(str(legacy_primitive_types), generated_url)
#         self.assertIn(str(legacy_prepared_statements), generated_url)
#         self.assertIn(quote_plus(access_token), generated_url)
#         self.assertIn(quote_plus(cert), generated_url)
#         self.assertIn(quote_plus(key), generated_url)
#         self.assertIn(str(verify), generated_url)
#         self.assertIn(quote_plus(str(roles)), generated_url)
#         self.assertIn(platform, generated_url)

#     def test_exception_handling(self):
#         with self.assertRaises(ValueError) as context:
#             _url()
#         self.assertTrue('host must be specified' in str(context.exception))

if __name__ == '__main__':
    unittest.main()

import unittest
import decimal
import pandas as pd
import datetime
import uuid
from komlogd.api.common import crypto, timeuuid
from komlogd.api.protocol import validation
from komlogd.api.model.metrics import Metrics

class ApiModelValidationTest(unittest.TestCase):

    def test_validate_username_invalid(self):
        ''' validate_username should raise a TypeError if value is invalid '''
        values = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'username\n',
            'username\t',
            'username\r',
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
        ]
        for value in values:
            with self.assertRaises(TypeError) as cm:
                validation.validate_username(value)
            self.assertEqual(str(cm.exception), 'username is not valid: '+str(value))

    def test_validate_username_valied(self):
        ''' validate_username should return True if value is a valid username '''
        usernames = [
            'username',
            'Username',
            'username1',
            'username-',
            'username_',
            '_username',
            '-username',
        ]
        for username in usernames:
            self.assertTrue(validation.validate_username(username))

    def test_validate_uri_failure_invalid_uri(self):
        ''' validate_uri should raise TypeError if uri is not valid '''
        uris = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
        ]
        for uri in uris:
            with self.assertRaises(TypeError) as cm:
                validation.validate_uri(uri)

    def test_validate_privkey_failure(self):
        ''' validate_privkey should raise a TypeError exception if value is not a private key '''
        values = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
            crypto.generate_rsa_key(key_size=2048) #min valid size is 4096
        ]
        for value in values:
            with self.assertRaises(TypeError) as cm:
                validation.validate_privkey(value)
            self.assertEqual(str(cm.exception),'Invalid private key')

    def test_validate_privkey_success(self):
        ''' validate_privkey should raise a TypeError exception if value is a valid private key '''
        privkey=crypto.generate_rsa_key()
        self.assertTrue(validation.validate_privkey(privkey))

    def test_validate_uri_success(self):
        ''' validate_uri should return True if uri is valid '''
        uris = [
            'valid.uri',
            'valid.uri.-dash',
            'valid.uri._underscore',
            'valid',
            'user:local.uri'
        ]
        for uri in uris:
            self.assertTrue(validation.validate_uri(uri))

    def test_validate_local_uri_failure(self):
        ''' raise TypeError if value is not a valid local uri '''
        uris = [
            None,
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
        ]
        for uri in uris:
            with self.assertRaises(TypeError) as cm:
                validation.validate_local_uri(uri)
            self.assertEqual(str(cm.exception), 'value is not a string: '+str(uri))
        uris = [
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            'user:local.uri'
        ]
        for uri in uris:
            with self.assertRaises(TypeError) as cm:
                validation.validate_local_uri(uri)
            self.assertEqual(str(cm.exception), 'value is not a valid local uri: '+str(uri))

    def test_validate_local_uri_success(self):
        ''' validate_local_uri should return True if uri is valid '''
        uris = [
            'valid.uri',
            'valid.uri.-dash',
            'valid.uri._underscore',
            'valid',
        ]
        for uri in uris:
            self.assertTrue(validation.validate_local_uri(uri))

    def test_validate_uri_level_failure(self):
        ''' validate_uri_level should raise TypeError value is not a valid uri level '''
        uris = [
            None,
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
        ]
        for uri in uris:
            with self.assertRaises(TypeError) as cm:
                validation.validate_uri_level(uri)
            self.assertEqual(str(cm.exception), 'value is not a string: '+str(uri))
        uris = [
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            'user:local.uri'
            'valid.uri',
            'valid.uri.-dash',
            'valid.uri._underscore',
        ]
        for uri in uris:
            with self.assertRaises(TypeError) as cm:
                validation.validate_uri_level(uri)
            self.assertEqual(str(cm.exception), 'value is not a valid uri level: '+str(uri))

    def test_validate_uri_level_success(self):
        ''' validate_uri_level should return True if value is a valid uri level '''
        uris = [
            'valid',
            '-',
            '_',
            'VALID',
            '00000',
            uuid.uuid4().hex,
        ]
        for uri in uris:
            self.assertTrue(validation.validate_uri_level(uri))

    def test_validate_ts_failure_invalid_ts(self):
        ''' validate_ts should fail if ts is not valid '''
        timestamps = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            '121',
            '323.1231',
            -1,
            -1.232,
            -1e2,
            2**32,
            decimal.Decimal('23'),
            decimal.Decimal(23),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
            pd.Timestamp('now'),
            ['a','list'],
            ('a','tuple'),
            uuid.uuid1(),
            uuid.uuid4(),
            uuid.uuid4().hex,
            datetime.datetime.utcnow(),
            'NaN',
            'Infinity',
            '-Infinity',
            {'a':'dict'},
            {'set'},
            '3016-07-18T17:07:00.002323Z', #year greater than max supported
            '30160-07-18T17:07:00.002323Z', #year greater than max supported
            '-2016-07-18T17:07:00.002323Z', #negative years not allowed
            '1400-07-18T17:07:00.002323Z', #year lower than min supported
            '2016-7-18T17:07:00.002323Z', #month without leading 0
            '2016-07-38T17:07:00.002323Z', #day greater than 31
            '2016-07-8T17:07:00.002323Z', #day without leading 0
            '2016-07-18 17:07:00.002323Z', #no T Time separator
            '2016-07-1817:07:00.002323Z', #no Time separator
            '2016-07-18T24:07:00.002323Z', #hour 24 not valid
            '2016-07-18T17:60:00.002323Z', #min 60 not valid
            '2016-07-18T17:00:60.002323Z', #sec 60 not valid
            '2016-07-18T17:07:00.002323z', #z not capital
            '2016-07-18T17:07:00.002323+24:00',#24h tz offset not valid
            '2016-07-18T17:07:00.002323-23:60', #60min tz offset not valid
            '2016-07-18T17:07:00.002323-23:40:00', #seconds in tz offset not valid
            '2016-07-18T17:07:00.002323+0000', #no colon in offset not valid
            '2016/07/18T17:07:00.002323+0000', #no hyphen
            '2016-07-18T17:07:00.002323', #not tz dont allowed
            pd.Timestamp('now').isoformat(),
            datetime.datetime.utcnow().isoformat(),
        ]
        for ts in timestamps:
            with self.assertRaises(TypeError) as cm:
                validation.validate_ts(ts)

    def test_validate_ts_success(self):
        ''' validate_ts should return True if ts is valid '''
        timestamps = [
            pd.Timestamp('now',tz='utc'),
            pd.Timestamp('now',tz='Europe/Madrid'),
            '2016-07-18T17:07:00.002323Z',
            '2016-07-18T17:07:00.002323+00:22',
            '2016-07-18T17:07:00.002323-00:22',
            '2016-07-18T17:07:00+00:22',
        ]
        for ts in timestamps:
            self.assertTrue(validation.validate_ts(ts))

    def test_validate_ds_value_failure_invalid_content(self):
        ''' validate_ds_value should fail if content is not valid '''
        contents = [
            None,
            -1,
            -1.232,
            -1e2,
            2**32,
            decimal.Decimal('23'),
            decimal.Decimal(23),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
            'a'*2**17+'a',
        ]
        for content in contents:
            with self.assertRaises(TypeError) as cm:
                validation.validate_ds_value(content)

    def test_validate_ds_value_success(self):
        ''' validate_ds_value should succeed if content is valid '''
        contents = [
            'a'*2**17,
            '§ºÆ¢¢§Ŧ®º¢Ð',
            'Some content\n with new lines\n tabs\t\tor ñ Ñ € æ¢ other chars.'
        ]
        for content in contents:
            self.assertTrue(validation.validate_ds_value(content))

    def test_validate_dp_value_failure_invalid_content(self):
        ''' validate_dp_value should fail if content is not valid '''
        contents = [
            None,
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
            'a'*2**17+'a',
            '1.1.1',
            '1,1',
            '1:1',
            '-1.e',
            '1e',
            '23,23e3',
            '23.23e3.3',
        ]
        for content in contents:
            with self.assertRaises(TypeError) as cm:
                validation.validate_dp_value(content)

    def test_validate_dp_value_success(self):
        ''' validate_dp_value should succeed if content is valid '''
        contents = [
            'NaN',
            decimal.Decimal('23'),
            decimal.Decimal(23),
            -1,
            -1.232,
            -1e2,
            2**32,
            '1'*2**7,
            '1.1',
            '1.5e+4',
            '1.005E+43',
            '-1.3e-34',
            '1e4',
            '-1e4',
            '-1.3e-3',
            '-1.3E-34',
            '1E4',
            '-1E4',
            '-1.3E-3',
            '+1.3E+3',
            '1.5e4\n',
            ' 23 ',
            ' 32\n',
            '32\n',
            '1'*2**7+'1',
        ]
        for content in contents:
            self.assertTrue(validation.validate_dp_value(content))

    def test_is_message_sequence_failure(self):
        ''' is_message_sequence should return False if param is no a valid sequence '''
        params = [
            'NaN',
            decimal.Decimal('23'),
            decimal.Decimal(23),
            -1,
            -1.232,
            -1e2,
            2**32,
            '1'*2**7,
            '1.1',
            '1.5e+4',
            '1.005E+43',
            '-1.3e-34',
            '1e4',
            '-1e4',
            '-1.3e-3',
            '-1.3E-34',
            '1E4',
            '-1E4',
            '-1.3E-3',
            '+1.3E+3',
            '1.5e4\n',
            ' 23 ',
            ' 32\n',
            '32\n',
            '1'*2**7+'1',
            timeuuid.TimeUUID().hex,
            uuid.uuid1(),
            uuid.uuid1().hex,
            uuid.uuid4().hex[0:20],
            uuid.uuid1().hex[0:10],
            uuid.uuid1().hex[0:20],
            uuid.uuid4(),
            {'set'},
            {'a':'dict'},
            ['a','list'],
            ('a','tuple'),
            None,
            1,
        ]
        for param in params:
            self.assertFalse(validation.is_message_sequence(param))

    def test_is_message_sequence_success(self):
        ''' is_message_sequence should return False if param is no a valid sequence '''
        params = [
            timeuuid.TimeUUID(),
        ]
        for param in params:
            self.assertTrue(validation.is_message_sequence(param))

    def test_is_local_uri_failure(self):
        ''' is_local_uri should return False if value is not a valid local uri '''
        uris = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
            'user:local.uri'
        ]
        for uri in uris:
            self.assertFalse(validation.is_local_uri(uri))

    def test_is_local_uri_success(self):
        ''' is_local_uri should return True if value is a valid local uri '''
        uris = [
            'valid.uri',
            'valid.uri.-dash',
            'valid.uri._underscore',
            'valid',
        ]
        for uri in uris:
            self.assertTrue(validation.is_local_uri(uri))

    def test_is_global_uri_failure(self):
        ''' is_global_uri should return False if value is not a valid global uri '''
        uris = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'uri\n',
            'uri\t',
            'uri\r',
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
        ]
        for uri in uris:
            self.assertFalse(validation.is_global_uri(uri))

    def test_is_global_uri_success(self):
        ''' is_global_uri should return True if value is a valid global uri '''
        uris = [
            'username:valid.uri',
            '_Username_:valid.uri.-dash',
            'user:valid.uri._underscore',
            'User1:valid',
        ]
        for uri in uris:
            self.assertTrue(validation.is_global_uri(uri))

    def test_is_username_failure(self):
        ''' is_username should return False if value is a valid username '''
        values = [
            None,
            '  not valid ',
            '..not_valid',
            'not..valid',
            'ññ',
            'username\n',
            'username\t',
            'username\r',
            121,
            323.1231,
            decimal.Decimal('23'),
            ['a','list'],
            {'set'},
            ('a','tuple'),
            {'a':'dict'},
        ]
        for value in values:
            self.assertFalse(validation.is_username(value))

    def test_is_username_success(self):
        ''' is_username should return True if value is a valid username '''
        usernames = [
            'username',
            'Username',
            'username1',
            'username-',
            'username_',
            '_username',
            '-username',
        ]
        for username in usernames:
            self.assertTrue(validation.is_username(username))


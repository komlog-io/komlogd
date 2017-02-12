import unittest
import decimal
import pandas as pd
import datetime
import uuid
from komlogd.api.model import validation
from komlogd.api.model.types import Metrics

class ApiModelValidationTest(unittest.TestCase):

    def test_validate_uri_failure_invalid_uri(self):
        ''' validate_uri should fail if uri is not valid '''
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

    def test_validate_uri_success(self):
        ''' validate_uri should return None if uri is valid '''
        uris = [
            'valid.uri',
            'valid.uri.-dash',
            'valid.uri._underscore',
            'valid',
        ]
        for uri in uris:
            self.assertTrue(validation.validate_uri(uri))

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

    def test_validate_ds_content_failure_invalid_content(self):
        ''' validate_ds_content should fail if content is not valid '''
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
                validation.validate_ds_content(content)

    def test_validate_ds_content_success(self):
        ''' validate_ds_content should succeed if content is valid '''
        contents = [
            'a'*2**17,
            '§ºÆ¢¢§Ŧ®º¢Ð',
            'Some content\n with new lines\n tabs\t\tor ñ Ñ € æ¢ other chars.'
        ]
        for content in contents:
            self.assertTrue(validation.validate_ds_content(content))

    def test_validate_dp_content_failure_invalid_content(self):
        ''' validate_dp_content should fail if content is not valid '''
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
                validation.validate_dp_content(content)

    def test_validate_dp_content_success(self):
        ''' validate_dp_content should succeed if content is valid '''
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
            self.assertTrue(validation.validate_dp_content(content))

    def test_validate_metric_type_failure(self):
        ''' validate_metric should fail if metric is not a Metric enum element or its value does not belong to any of them '''
        contents = [
            None,
            [Metrics.DATASOURCE],
            {Metrics.DATASOURCE},
            (Metrics.DATASOURCE,Metrics.DATAPOINT),
            {Metrics.DATASOURCE:Metrics.DATASOURCE},
            'non_existent_metric_value',
            decimal.Decimal('23'),
            -1,
            -1.2,
            uuid.uuid4(),
            uuid.uuid1(),
            pd.Timestamp('now',tz='utc')
        ]
        for content in contents:
            with self.assertRaises(TypeError) as cm:
                validation.validate_metric_type(content)
            self.assertEqual(str(cm.exception), 'Invalid metric type')

    def test_validate_metric_type_success(self):
        ''' validate_metric should succeed if metric is a Metric enum element or its value belongs to any of them '''
        metrics=[metric for metric in Metrics]
        metrics_values=[metric.value for metric in Metrics]
        contents=metrics+metrics_values
        for content in contents:
            self.assertTrue(validation.validate_metric_type(content))


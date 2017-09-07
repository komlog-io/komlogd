import unittest
import uuid
import pandas as pd
import decimal
from komlogd.api.common import exceptions
from komlogd.api.common.timeuuid import TimeUUID
from komlogd.api.model import metrics

class ApiModelMetricsTest(unittest.TestCase):

    def test_metrics_enum(self):
        ''' Check existing metric types '''
        self.assertEqual(len(metrics.Metrics),2)
        self.assertEqual(metrics.Metrics.DATASOURCE.value,'d')
        self.assertEqual(metrics.Metrics.DATAPOINT.value,'p')

    def test_Metric_creation_failure(self):
        ''' creating a Metric object is disallowed '''
        with self.assertRaises(TypeError) as cm:
            m = metrics.Metric(uri='uri',session=None)

    def test_Datasource_creation_failure_invalid_uri(self):
        ''' Datasource creation should fail if uri is invalid '''
        uri = 'Invalid uri'
        with self.assertRaises(TypeError) as cm:
            ds = metrics.Datasource(uri=uri)

    def test_Datasource_creation_failure_invalid_supplies_type(self):
        ''' Datasource creation should fail if supplies is not a list '''
        uri = 'ds'
        supplies = {'a':'dict'}
        with self.assertRaises(TypeError) as cm:
            ds = metrics.Datasource(uri=uri, supplies=supplies)
        self.assertEqual(str(cm.exception), 'Invalid supplies parameter')

    def test_Datasource_creation_failure_invalid_supplies_uris_strings(self):
        ''' Datasource creation should fail if supplies uris are not valid uris '''
        uri = 'ds'
        supplies = ['valid.uri',uuid.uuid4()]
        with self.assertRaises(TypeError) as cm:
            ds = metrics.Datasource(uri=uri, supplies=supplies)
        self.assertEqual(str(cm.exception), 'value is not a string: '+str(supplies[1]))

    def test_Datasource_creation_failure_invalid_supplies_uris(self):
        ''' Datasource creation should fail if supplies uris are not valid uris '''
        uri = 'ds'
        supplies = ['valid.uri','invalid uri']
        with self.assertRaises(TypeError) as cm:
            ds = metrics.Datasource(uri=uri, supplies=supplies)
        self.assertEqual(str(cm.exception), 'value is not a valid local uri: invalid uri')

    def test_Datasource_creation_failure_invalid_supplies_uris_not_local_uri(self):
        ''' Datasource creation should fail if supplies uris are not local uris '''
        uri = 'ds'
        supplies = ['valid.uri','global:uri']
        with self.assertRaises(TypeError) as cm:
            ds = metrics.Datasource(uri=uri, supplies=supplies)
        self.assertEqual(str(cm.exception), 'value is not a valid local uri: global:uri')

    def test_Datasource_creation_success_local_uri(self):
        ''' Datasource creation should succeed if no session exists yet '''
        uri = 'ds.uri'
        ds = metrics.Datasource(uri=uri)
        self.assertEqual(ds.uri, uri)
        self.assertEqual(ds.supplies, None)
        self.assertEqual(ds._m_type_, metrics.Metrics.DATASOURCE)

    def test_Datasource_creation_success_local_uri_with_supplies_uris(self):
        ''' Datasource creation should succeed if uri is a valid local uir and supplies are valid local uris '''
        uri = 'ds.uri'
        supplies = ['dp1','dp2','dp3','other.dp1','other.dp2','other.dp3']
        ds = metrics.Datasource(uri=uri, supplies=supplies)
        self.assertEqual(ds.uri, uri)
        self.assertEqual(ds.supplies, sorted(supplies))
        self.assertEqual(ds._m_type_, metrics.Metrics.DATASOURCE)

    def test_Datasource_creation_success_global_uri(self):
        ''' Datasource creation should succeed if we pass a global uri '''
        uri = 'user:uri'
        ds = metrics.Datasource(uri=uri)
        self.assertEqual(ds.uri, uri)
        self.assertEqual(ds.supplies, None)
        self.assertEqual(ds._m_type_, metrics.Metrics.DATASOURCE)

    def test_Datasource_creation_success_global_uri_with_supplies_uris(self):
        ''' Datasource creation should succeed if we pass a global uri and valid supplies uris'''
        uri = 'user:uri'
        supplies = ['dp1','dp2','dp3','other.dp1','other.dp2','other.dp3']
        ds = metrics.Datasource(uri=uri, supplies=supplies)
        self.assertEqual(ds.supplies, sorted(supplies))
        self.assertEqual(ds.uri, uri)

    def test_compare_Datasource_local_uri_case_sensitive(self):
        ''' if we compare two Datasource objects, local uri is case sensitive '''
        uri1 = 'my_uri'
        uri2 = 'MY_uri'
        ds1 = metrics.Datasource(uri1)
        ds2 = metrics.Datasource(uri2)
        self.assertNotEqual(ds1, ds2)

    def test_compare_Datasource_username_in_uri_case_insensitive(self):
        ''' if we compare two Datasource objects, username in uri is case insensitive '''
        uri1 = 'USER:my_uri'
        uri2 = 'user:my_uri'
        uri3 = 'user:my_uri2'
        ds1 = metrics.Datasource(uri1)
        ds2 = metrics.Datasource(uri2)
        ds3 = metrics.Datasource(uri3)
        self.assertEqual(ds1, ds2)
        self.assertNotEqual(ds1, ds3)
        self.assertNotEqual(ds2, ds3)

    def test_compare_Datasource_with_no_metric_object_failure(self):
        ''' comparing a Datasource object with a non Metric one will return False '''
        ds = metrics.Datasource('uri')
        others = ['uri',None, 1, ['a','list'], {'set'}, {'a':'dict'}, uuid.uuid4()]
        for other in others:
            self.assertFalse(ds == other)

    def test_Datapoint_creation_failure_invalid_uri(self):
        ''' Datapoint creation should fail if uri is invalid '''
        uri = 'Invalid uri'
        with self.assertRaises(TypeError) as cm:
            dp = metrics.Datapoint(uri=uri)

    def test_Datapoint_creation_success(self):
        ''' Datapoint creation should succeed if no session exists yet '''
        uri = 'dp.uri'
        dp = metrics.Datapoint(uri=uri)
        self.assertEqual(dp.uri, uri)
        self.assertEqual(dp._m_type_, metrics.Metrics.DATAPOINT)

    def test_Datapoint_creation_success_global_uri(self):
        ''' Datapoint creation should succeed if we pass a global uri '''
        uri = 'user:uri'
        dp = metrics.Datapoint(uri=uri)
        self.assertEqual(dp.uri, uri)

    def test_compare_Datapoint_local_uri_case_sensitive(self):
        ''' if we compare two Datapoint objects, local uri is case sensitive '''
        uri1 = 'my_uri'
        uri2 = 'MY_uri'
        dp1 = metrics.Datapoint(uri1)
        dp2 = metrics.Datapoint(uri2)
        self.assertNotEqual(dp1, dp2)

    def test_compare_Datapoint_username_in_uri_case_insensitive(self):
        ''' if we compare two Datapoint objects, username in uri is case insensitive '''
        uri1 = 'USER:my_uri'
        uri2 = 'user:my_uri'
        uri3 = 'user:my_uri2'
        dp1 = metrics.Datapoint(uri1)
        dp2 = metrics.Datapoint(uri2)
        dp3 = metrics.Datapoint(uri3)
        self.assertEqual(dp1, dp2)
        self.assertNotEqual(dp1, dp3)
        self.assertNotEqual(dp2, dp3)

    def test_compare_Datapoint_with_no_metric_object_failure(self):
        ''' comparing a Datapoint object with a non Metric one will return False '''
        dp = metrics.Datapoint('uri')
        others = ['uri',None, 1, ['a','list'], {'set'}, {'a':'dict'}, uuid.uuid4()]
        for other in others:
            self.assertFalse(dp == other)

    def test_compare_Datapoint_and_Datasource_objects_with_same_uri(self):
        ''' When comparing metrics, m_type will be compared  '''
        uri = 'uri'
        dp = metrics.Datapoint(uri)
        ds = metrics.Datasource(uri)
        self.assertNotEqual(ds,dp)

    def test_retrieving_metrics_from_hashed_types(self):
        ''' Retrieving elements from hashed types '''
        uri_ds = 'uri.ds'
        uri_dp = 'uri.dp'
        ds1 = metrics.Datasource(uri_ds)
        ds2 = metrics.Datasource(uri_ds)
        self.assertEqual(ds1,ds2)
        dp1 = metrics.Datapoint(uri_dp)
        dp2 = metrics.Datapoint(uri_dp)
        self.assertEqual(dp1,dp2)
        value_ds = 1
        value_dp = 2
        my_dict = {ds1:value_ds, dp1:value_dp}
        self.assertTrue(my_dict[ds2] == value_ds)
        self.assertTrue(my_dict[dp2] == value_dp)
        same_uri = 'uri'
        ds3 = metrics.Datasource(same_uri)
        dp3 = metrics.Datapoint(same_uri)
        self.assertNotEqual(ds3,dp3)
        my_dict2 = {ds3:value_ds, dp3:value_dp}
        self.assertTrue(my_dict2[ds3] == value_ds)
        self.assertTrue(my_dict2[dp3] == value_dp)

    def test_create_Anomaly_from_Datapoint_success(self):
        ''' creating an Anomaly object should succeed, and it should behave as a standard Datapoint object '''
        uri = 'uri'
        dp = metrics.Datapoint(uri)
        anom = metrics.Anomaly(dp)
        dp_anomaly = metrics.Datapoint(uri = anom.uri)
        self.assertEqual(anom, dp_anomaly)
        ds_anomaly = metrics.Datasource(uri = anom.uri)
        self.assertNotEqual(anom, ds_anomaly)

    def test_create_Anomaly_from_Datasource_success(self):
        ''' creating an Anomaly object should succeed, and it should behave as a standard Datapoint object '''
        uri = 'uri'
        ds = metrics.Datasource(uri)
        anom = metrics.Anomaly(ds)
        dp_anomaly = metrics.Datapoint(uri = anom.uri)
        self.assertEqual(anom, dp_anomaly)
        ds_anomaly = metrics.Datasource(uri = anom.uri)
        self.assertNotEqual(anom, ds_anomaly)

    def test_create_Tag_from_Datapoint_success(self):
        ''' creating a Tag object should succeed, and it should behave as a standard Datapoint object '''
        uri = 'uri'
        dp = metrics.Datapoint(uri)
        tag = metrics.Tag(dp, key='key', value='value')
        dp_tag = metrics.Datapoint(uri = tag.uri)
        self.assertEqual(tag, dp_tag)
        ds_tag = metrics.Datasource(uri = tag.uri)
        self.assertNotEqual(tag, ds_tag)

    def test_create_Tag_from_Datasource_success(self):
        ''' creating a Tag object should succeed, and it should behave as a standard Datapoint object '''
        uri = 'uri'
        keys = ['my_key','1-my_key1','_','-','My_KEY']
        values = ['my_value','1-my_value1','_','-','_My_VALUE_12_']
        for key in keys:
            for value in values:
                ds = metrics.Datasource(uri)
                tag = metrics.Tag(ds, key=key, value=value)
                dp_tag = metrics.Datapoint(uri = tag.uri)
                self.assertEqual(tag, dp_tag)
                ds_tag = metrics.Datasource(uri = tag.uri)
                self.assertNotEqual(tag, ds_tag)

    def test_create_Tag_failure_invalid_key(self):
        ''' creating a Tag object should fail if key is not a string with characters [a-zA-Z0-9\-_] '''
        keys = [None, 1, 1.1, ['a','list'],{'set'},{'a':'dict'},uuid.uuid4()]
        value = 'value'
        dp = metrics.Datapoint('uri')
        for key in keys:
            with self.assertRaises(TypeError) as cm:
                tag = metrics.Tag(dp, key=key, value=value)
            self.assertEqual(str(cm.exception),'value is not a string: '+str(key))
        keys = ['not valid uri','.uri','user:uri','uri\n','uri\r\n','uri\t',':uri','\turi','\nuri','\0uri']
        for key in keys:
            with self.assertRaises(TypeError) as cm:
                tag = metrics.Tag(dp, key=key, value=value)
            self.assertEqual(str(cm.exception),'value is not a valid uri level: '+key)

    def test_create_Tag_failure_invalid_value(self):
        ''' creating a Tag object should fail if value is not a string with characters [a-zA-Z0-9\-_] '''
        key = 'key'
        values = [None, 1, 1.1, ['a','list'],{'set'},{'a':'dict'},uuid.uuid4()]
        dp = metrics.Datapoint('uri')
        for value in values:
            with self.assertRaises(TypeError) as cm:
                tag = metrics.Tag(dp, key=key, value=value)
            self.assertEqual(str(cm.exception),'value is not a string: '+str(value))
        values = ['not valid uri','.uri','user:uri','uri\n','uri\r\n','uri\t',':uri','\turi','\nuri','\0uri']
        for value in values:
            with self.assertRaises(TypeError) as cm:
                tag = metrics.Tag(dp, key=key, value=value)
            self.assertEqual(str(cm.exception),'value is not a valid uri level: '+value)

    def test_create_Sample_failure_non_Metric_obj(self):
        ''' creating a Sample object should fail if we pass a non Metric object '''
        mets = [None, 1, 1.1, 'string', ('a','tuple'), ['a','list'],{'set'}, {'a':'dict'}, uuid.uuid4()]
        t = TimeUUID()
        value = 5
        for m in mets:
            with self.assertRaises(TypeError) as cm:
                s = metrics.Sample(metric=m, t=t, value=value)
            self.assertEqual(str(cm.exception),'Invalid metric parameter')

    def test_create_Sample_failure_invalid_t(self):
        ''' creating a Sample object should fail if we pass an invalid t '''
        mets = [metrics.Datasource('uri'), metrics.Datapoint('uri')]
        ts = [None, 1, 1.1, 'string', ('a','tuple'), ['a','list'],{'set'}, {'a':'dict'}, uuid.uuid4()]
        value = 5
        for m in mets:
            for t in ts:
                with self.assertRaises(TypeError) as cm:
                    s = metrics.Sample(metric=m, t=t, value=value)
                self.assertEqual(str(cm.exception),'value is not a valid TimeUUID: '+str(t))

    def test_create_Sample_failure_invalid_value_for_datasource(self):
        ''' creating a Sample object should fail if we pass an invalid value for a datasource sample '''
        met = metrics.Datasource('uri')
        t = TimeUUID()
        values = [None, 1, 1.1, ('a','tuple'), ['a','list'],{'set'}, {'a':'dict'}, uuid.uuid4()]
        for value in values:
            with self.assertRaises(TypeError) as cm:
                s = metrics.Sample(metric=met, t=t, value=value)
            self.assertEqual(str(cm.exception),'value not a string')

    def test_create_Sample_failure_invalid_value_for_datapoint(self):
        ''' creating a Sample object should fail if we pass an invalid value for a datasource sample '''
        met = metrics.Datapoint('uri')
        t = TimeUUID()
        values = [None, 'string', ('a','tuple'), ['a','list'],{'set'}, {'a':'dict'}, uuid.uuid4()]
        for value in values:
            with self.assertRaises(TypeError) as cm:
                s = metrics.Sample(metric=met, t=t, value=value)
            self.assertEqual(str(cm.exception),'value not a number')

    def test_create_Sample_success_datasource(self):
        ''' creating a Sample should succeed for a datasource metric '''
        m = metrics.Datasource('uri')
        t = TimeUUID()
        value = 'something'
        s = metrics.Sample(metric=m, t=t, value=value)
        self.assertEqual(s.t,t)
        self.assertEqual(s.metric,m)
        self.assertEqual(s.value,value)

    def test_create_Sample_success_datapoint(self):
        ''' creating a Sample should succeed for a datapoint metric '''
        m = metrics.Datapoint('uri')
        t = TimeUUID()
        value = 1
        s = metrics.Sample(metric=m, t=t, value=value)
        self.assertEqual(s.t,t)
        self.assertEqual(s.metric,m)
        self.assertEqual(s.value,decimal.Decimal(value))


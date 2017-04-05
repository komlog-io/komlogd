import unittest
import decimal
import pandas as pd
import datetime
import uuid
from komlogd.api.protocol.model import types

class ApiProtocolModelTypesTest(unittest.TestCase):

    def test_actions(self):
        ''' Actions available in this protocol version '''
        self.assertEqual(len(types.Actions),8)
        self.assertEqual(types.Actions.SEND_DS_DATA.value,'send_ds_data')
        self.assertEqual(types.Actions.SEND_DP_DATA.value,'send_dp_data')
        self.assertEqual(types.Actions.SEND_MULTI_DATA.value,'send_multi_data')
        self.assertEqual(types.Actions.HOOK_TO_URI.value,'hook_to_uri')
        self.assertEqual(types.Actions.UNHOOK_FROM_URI.value,'unhook_from_uri')
        self.assertEqual(types.Actions.REQUEST_DATA.value,'request_data')
        self.assertEqual(types.Actions.SEND_DATA_INTERVAL.value,'send_data_interval')
        self.assertEqual(types.Actions.GENERIC_RESPONSE.value,'generic_response')

    def test_metrics(self):
        ''' Metrics available in this protocol version '''
        self.assertEqual(len(types.Metrics),2)
        self.assertEqual(types.Metrics.DATASOURCE.value,'d')
        self.assertEqual(types.Metrics.DATAPOINT.value,'p')

    def test_metric_failure_invalid_uri(self):
        ''' creating a Metric with invalid uri must fail '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            metric=types.Metric(uri=uri)
        self.assertEqual(str(cm.exception),'uri is not valid: '+uri)

    def test_metric_success(self):
        ''' creating a Metric object should succeed '''
        uri='valid.uri'
        metric=types.Metric(uri)
        self.assertTrue(isinstance(metric,types.Metric))
        self.assertEqual(metric.uri, uri)
        self.assertEqual(metric.m_type, None)

    def test_metric_uri_cannot_be_modified(self):
        ''' modifying a metric uri is not allowed '''
        uri='valid.uri'
        metric=types.Metric(uri)
        with self.assertRaises(TypeError) as cm:
            metric.uri='new.uri'
        self.assertEqual(str(cm.exception), 'uri cannot be modified')

    def test_metric_mtype_cannot_be_modified(self):
        ''' modifying a metric mtype is not allowed '''
        uri='valid.uri'
        metric=types.Metric(uri)
        with self.assertRaises(TypeError) as cm:
            metric.m_type=types.Metrics.DATASOURCE
        self.assertEqual(str(cm.exception), 'm_type cannot be modified')

    def test_datasource_metric_success(self):
        ''' creating a Datasource object should succeed '''
        uri='valid.uri'
        metric=types.Datasource(uri)
        self.assertTrue(isinstance(metric,types.Datasource))
        self.assertEqual(metric.uri, uri)
        self.assertEqual(metric.m_type, types.Metrics.DATASOURCE)

    def test_datapoint_metric_success(self):
        ''' creating a Datapoint object should succeed '''
        uri='valid.uri'
        metric=types.Datapoint(uri)
        self.assertTrue(isinstance(metric,types.Datapoint))
        self.assertEqual(metric.uri, uri)
        self.assertEqual(metric.m_type, types.Metrics.DATAPOINT)

    def test_comparing_two_metrics_is_done_by_uri(self):
        ''' when comparing two metrics, we check the uri field '''
        ds1=types.Metric('metrics.ds')
        ds2=types.Datasource('metrics.ds')
        self.assertEqual(ds1,ds2)

    def test_deleting_a_metric_from_an_array_is_done_checking_the_uri_field(self):
        ''' when comparing two metrics, we check the uri field '''
        metrics=[]
        metrics.append(types.Metric('metrics.uri1'))
        metrics.append(types.Metric('metrics.uri2'))
        metrics.append(types.Metric('metrics.uri3'))
        metrics.append(types.Metric('metrics.uri4'))
        metrics.append(types.Metric('metrics.uri5'))
        metrics.append(types.Metric('metrics.uri6'))
        self.assertEqual(len(metrics),6)
        existing_metric=types.Datapoint('metrics.uri1')
        non_existing_metric=types.Datapoint('metrics.uri10')
        self.assertTrue(existing_metric in metrics)
        self.assertFalse(non_existing_metric in metrics)
        metrics.remove(existing_metric)
        self.assertFalse(existing_metric in metrics)
        self.assertEqual(len(metrics),5)

    def test_metrics_are_hashable_objects(self):
        ''' a Metric is a hashable object, so you can use it as a key in dictionaries '''
        metrics={}
        for uri in range(1,10):
            metric=types.Metric(uri=str(uri))
            metrics[metric]=uri
        for uri in range(1,10):
            metric=types.Metric(uri=str(uri))
            self.assertEqual(metrics[metric],uri)


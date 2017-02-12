import unittest
import decimal
import pandas as pd
import datetime
import uuid
from komlogd.api.model import types, orm

class ApiModelOrmTest(unittest.TestCase):

    def test_metric_failure_invalid_uri(self):
        ''' creating a Metric with invalid uri must fail '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            metric=orm.Metric(uri=uri)
        self.assertEqual(str(cm.exception),'uri is not valid: '+uri)

    def test_metric_success(self):
        ''' creating a Metric object should succeed '''
        uri='valid.uri'
        metric=orm.Metric(uri)
        self.assertTrue(isinstance(metric, orm.Metric))
        self.assertEqual(metric.uri, uri)
        self.assertEqual(metric.m_type, None)

    def test_metric_uri_cannot_be_modified(self):
        ''' modifying a metric uri is not allowed '''
        uri='valid.uri'
        metric=orm.Metric(uri)
        with self.assertRaises(TypeError) as cm:
            metric.uri='new.uri'
        self.assertEqual(str(cm.exception), 'uri cannot be modified')

    def test_metric_mtype_cannot_be_modified(self):
        ''' modifying a metric mtype is not allowed '''
        uri='valid.uri'
        metric=orm.Metric(uri)
        with self.assertRaises(TypeError) as cm:
            metric.m_type=types.Metrics.DATASOURCE
        self.assertEqual(str(cm.exception), 'm_type cannot be modified')

    def test_datasource_metric_success(self):
        ''' creating a Datasource object should succeed '''
        uri='valid.uri'
        metric=orm.Datasource(uri)
        self.assertTrue(isinstance(metric, orm.Datasource))
        self.assertEqual(metric.uri, uri)
        self.assertEqual(metric.m_type, types.Metrics.DATASOURCE)

    def test_datapoint_metric_success(self):
        ''' creating a Datapoint object should succeed '''
        uri='valid.uri'
        metric=orm.Datapoint(uri)
        self.assertTrue(isinstance(metric, orm.Datapoint))
        self.assertEqual(metric.uri, uri)
        self.assertEqual(metric.m_type, types.Metrics.DATAPOINT)

    def test_comparing_two_metrics_is_done_by_uri(self):
        ''' when comparing two metrics, we check the uri field '''
        ds1=orm.Metric('metrics.ds')
        ds2=orm.Datasource('metrics.ds')
        self.assertEqual(ds1,ds2)

    def test_deleting_a_metric_from_an_array_is_done_checking_the_uri_field(self):
        ''' when comparing two metrics, we check the uri field '''
        metrics=[]
        metrics.append(orm.Metric('metrics.uri1'))
        metrics.append(orm.Metric('metrics.uri2'))
        metrics.append(orm.Metric('metrics.uri3'))
        metrics.append(orm.Metric('metrics.uri4'))
        metrics.append(orm.Metric('metrics.uri5'))
        metrics.append(orm.Metric('metrics.uri6'))
        self.assertEqual(len(metrics),6)
        existing_metric=orm.Datapoint('metrics.uri1')
        non_existing_metric=orm.Datapoint('metrics.uri10')
        self.assertTrue(existing_metric in metrics)
        self.assertFalse(non_existing_metric in metrics)
        metrics.remove(existing_metric)
        self.assertFalse(existing_metric in metrics)
        self.assertEqual(len(metrics),5)

    def test_metrics_are_hashable_objects(self):
        ''' a Metric is a hashable object, so you can use it as a key in dictionaries '''
        metrics={}
        for uri in range(1,10):
            metric=orm.Metric(uri=str(uri))
            metrics[metric]=uri
        for uri in range(1,10):
            metric=orm.Metric(uri=str(uri))
            self.assertEqual(metrics[metric],uri)


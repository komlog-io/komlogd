import asyncio
import unittest
import uuid
import time
import pandas as pd
from komlogd.api.common import exceptions
from komlogd.api.model import schedules, metrics

class ApiModelSchedulesTest(unittest.TestCase):

    def test_Schedule_creation_failure(self):
        ''' creating a Schedule object is disallowed '''
        with self.assertRaises(TypeError) as cm:
            m = schedules.Schedule(uri='uri',session=None)
        self.assertEqual(str(cm.exception), "Schedule base class may not be instantiated")

    def test_creation_DummySchedule_success_with_defaults(self):
        ''' creating a DummySchedule object should succeed. exec_on_load should be False by default '''
        sc = schedules.DummySchedule()
        self.assertEqual(sc.exec_on_load, False)
        self.assertEqual(sc.activation_metrics, [])
        self.assertEqual(sc.meets(), False)

    def test_creation_DummySchedule_success_exec_on_load_true(self):
        ''' creating a DummySchedule object should succeed  '''
        sc = schedules.DummySchedule(exec_on_load = True)
        self.assertEqual(sc.exec_on_load, True)
        self.assertEqual(sc.activation_metrics, [])
        self.assertEqual(sc.meets(), False)

    def test_DummySchedule_activation_metrics_modification_not_allowed(self):
        ''' DummySchedule's activation_metrics parameter cannot be modified '''
        sc = schedules.DummySchedule()
        self.assertEqual(sc.exec_on_load, False)
        self.assertEqual(sc.activation_metrics, [])
        with self.assertRaises(AttributeError) as cm:
            sc.activation_metrics = ['a']
        self.assertEqual(str(cm.exception), "can't set attribute")

    def test_creation_OnUpdateSchedule_success_with_defaults(self):
        ''' creating a OnUpdateSchedule object should succeed. exec_on_load should be False by default '''
        sc = schedules.OnUpdateSchedule()
        self.assertEqual(sc.exec_on_load, False)
        self.assertEqual(sc.activation_metrics, [])
        self.assertEqual(sc.meets(), False)

    def test_creation_OnUpdateSchedule_success_with_exec_on_load_True(self):
        ''' creating a OnUpdateSchedule object should succeed with exec_on_load True'''
        sc = schedules.OnUpdateSchedule(exec_on_load=True)
        self.assertEqual(sc.exec_on_load, True)
        self.assertEqual(sc.activation_metrics, [])
        self.assertEqual(sc.meets(), False)

    def test_creation_OnUpdateSchedule_success_with_activation_metrics_some_object(self):
        ''' creating a OnUpdateSchedule object should succeed and find Metrics if activation_metrics != None '''
        class MyObj:
            def __init__(self):
                self.metrics = [metrics.Datasource('uri_ds'),metrics.Datapoint('uri_dp')]
                self.ds_anom = metrics.Anomaly(metrics.Datasource('uri_ds'))
                self.dp_tag = metrics.Tag(metrics.Datapoint('uri_dp'),key='key',value='value')
        my_obj = MyObj()
        sc = schedules.OnUpdateSchedule(activation_metrics=my_obj)
        self.assertEqual(sc.exec_on_load, False)
        expected_metrics = [metrics.Datasource('uri_ds'),metrics.Datapoint('uri_dp'),metrics.Anomaly(metrics.Datasource('uri_ds')), metrics.Tag(metrics.Datapoint('uri_dp'),key='key',value='value')]
        self.assertEqual(sorted(sc.activation_metrics, key= lambda x: x.__hash__()), sorted(expected_metrics, key=lambda x: x.__hash__()))
        self.assertEqual(sc.meets(), False)

    def test_creation_OnUpdateSchedule_success_with_activation_metrics_a_dict_with_metrics(self):
        ''' creating a OnUpdateSchedule object should succeed and find Metrics if activation_metrics != None '''
        activation_metrics = {
            '0':metrics.Datasource('uri_1'),
            '1':metrics.Datapoint('uri_2'),
            '2':metrics.Anomaly(metrics.Datasource('uri_3')),
            '3':metrics.Tag(metrics.Datasource('uri_4'), key='key',value='value')
        }
        sc = schedules.OnUpdateSchedule(activation_metrics=activation_metrics)
        self.assertEqual(sc.exec_on_load, False)
        expected_metrics = [metrics.Datasource('uri_1'),metrics.Datapoint('uri_2'),metrics.Anomaly(metrics.Datasource('uri_3')), metrics.Tag(metrics.Datasource('uri_4'),key='key',value='value')]
        self.assertEqual(sorted(sc.activation_metrics, key= lambda x: x.__hash__()), sorted(expected_metrics, key=lambda x: x.__hash__()))
        self.assertEqual(sc.meets(), False)

    def test_creation_CronSchedule_success_with_defaults(self):
        ''' creating a CronSchedule object should succeed. exec_on_load should be False by default '''
        sc = schedules.CronSchedule()
        self.assertEqual(sc.exec_on_load, False)
        self.assertEqual(sc.activation_metrics, [])
        self.assertEqual(sc.minute, '*')
        self.assertEqual(sc.hour, '*')
        self.assertEqual(sc.month, '*')
        self.assertEqual(sc.dow, '*')
        self.assertEqual(sc.dom, '*')

    def test_creation_CronSchedule_failure_invalid_minute(self):
        ''' creating a CronSchedule object should fail if minute is invalid '''
        minutes = [-1, 60, 9000, {'set'}, ['a','list'], {'a':'dict'}]
        for minute in minutes:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                sc = schedules.CronSchedule(minute=minute)
            self.assertEqual(str(cm.exception), 'Invalid minute value: '+str(minute))

    def test_creation_CronSchedule_failure_invalid_hour(self):
        ''' creating a CronSchedule object should fail if hour is invalid '''
        hours = [-1, 24, 9000, {'set'}, ['a','list'], {'a':'dict'}]
        for hour in hours:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                sc = schedules.CronSchedule(hour=hour)
            self.assertEqual(str(cm.exception), 'Invalid hour value: '+str(hour))

    def test_creation_CronSchedule_failure_invalid_month(self):
        ''' creating a CronSchedule object should fail if month is invalid '''
        months = [0, 13, 9000, {'set'}, ['a','list'], {'a':'dict'}]
        for month in months:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                sc = schedules.CronSchedule(month=month)
            self.assertEqual(str(cm.exception), 'Invalid month value: '+str(month))

    def test_creation_CronSchedule_failure_invalid_dow(self):
        ''' creating a CronSchedule object should fail if dow is invalid '''
        dows = [-1, 7, 9000, {'set'}, ['a','list'], {'a':'dict'}]
        for dow in dows:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                sc = schedules.CronSchedule(dow=dow)
            self.assertEqual(str(cm.exception), 'Invalid dow value: '+str(dow))

    def test_creation_CronSchedule_failure_invalid_dom(self):
        ''' creating a CronSchedule object should fail if dom is invalid '''
        doms = [0, 32, 9000, {'set'}, ['a','list'], {'a':'dict'}]
        for dom in doms:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                sc = schedules.CronSchedule(dom=dom)
            self.assertEqual(str(cm.exception), 'Invalid dom value: '+str(dom))

    def test_process_var_failure_invalid_value(self):
        ''' process_var should fail if parameter is not a string '''
        values = [0, 3.2, {'set'}, ['a','list'], {'a':'dict'}, uuid.uuid4()]
        sc = schedules.CronSchedule()
        for value in values:
            with self.assertRaises(TypeError) as cm:
                sc._process_var(value, max_value=100, min_value=10)
            self.assertEqual(str(cm.exception), 'value not a string')

    def test_process_var_success_asterisk_char(self):
        ''' process_var should succeed if parameter is asterisk, returning a list of comma-separated integers between max_value and min_value '''
        sc = schedules.CronSchedule()
        processed = sc._process_var('*', 10, 1)
        self.assertEqual(sorted(processed), list(range(1,11)))

    def test_process_var_success_comma_separated_items(self):
        ''' process_var should succeed if parameter is a comma separated string with different items '''
        sc = schedules.CronSchedule()
        processed = sc._process_var('1,2,3,4,5', 10, 1)
        self.assertEqual(sorted(processed), [1,2,3,4,5])

    def test_process_var_success_asterisk_slash(self):
        ''' process_var should succeed if parameter is */n type '''
        sc = schedules.CronSchedule()
        processed = sc._process_var('*/2', 10, 1)
        self.assertEqual(sorted(processed), [2,4,6,8,10])

    def test_process_var_success_combining(self):
        ''' process_var should succeed if parameter is a combination of all above'''
        sc = schedules.CronSchedule()
        processed = sc._process_var('1,2,4-6,*/3,*/7', 20, 1)
        self.assertEqual(sorted(processed), [1,2,3,4,5,6,7,9,12,14,15,18])

    def test_CronSchedule_meets_false(self):
        ''' CronSchedule meets should return false if timestruct does not meet schedule '''
        sc = schedules.CronSchedule()
        sc._schedule={'minute':[],'hour':[],'month':[],'dow':[],'dom':[]}
        t = time.localtime()
        self.assertFalse(sc.meets(t))

    def test_CronSchedule_meets_true(self):
        ''' CronSchedule meets should return true if timestruct meets schedule '''
        sc = schedules.CronSchedule()
        t = time.localtime()
        sc._schedule={'minute':[t.tm_min],'hour':[t.tm_hour],'month':[t.tm_mon],'dow':[t.tm_wday],'dom':[t.tm_mday]}
        self.assertTrue(sc.meets(t))


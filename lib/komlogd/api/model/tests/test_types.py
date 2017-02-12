import unittest
import decimal
import pandas as pd
import datetime
import uuid
from komlogd.api.model import types

class ApiModelTypesTest(unittest.TestCase):

    def test_actions(self):
        ''' Actions available in this protocol version '''
        self.assertEqual(len(types.Actions),7)
        self.assertEqual(types.Actions.SEND_DS_DATA.value,'send_ds_data')
        self.assertEqual(types.Actions.SEND_DP_DATA.value,'send_dp_data')
        self.assertEqual(types.Actions.SEND_MULTI_DATA.value,'send_multi_data')
        self.assertEqual(types.Actions.HOOK_TO_URI.value,'hook_to_uri')
        self.assertEqual(types.Actions.UNHOOK_FROM_URI.value,'unhook_from_uri')
        self.assertEqual(types.Actions.REQUEST_DATA.value,'request_data')
        self.assertEqual(types.Actions.SEND_DATA_INTERVAL.value,'send_data_interval')

    def test_metrics(self):
        ''' Metrics available in this protocol version '''
        self.assertEqual(len(types.Metrics),2)
        self.assertEqual(types.Metrics.DATASOURCE.value,'d')
        self.assertEqual(types.Metrics.DATAPOINT.value,'p')


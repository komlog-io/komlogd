import unittest
import uuid
import asyncio
import pandas as pd
from komlogd.api import transfer_methods, exceptions
from komlogd.api.protocol.model.types import Metric

class ApiTransferMethodsTest(unittest.TestCase):

    def test_transfermethod_failure_invalid_uris_parameter_type(self):
        ''' creation of a transfermethod object should fail if uris parameter type is not a list '''
        uris=[332,1.1,{'a':'dict'},'string',('tu','ple')]
        for uri in uris:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                transfer_methods.transfermethod(uris=uri)
            self.assertEqual(cm.exception.msg, 'Invalid uris parameter type')

    def test_transfermethod_failure_invalid_uri(self):
        ''' creation of a transfermethod object should fail if any uri is invalid '''
        uris=['valid','valid','valid','invalid uri']
        with self.assertRaises(exceptions.BadParametersException) as cm:
            transfer_methods.transfermethod(uris=uris)
        self.assertEqual(str(cm.exception), 'Invalid uri')

    def test_transfermethod_failure_invalid_min_exec_delta(self):
        ''' creation of a transfermethod object should fail if min_exec_delta parameter is invalid '''
        uri='valid.uri'
        min_exec_delta='once upon a time'
        with self.assertRaises(exceptions.BadParametersException) as cm:
            tm=transfer_methods.transfermethod(uris=[uri], min_exec_delta=min_exec_delta)
        self.assertEqual(cm.exception.msg, 'Invalid min_exec_delta value')

    def test_transfermethod_success_unique_uri(self):
        ''' creation of a transfermethod object should succeed if uri is valid '''
        uri='valid.uri'
        min_exec_delta = '2h'
        tm=transfer_methods.transfermethod(uris=[uri], min_exec_delta=min_exec_delta)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, pd.Timedelta(min_exec_delta))
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [Metric(uri=uri)])
        self.assertEqual(tm.uris, [uri])
        self.assertEqual(tm.exec_on_load, False)

    def test_transfermethod_success_list_uri(self):
        ''' creation of a transfermethod object should succeed if a list of valid uris is passed '''
        uris=['valid.uri1','valid.uri2','valid.uri3']
        tm=transfer_methods.transfermethod(uris=uris)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [Metric(uri=uri) for uri in uris])
        self.assertEqual(tm.uris, uris)
        self.assertEqual(tm.exec_on_load, False)

    def test_transfermethod_success_registering_transfermethod(self):
        '''transfermethod object should be able to register the associated method successfully '''
        uris=['valid.uri1','valid.uri2','valid.uri3']
        tm=transfer_methods.transfermethod(uris=uris)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [Metric(uri=uri) for uri in uris])
        self.assertEqual(tm.uris, uris)
        self.assertEqual(tm.exec_on_load, False)
        def func():
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(tm.funcargs,{})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_transfermethod_success_exec_on_load_true(self):
        ''' creation of a transfermethod object should succeed and set exec_on_load to true '''
        uri='valid.uri'
        tm=transfer_methods.transfermethod(uris=[uri], exec_on_load=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [Metric(uri=uri)])
        self.assertEqual(tm.uris, [uri])
        self.assertEqual(tm.exec_on_load, True)


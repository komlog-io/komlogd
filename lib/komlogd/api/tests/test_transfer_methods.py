import unittest
import uuid
import asyncio
from komlogd.api import transfer_methods
from komlogd.api.model import exceptions, orm

class ApiTransferMethodsTest(unittest.TestCase):

    def test_transfermethod_failure_invalid_uri(self):
        ''' creation of a transfermethod object should fail if uri is invalid '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            transfer_methods.transfermethod(uris=[uri])
        self.assertEqual(str(cm.exception), 'uri is not valid: '+uri)

    def test_transfermethod_failure_invalid_uris(self):
        ''' creation of a transfermethod object should fail if uris param is not a list '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            transfer_methods.transfermethod(uris=uri)
        self.assertEqual(str(cm.exception), 'uris parameter must be a list')

    def test_transfermethod_success_unique_uri(self):
        ''' creation of a transfermethod object should succeed if uri is valid '''
        uri='valid.uri'
        tm=transfer_methods.transfermethod(uris=[uri])
        self.assertTrue(isinstance(tm.lid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [orm.Metric(uri=uri)])
        self.assertEqual(tm.uris, [uri])

    def test_transfermethod_success_list_uri(self):
        ''' creation of a transfermethod object should succeed if a list of valid uris is passed '''
        uris=['valid.uri1','valid.uri2','valid.uri3']
        tm=transfer_methods.transfermethod(uris=uris)
        self.assertTrue(isinstance(tm.lid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [orm.Metric(uri=uri) for uri in uris])
        self.assertEqual(tm.uris, uris)

    def test_transfermethod_success_registering_transfermethod(self):
        '''transfermethod object should be able to register the associated method successfully '''
        uris=['valid.uri1','valid.uri2','valid.uri3']
        tm=transfer_methods.transfermethod(uris=uris)
        self.assertTrue(isinstance(tm.lid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.min_exec_delta, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.metrics, [orm.Metric(uri=uri) for uri in uris])
        self.assertEqual(tm.uris, uris)
        def func():
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(tm.funcargs,{})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))


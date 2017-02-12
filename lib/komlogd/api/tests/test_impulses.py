import unittest
import uuid
import asyncio
from komlogd.api import impulses
from komlogd.api.model import exceptions, orm

class ApiImpulsesTest(unittest.TestCase):

    def test_impulsemethod_failure_invalid_uri(self):
        ''' creation of a impulsemethod object should fail if uri is invalid '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            impulses.impulsemethod(uris=[uri])
        self.assertEqual(str(cm.exception), 'uri is not valid: '+uri)

    def test_impulsemethod_failure_invalid_uris(self):
        ''' creation of a impulsemethod object should fail if uris param is not a list '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            impulses.impulsemethod(uris=uri)
        self.assertEqual(str(cm.exception), 'uris parameter must be a list')

    def test_impulsemethod_success_unique_uri(self):
        ''' creation of a impulsemethod object should succeed if uri is valid '''
        uri='valid.uri'
        lm=impulses.impulsemethod(uris=[uri])
        self.assertTrue(isinstance(lm.lid,uuid.UUID))
        self.assertEqual(lm.last_exec, None)
        self.assertEqual(lm.min_exec_delta, None)
        self.assertEqual(lm.data_reqs, None)
        self.assertEqual(lm.metrics, [orm.Metric(uri=uri)])
        self.assertEqual(lm.uris, [uri])

    def test_impulsemethod_success_list_uri(self):
        ''' creation of a impulsemethod object should succeed if a list of valid uris is passed '''
        uris=['valid.uri1','valid.uri2','valid.uri3']
        lm=impulses.impulsemethod(uris=uris)
        self.assertTrue(isinstance(lm.lid,uuid.UUID))
        self.assertEqual(lm.last_exec, None)
        self.assertEqual(lm.min_exec_delta, None)
        self.assertEqual(lm.data_reqs, None)
        self.assertEqual(lm.metrics, [orm.Metric(uri=uri) for uri in uris])
        self.assertEqual(lm.uris, uris)

    def test_impulsemethod_success_registering_labmdamethod(self):
        '''impulsemethod object should be able to register the associated method successfully '''
        uris=['valid.uri1','valid.uri2','valid.uri3']
        lm=impulses.impulsemethod(uris=uris)
        self.assertTrue(isinstance(lm.lid,uuid.UUID))
        self.assertEqual(lm.last_exec, None)
        self.assertEqual(lm.min_exec_delta, None)
        self.assertEqual(lm.data_reqs, None)
        self.assertEqual(lm.metrics, [orm.Metric(uri=uri) for uri in uris])
        self.assertEqual(lm.uris, uris)
        def func():
            pass
        f=lm(func)
        self.assertEqual(f,func)
        self.assertEqual(lm.funcargs,{})
        self.assertIsNotNone(getattr(lm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(lm.f))


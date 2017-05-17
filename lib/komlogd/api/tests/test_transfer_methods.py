import unittest
import uuid
import asyncio
import pandas as pd
from komlogd.api import transfer_methods, exceptions
from komlogd.api.model.store import MetricsStore
from komlogd.api.protocol.model.types import Metric, Datasource, Datapoint, Sample
from komlogd.api.protocol.model.schedules import OnUpdateSchedule
from komlogd.api.protocol.model.transfer_methods import DataRequirements

class ApiTransferMethodsTest(unittest.TestCase):

    def test_transfermethod_failure_invalid_p_in(self):
        ''' creation of a transfermethod object should fail if p_in is invalid '''
        p_ins = [1,'str',{'set'},['list'],('tupl','e'),Metric(uri='uri'),pd.Timestamp('now')]
        for p_in in p_ins:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                tm=transfer_methods.transfermethod(p_in=p_in)
            self.assertEqual(cm.exception.msg, '"p_in" attribute must be a dict')

    def test_transfermethod_failure_reserved_p_in_key(self):
        ''' creation of a transfermethod object should fail if p_in has a reserved key '''
        p_ins=[{'ts':1},{'updated':1},{'others':1}]
        for p_in in p_ins:
            key = list(p_in.keys())[0]
            with self.assertRaises(exceptions.BadParametersException) as cm:
                tm=transfer_methods.transfermethod(p_in=p_in)
            self.assertEqual(cm.exception.msg, 'Invalid input parameter. "{}" is a reserved parameter'.format(key))

    def test_transfermethod_failure_invalid_p_out(self):
        ''' creation of a transfermethod object should fail if p_out is invalid '''
        p_outs = [1,'str',{'set'},['list'],('tupl','e'),Metric(uri='uri'),pd.Timestamp('now')]
        for p_out in p_outs:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                tm=transfer_methods.transfermethod(p_out=p_out)
            self.assertEqual(cm.exception.msg, '"p_out" attribute must be a dict')

    def test_transfermethod_failure_reserved_p_out_key(self):
        ''' creation of a transfermethod object should fail if p_out has a reserved key '''
        p_outs=[{'ts':1},{'updated':1},{'others':1}]
        for p_out in p_outs:
            key = list(p_out.keys())[0]
            with self.assertRaises(exceptions.BadParametersException) as cm:
                tm=transfer_methods.transfermethod(p_out=p_out)
            self.assertEqual(cm.exception.msg, 'Invalid output parameter. "{}" is a reserved parameter'.format(key))

    def test_transfermethod_failure_invalid_schedule(self):
        ''' creation of a transfermethod object should fail if shedule parameter is invalid '''
        schedule='once upon a time'
        with self.assertRaises(exceptions.BadParametersException) as cm:
            tm=transfer_methods.transfermethod(schedule=schedule)
        self.assertEqual(cm.exception.msg, 'Invalid "schedule" attribute')

    def test_transfermethod_failure_invalid_data_reqs(self):
        ''' creation of a transfermethod object should fail if data_reqs parameter is invalid '''
        data_reqs='give me all'
        with self.assertRaises(exceptions.BadParametersException) as cm:
            tm=transfer_methods.transfermethod(data_reqs=data_reqs)
        self.assertEqual(cm.exception.msg, 'Invalid data_reqs parameter')

    def test_transfermethod_success_no_metrics_in_params(self):
        ''' creation of a transfermethod object should succeed if uri is valid '''
        p_in={'arg1':'valid.uri','arg2':'another.uri'}
        p_out={'arg3':'valid.uri','arg4':'another.uri'}
        data_reqs = DataRequirements(past_delta=pd.Timedelta('6h'))
        tm=transfer_methods.transfermethod(p_in=p_in, p_out=p_out, data_reqs=data_reqs, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.schedule, None)
        self.assertEqual(tm.data_reqs, data_reqs)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.allow_loops, True)

    def test_transfermethod_success_registering_transfermethod_no_metrics_in_params(self):
        '''transfermethod object should be able to register the associated method successfully '''
        p_in={'arg1':'valid.uri','arg2':'another.uri'}
        p_out={'arg3':'valid.uri','arg4':'another.uri'}
        data_reqs = DataRequirements(past_delta=pd.Timedelta('6h'))
        tm=transfer_methods.transfermethod(p_in=p_in, p_out=p_out, data_reqs=data_reqs, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.schedule, None)
        self.assertEqual(tm.data_reqs, data_reqs)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.allow_loops, True)
        def func():
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(tm._func_params,{})
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, OnUpdateSchedule))
        self.assertEqual(tm.schedule.metrics, [])
        self.assertEqual(tm.schedule.exec_on_load, False)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_transfermethod_success_registering_transfermethod_with_input_params(self):
        '''transfermethod object should be able to register the associated method and find input params '''
        p_in={'arg1':Datapoint('valid.datapoint'),'arg2':Datasource('valid.datasource')}
        data_reqs = DataRequirements(past_delta=pd.Timedelta('6h'))
        tm=transfer_methods.transfermethod(p_in=p_in, data_reqs=data_reqs, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, data_reqs)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, {})
        self.assertEqual(tm.allow_loops, True)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_in_routes,{'arg1':[[('s',0)]],'arg2':[[('s',0)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datapoint('valid.datapoint'),Datasource('valid.datasource')})
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, OnUpdateSchedule))
        self.assertEqual(sorted(tm.schedule.metrics, key=lambda x:x.uri), sorted(list(p_in.values()),key=lambda x:x.uri))
        self.assertEqual(tm.schedule.exec_on_load, False)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_transfermethod_success_registering_transfermethod_onupdateschedule_different_input_metric(self):
        '''transfermethod object should be able to register the associated method and find input params '''
        p_in={'arg1':Datapoint('valid.datapoint'),'arg2':Datasource('valid.datasource')}
        data_reqs = DataRequirements(past_delta=pd.Timedelta('6h'))
        schedule=OnUpdateSchedule(metrics=[Datapoint('valid.datapoint2')], exec_on_load=True)
        tm=transfer_methods.transfermethod(p_in=p_in, data_reqs=data_reqs, schedule=schedule, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, data_reqs)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, {})
        self.assertEqual(tm.allow_loops, True)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_in_routes,{'arg1':[[('s',0)]],'arg2':[[('s',0)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datapoint('valid.datapoint'),Datasource('valid.datasource')})
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, OnUpdateSchedule))
        self.assertEqual(tm.schedule.metrics, schedule.metrics)
        self.assertEqual(tm.schedule.exec_on_load, True)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_transfermethod_success_registering_transfermethod_with_output_params(self):
        '''transfermethod object should be able to register the associated method and find output params '''
        p_out={'arg1':Datapoint('valid.datapoint'),'arg2':Datasource('valid.datasource')}
        data_reqs = DataRequirements(past_delta=pd.Timedelta('6h'))
        tm=transfer_methods.transfermethod(p_out=p_out, data_reqs=data_reqs, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, data_reqs)
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.p_in, {})
        self.assertEqual(tm.allow_loops, True)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_out_routes,{'arg1':[[('s',0)]],'arg2':[[('s',0)]]})
        self.assertEqual(tm._p_in_routes,{})
        self.assertEqual(tm._m_in, set())
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, OnUpdateSchedule))
        self.assertEqual(tm.schedule.metrics, [])
        self.assertEqual(tm.schedule.exec_on_load, False)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_inspect_input_params_success_list(self):
        '''transfermethod object should be able to identify params in a list '''
        p_in={'arg1':[Datapoint('valid.datapoint'), Datasource('valid.datasource')]}
        tm=transfer_methods.transfermethod(p_in=p_in)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_out, {})
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.allow_loops, False)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_in_routes,{'arg1':[[('i',0)],[('i',1)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datapoint('valid.datapoint'),Datasource('valid.datasource')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_inspect_input_params_success_dict(self):
        '''transfermethod object should be able to identify params in a dict '''
        p_in={'arg1':{'dp':Datapoint('valid.datapoint'), 'ds':Datasource('valid.datasource')}}
        tm=transfer_methods.transfermethod(p_in=p_in)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_out, {})
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.allow_loops, False)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_in_routes,{'arg1':[[('k','dp')],[('k','ds')]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datapoint('valid.datapoint'),Datasource('valid.datasource')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_inspect_input_params_success_class(self):
        '''transfermethod object should be able to identify params in a user defined class instance '''
        class MyObj:
            def __init__(self, uri):
                self.uri = uri
                self.ds = Datasource('.'.join((uri,'ds')))
                self.dp = Datapoint('.'.join((uri,'dp')))
        p_in={'arg1':MyObj('valid')}
        tm=transfer_methods.transfermethod(p_in=p_in)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_out, {})
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.allow_loops, False)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_in_routes,{'arg1':[[('a','__dict__'),('k','ds')],[('a','__dict__'),('k','dp')]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datasource('valid.ds'),Datapoint('valid.dp')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_inspect_input_params_success_combination(self):
        '''transfermethod object should be able to identify params in a user defined class instance '''
        class MyObj:
            def __init__(self, uri):
                self.uri = uri
                self.elements={'metrics':[Datasource('.'.join((uri,'ds'))), Datapoint('.'.join((uri,'dp')))]}
        p_in={'arg1':MyObj('valid')}
        tm=transfer_methods.transfermethod(p_in=p_in)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_out, {})
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.allow_loops, False)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_in_routes,{'arg1':[[('a','__dict__'),('k','elements'),('k','metrics'),('i',0)],[('a','__dict__'),('k','elements'),('k','metrics'),('i',1)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datasource('valid.ds'),Datapoint('valid.dp')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_inspect_output_params_success_combination(self):
        '''transfermethod object should be able to identify params in a user defined class instance '''
        class MyObj:
            def __init__(self, uri):
                self.uri = uri
                self.elements={'metrics':[Datasource('.'.join((uri,'ds'))), Datapoint('.'.join((uri,'dp')))]}
        p_out={'arg1':MyObj('valid')}
        tm=transfer_methods.transfermethod(p_out=p_out)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_in, {})
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.allow_loops, False)
        def func(arg1, arg2):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['arg1','arg2'])
        self.assertEqual(tm._p_out_routes,{'arg1':[[('a','__dict__'),('k','elements'),('k','metrics'),('i',0)],[('a','__dict__'),('k','elements'),('k','metrics'),('i',1)]]})
        self.assertEqual(tm._p_in_routes,{})
        self.assertEqual(tm._m_in, set())
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))

    def test_process_exec_result_allow_loops(self):
        '''transfermethod _process_exec_result should allow modifying input parameters if loops are allowed '''
        class MySession:
            def __init__(self):
                self._metrics_store = MetricsStore(owner='user')
            async def send_samples(self, samples):
                self.samples = samples
        p_in = {'ds':Datasource('valid.ds')}
        p_out={'ds':None} #if arg is on p_in, no matter what we put here, only checks that the key is on p_in too
        tm=transfer_methods.transfermethod(p_in=p_in, p_out=p_out, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.allow_loops, True)
        def func(ds):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['ds'])
        self.assertEqual(tm._p_in_routes,{'ds':[[('s',0)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datasource('valid.ds')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        my_session = MySession()
        ds = Datasource('valid.ds')
        ds.data = pd.Series('ds content', index=[pd.Timestamp('now',tz='utc')])
        params = {'ds':ds}
        expected_sample = Sample(metric=ds, ts=ds.data.index[0], data=ds.data[0])
        loop=asyncio.get_event_loop()
        loop.run_until_complete(tm._process_exec_result(session=my_session, params=params))
        self.assertEqual(len(my_session.samples),1)
        self.assertEqual(my_session.samples[0].metric, ds)
        self.assertEqual(my_session.samples[0].ts, expected_sample.ts)
        self.assertEqual(my_session.samples[0].data, expected_sample.data)

    def test_process_exec_result_do_not_allow_loops(self):
        '''transfermethod _process_exec_result should ignore modifying input parameters if loops aren't allowed '''
        class MySession:
            def __init__(self):
                self._metrics_store = MetricsStore(owner='user')
            async def send_samples(self, samples):
                self.samples = samples
        p_in = {'ds':Datasource('valid.ds')}
        p_out={'ds':None} #if arg is on p_in, no matter what we put here, only checks that the key is on p_in too
        tm=transfer_methods.transfermethod(p_in=p_in, p_out=p_out)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.allow_loops, False)
        def func(ds):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['ds'])
        self.assertEqual(tm._p_in_routes,{'ds':[[('s',0)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datasource('valid.ds')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        my_session = MySession()
        ds = Datasource('valid.ds')
        ds.data = pd.Series('ds content', index=[pd.Timestamp('now',tz='utc')])
        params = {'ds':ds}
        loop=asyncio.get_event_loop()
        loop.run_until_complete(tm._process_exec_result(session=my_session, params=params))
        self.assertEqual(len(my_session.samples),0)


    def test_process_exec_result_allow_loops_but_existing_in_store(self):
        '''transfermethod _process_exec_result should not send already existing samples in store '''
        class MySession:
            def __init__(self):
                self._metrics_store = MetricsStore(owner='user')
            async def send_samples(self, samples):
                self.samples = samples
        p_in = {'ds':Datasource('valid.ds')}
        p_out={'ds':None} #if arg is on p_in, no matter what we put here, only checks that the key is on p_in too
        tm=transfer_methods.transfermethod(p_in=p_in, p_out=p_out, allow_loops=True)
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.last_exec, None)
        self.assertEqual(tm.data_reqs, None)
        self.assertEqual(tm.p_in, p_in)
        self.assertEqual(tm.p_out, p_out)
        self.assertEqual(tm.allow_loops, True)
        def func(ds):
            pass
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(list(tm._func_params.keys()),['ds'])
        self.assertEqual(tm._p_in_routes,{'ds':[[('s',0)]]})
        self.assertEqual(tm._p_out_routes,{})
        self.assertEqual(tm._m_in, {Datasource('valid.ds')})
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        my_session = MySession()
        ds = Datasource('valid.ds')
        ds.data = pd.Series('ds content', index=[pd.Timestamp('now',tz='utc')])
        params = {'ds':ds}
        my_session._metrics_store.store(metric=ds, ts=ds.data.index[0], content=ds.data[0])
        loop=asyncio.get_event_loop()
        loop.run_until_complete(tm._process_exec_result(session=my_session, params=params))
        self.assertEqual(len(my_session.samples),0)


import gc
import unittest
import uuid
import asyncio
import pandas as pd
from komlogd.api import transfer_methods
from komlogd.api.common import exceptions
from komlogd.api.model import schedules, test
from komlogd.api.model.store import MetricStore
from komlogd.api.model.metrics import Metric, Datasource, Datapoint, Sample
from komlogd.api.model.transfer_methods import tmIndex

loop = asyncio.get_event_loop()

class ApiTransferMethodsTest(unittest.TestCase):

    def test_transfermethod_failure_invalid_f_params(self):
        ''' creation of a transfermethod object should fail if f_params is invalid '''
        params = [1,'str',{'set'},['list'],('tupl','e'),Datasource(uri='uri'),pd.Timestamp('now')]
        for f_params in params:
            with self.assertRaises(exceptions.BadParametersException) as cm:
                tm=transfer_methods.transfermethod(f_params=f_params)
            self.assertEqual(cm.exception.msg, '"f_params" attribute must be a dict')

    def test_transfermethod_failure_reserved_f_params_key(self):
        ''' creation of a transfermethod object should fail if f_params has a reserved key '''
        params=[{'t':1},{'updated':1},{'others':1}]
        for f_params in params:
            key = list(f_params.keys())[0]
            with self.assertRaises(exceptions.BadParametersException) as cm:
                tm=transfer_methods.transfermethod(f_params=f_params)
            self.assertEqual(cm.exception.msg, 'Invalid function parameter. "{}" is a reserved parameter'.format(key))

    def test_transfermethod_failure_invalid_schedule(self):
        ''' creation of a transfermethod object should fail if shedule parameter is invalid '''
        schedule='once upon a time'
        with self.assertRaises(exceptions.BadParametersException) as cm:
            tm=transfer_methods.transfermethod(schedule=schedule)
        self.assertEqual(cm.exception.msg, 'Invalid "schedule" attribute')

    def test_transfer_method_DummySchedule_success(self):
        ''' creation of a transfermethod object should succeed if schedule is DummySchedule '''
        schedule = schedules.DummySchedule()
        tm=transfer_methods.transfermethod(schedule=schedule)
        self.assertEqual(tm.schedule, schedule)

    def test_transfer_method_OnUpdateSchedule_success(self):
        ''' creation of a transfermethod object should succeed if schedule is OnUpdateSchedule'''
        schedule = schedules.OnUpdateSchedule()
        tm=transfer_methods.transfermethod(schedule=schedule)
        self.assertEqual(tm.schedule, schedule)

    def test_transfer_method_CronSchedule_success(self):
        ''' creation of a transfermethod object should succeed if schedule is CronSchedule'''
        schedule = schedules.CronSchedule()
        tm=transfer_methods.transfermethod(schedule=schedule)
        self.assertEqual(tm.schedule, schedule)

    def test_transfermethod_success_no_params(self):
        ''' creation of a transfermethod object should succeed '''
        tm=transfer_methods.transfermethod()
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm._f, None)
        self.assertEqual(tm.f_params, {})
        self.assertEqual(tm.schedule, None)

    def test_transfermethod_success_registering_decorated_transfermethod_without_schedule(self):
        '''transfermethod object should be able to register a decorated transfer_method without schedule '''
        def func(param):
            pass
        tm=transfer_methods.transfermethod(f_params={'param':'param'})
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm.schedule, None)
        self.assertEqual(tm.f_params, {'param':'param'})
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.OnUpdateSchedule))
        self.assertEqual(tm.schedule.activation_metrics, [])
        self.assertEqual(tm.schedule.exec_on_load, False)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        tm_info = tmIndex.get_tm_info(tm.mid)
        self.assertEqual(tm_info['enabled'], False)
        self.assertEqual(tm_info['tm'], tm)

    def test_transfermethod_success_registering_decorated_transfermethod_CronSchedule(self):
        '''transfermethod object should be able to register a decorated transfer_method with CronSchedule '''
        def func(param):
            pass
        tm=transfer_methods.transfermethod(f_params={'param':'param'}, schedule=schedules.CronSchedule())
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.CronSchedule))
        self.assertEqual(tm.f_params, {'param':'param'})
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.CronSchedule))
        self.assertEqual(tm.schedule.activation_metrics, [])
        self.assertEqual(tm.schedule.exec_on_load, False)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        tm_info = tmIndex.get_tm_info(tm.mid)
        self.assertEqual(tm_info['enabled'], False)
        self.assertEqual(tm_info['tm'], tm)

    def test_transfermethod_success_registering_decorated_transfermethod_DummySchedule(self):
        '''transfermethod object should be able to register a decorated transfer_method with DummySchedule '''
        def func(param):
            pass
        tm=transfer_methods.transfermethod(f_params={'param':'param'}, schedule=schedules.DummySchedule(exec_on_load=True))
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
        self.assertEqual(tm.schedule.exec_on_load, True)
        self.assertEqual(tm.f_params, {'param':'param'})
        f=tm(func)
        self.assertEqual(f,func)
        self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
        self.assertEqual(tm.schedule.activation_metrics, [])
        self.assertEqual(tm.schedule.exec_on_load, True)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        tm_info = tmIndex.get_tm_info(tm.mid)
        self.assertEqual(tm_info['enabled'], False)
        self.assertEqual(tm_info['tm'], tm)

    @test.sync(loop)
    async def test_bind_transfermethod_error_no_associated_function(self):
        ''' calling bind should fail if tm has no associated function '''
        try:
            async def enable_tm(mid):
                return True
            enable_tm_bck = tmIndex.enable_tm
            tmIndex.enable_tm = enable_tm
            def func(param):
                pass
            tm=transfer_methods.transfermethod(f_params={'param':'param'})
            self.assertTrue(isinstance(tm.mid,uuid.UUID))
            self.assertEqual(tm.schedule, None)
            self.assertEqual(tm.f_params, {'param':'param'})
            with self.assertRaises(exceptions.BadParametersException) as cm:
                await tm.bind()
        except:
            raise
        finally:
            tmIndex.enable_tm = enable_tm_bck

    @test.sync(loop)
    async def test_bind_transfermethod_success_without_schedule(self):
        ''' calling bind should succeed with a tm without schedule '''
        try:
            async def enable_tm(mid):
                return True
            enable_tm_bck = tmIndex.enable_tm
            tmIndex.enable_tm = enable_tm
            def func(param):
                pass
            tm=transfer_methods.transfermethod(f=func, f_params={'param':'param'})
            self.assertTrue(isinstance(tm.mid,uuid.UUID))
            self.assertEqual(tm._f, func)
            self.assertEqual(tm.schedule, None)
            self.assertEqual(tm.f_params, {'param':'param'})
            await tm.bind()
            self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.OnUpdateSchedule))
            self.assertEqual(tm.schedule.activation_metrics, [])
            self.assertEqual(tm.schedule.exec_on_load, False)
            self.assertIsNotNone(getattr(tm,'f',None))
            self.assertTrue(asyncio.iscoroutinefunction(tm.f))
            tm_info = tmIndex.get_tm_info(tm.mid)
            self.assertEqual(tm_info['enabled'], False)
            self.assertEqual(tm_info['tm'], tm)
        except:
            raise
        finally:
            tmIndex.enable_tm = enable_tm_bck

    @test.sync(loop)
    async def test_bind_transfermethod_success_with_CronSchedule(self):
        ''' calling bind should succeed with a tm with CronSchedule '''
        try:
            async def enable_tm(mid):
                return True
            enable_tm_bck = tmIndex.enable_tm
            tmIndex.enable_tm = enable_tm
            def func(param):
                pass
            tm=transfer_methods.transfermethod(f=func, f_params={'param':'param'}, schedule=schedules.CronSchedule())
            self.assertTrue(isinstance(tm.mid,uuid.UUID))
            self.assertEqual(tm._f, func)
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.CronSchedule))
            self.assertEqual(tm.f_params, {'param':'param'})
            await tm.bind()
            self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.CronSchedule))
            self.assertEqual(tm.schedule.activation_metrics, [])
            self.assertEqual(tm.schedule.exec_on_load, False)
            self.assertIsNotNone(getattr(tm,'f',None))
            self.assertTrue(asyncio.iscoroutinefunction(tm.f))
            tm_info = tmIndex.get_tm_info(tm.mid)
            self.assertEqual(tm_info['enabled'], False)
            self.assertEqual(tm_info['tm'], tm)
        except:
            raise
        finally:
            tmIndex.enable_tm = enable_tm_bck

    @test.sync(loop)
    async def test_bind_transfermethod_success_with_DummySchedule(self):
        ''' calling bind should succeed with a tm with DummySchedule '''
        try:
            async def enable_tm(mid):
                return True
            def func(param):
                pass
            enable_tm_bck = tmIndex.enable_tm
            tmIndex.enable_tm = enable_tm
            tm=transfer_methods.transfermethod(f=func, f_params={'param':'param'}, schedule=schedules.DummySchedule())
            self.assertTrue(isinstance(tm.mid,uuid.UUID))
            self.assertEqual(tm._f, func)
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
            self.assertEqual(tm.f_params, {'param':'param'})
            await tm.bind()
            self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
            self.assertEqual(tm.schedule.activation_metrics, [])
            self.assertEqual(tm.schedule.exec_on_load, False)
            self.assertIsNotNone(getattr(tm,'f',None))
            self.assertTrue(asyncio.iscoroutinefunction(tm.f))
            tm_info = tmIndex.get_tm_info(tm.mid)
            self.assertEqual(tm_info['enabled'], False)
            self.assertEqual(tm_info['tm'], tm)
        except:
            raise
        finally:
            tmIndex.enable_tm = enable_tm_bck

    def test_unbind_transfermethod_success_not_binded_before(self):
        ''' we should be able to unbind a not previously binded transfermethod '''
        def func(param):
            pass
        tm=transfer_methods.transfermethod(f=func, f_params={'param':'param'})
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm._f, func)
        self.assertEqual(tm.schedule, None)
        self.assertEqual(tm.f_params, {'param':'param'})
        tm_info = tmIndex.get_tm_info(tm.mid)
        self.assertIsNone(tm_info)
        tm.unbind()
        tm_info = tmIndex.get_tm_info(tm.mid)
        self.assertIsNone(tm_info)

    @test.sync(loop)
    async def test_unbind_transfermethod_success_binded_before(self):
        ''' we should be able to unbind a previously binded transfermethod '''
        try:
            async def enable_tm(mid):
                return True
            enable_tm_bck = tmIndex.enable_tm
            tmIndex.enable_tm = enable_tm
            def func(param):
                pass
            tm=transfer_methods.transfermethod(f=func, f_params={'param':'param'})
            self.assertTrue(isinstance(tm.mid,uuid.UUID))
            self.assertEqual(tm._f, func)
            self.assertEqual(tm.schedule, None)
            self.assertEqual(tm.f_params, {'param':'param'})
            await tm.bind()
            self.assertEqual(tm._func_params.keys(), {'param':'param'}.keys())
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.OnUpdateSchedule))
            self.assertEqual(tm.schedule.activation_metrics, [])
            self.assertEqual(tm.schedule.exec_on_load, False)
            self.assertIsNotNone(getattr(tm,'f',None))
            self.assertTrue(asyncio.iscoroutinefunction(tm.f))
            tm_info = tmIndex.get_tm_info(tm.mid)
            self.assertEqual(tm_info['enabled'], False)
            self.assertEqual(tm_info['tm'], tm)
            tm.unbind()
            tm_info = tmIndex.get_tm_info(tm.mid)
            self.assertIsNone(tm_info)
        except:
            raise
        finally:
            tmIndex.enable_tm = enable_tm_bck

    def test_run_transfermethod_failure_not_decorated_or_binded_function_found(self):
        ''' calling run should fail if tm has not associated function '''
        tm=transfer_methods.transfermethod( f_params={'param':5}, schedule=schedules.DummySchedule())
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm._f, None)
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
        self.assertEqual(tm.f_params, {'param':5})
        ts = pd.Timestamp('now',tz='utc')
        metrics = []
        loop.run_until_complete(tm.run(ts,metrics))

    @test.sync(loop)
    async def test_run_transfermethod_success_binded_tm(self):
        ''' calling run should execute the tm associated function '''
        try:
            async def enable_tm(mid):
                return True
            enable_tm_bck = tmIndex.enable_tm
            tmIndex.enable_tm = enable_tm
            var = 0
            def func(param):
                nonlocal var
                var += param
            tm=transfer_methods.transfermethod(f=func, f_params={'param':5}, schedule=schedules.DummySchedule())
            self.assertTrue(isinstance(tm.mid,uuid.UUID))
            self.assertEqual(tm._f, func)
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
            self.assertEqual(tm.f_params, {'param':5})
            await tm.bind()
            self.assertEqual(tm._func_params.keys(), {'param':5}.keys())
            self.assertNotEqual(tm.schedule, None)
            self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
            self.assertEqual(tm.schedule.activation_metrics, [])
            self.assertEqual(tm.schedule.exec_on_load, False)
            self.assertIsNotNone(getattr(tm,'f',None))
            self.assertTrue(asyncio.iscoroutinefunction(tm.f))
            tm_info = tmIndex.get_tm_info(tm.mid)
            self.assertEqual(tm_info['enabled'], False)
            self.assertEqual(tm_info['tm'], tm)
            ts = pd.Timestamp('now',tz='utc')
            metrics = []
            await tm.run(ts,metrics)
            self.assertEqual(var,5)
        except:
            raise
        finally:
            tmIndex.enable_tm = enable_tm_bck

    def test_run_transfermethod_success_decorated_tm(self):
        ''' calling run should execute the tm associated function '''
        var = 0
        def func(param):
            nonlocal var
            var += param
        tm=transfer_methods.transfermethod(f=func, f_params={'param':5}, schedule=schedules.DummySchedule())
        self.assertTrue(isinstance(tm.mid,uuid.UUID))
        self.assertEqual(tm._f, func)
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
        self.assertEqual(tm.f_params, {'param':5})
        tm(func)
        self.assertEqual(tm._func_params.keys(), {'param':5}.keys())
        self.assertNotEqual(tm.schedule, None)
        self.assertTrue(isinstance(tm.schedule, schedules.DummySchedule))
        self.assertEqual(tm.schedule.activation_metrics, [])
        self.assertEqual(tm.schedule.exec_on_load, False)
        self.assertIsNotNone(getattr(tm,'f',None))
        self.assertTrue(asyncio.iscoroutinefunction(tm.f))
        tm_info = tmIndex.get_tm_info(tm.mid)
        self.assertEqual(tm_info['enabled'], False)
        self.assertEqual(tm_info['tm'], tm)
        ts = pd.Timestamp('now',tz='utc')
        metrics = []
        loop.run_until_complete(tm.run(ts,metrics))
        self.assertEqual(var,5)

    def tearDown(self):
        [task.cancel() for task in asyncio.Task.all_tasks()]


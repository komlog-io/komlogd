import asyncio
import decimal
import unittest
import uuid
import time
import komlogd.api.protocol.processing.procedure as prproc
from unittest.mock import call, Mock, patch
from komlogd.api.session import KomlogSession
from komlogd.api.common import exceptions
from komlogd.api.common.timeuuid import TimeUUID, MIN_TIMEUUID, MAX_TIMEUUID
from komlogd.api.model import test
from komlogd.api.model.store import MetricStore
from komlogd.api.model.metrics import Datasource, Datapoint, Sample
from komlogd.api.model.transactions import TransactionTask, Transaction
from komlogd.api.model.session import sessionIndex

loop = asyncio.get_event_loop()

class ApiModelStoreTest(unittest.TestCase):

    def tearDown(self):
        [task.cancel() for task in asyncio.Task.all_tasks()]

    @test.sync(loop)
    async def test_creating_MetricStore_object(self):
        ''' test creating a MetricStore object '''
        ms = MetricStore()
        self.assertEqual(ms._dfs,{})
        self.assertEqual(ms._synced_ranges,{})
        self.assertEqual(ms._tr_dfs,{})
        self.assertEqual(ms._hooked,set())

    @test.sync(loop)
    async def test_sync_success_no_previous_hooked(self):
        ''' sync should try to hook the previously hooked metrics '''
        ms = MetricStore()
        self.assertTrue(await ms.sync())

    @test.sync(loop)
    async def test_sync_success_previous_hooked(self):
        ''' sync should try to hook the previously hooked metrics '''
        ms = MetricStore()
        ms.hook = test.AsyncMock(return_value = {'hooked':True,'exists':True})
        ms._prev_hooked = set()
        ms._prev_hooked.add(Datasource(uri='uri'))
        self.assertTrue(await ms.sync())
        ms.hook.assert_called_with(Datasource(uri='uri'))
        self.assertRaises(AttributeError, getattr, ms, '_prev_hooked')

    @test.sync(loop)
    async def test_sync_fails_no_previous_hook_lost(self):
        ''' if sync fails, assert no previous hooked metric is lost '''
        ms = MetricStore()
        ms.hook = test.AsyncMock(side_effect = [
            {'hooked':True,'exists':False},
            {'hooked':True,'exists':True},
            {'hooked':False,'exists':False}])
        ms._prev_hooked = set()
        ms._prev_hooked.add(Datasource(uri='uri'))
        ms._prev_hooked.add(Datasource(uri='uri2'))
        ms._prev_hooked.add(Datasource(uri='uri3'))
        ms._prev_hooked.add(Datasource(uri='uri4'))
        self.assertFalse(await ms.sync())
        self.assertEqual(ms.hook.call_count, 3)
        self.assertTrue(Datasource(uri='uri') in ms._prev_hooked)
        self.assertTrue(Datasource(uri='uri2') in ms._prev_hooked)
        self.assertTrue(Datasource(uri='uri3') in ms._prev_hooked)
        self.assertTrue(Datasource(uri='uri4') in ms._prev_hooked)

    @test.sync(loop)
    async def test_clear_synced_success_no_synced_ranges(self):
        ''' clear_synced should remove the synced ranges '''
        ms = MetricStore()
        ms.clear_synced()
        self.assertEqual(ms._dfs,{})
        self.assertEqual(ms._synced_ranges,{})
        self.assertEqual(ms._tr_dfs,{})
        self.assertEqual(ms._hooked,set())
        self.assertFalse(hasattr(ms, '_prev_hooked'))

    def test_insert_failure_invalid_metric(self):
        ''' insert should fail if metric is not a valid Metric Object '''
        ms = MetricStore()
        metrics = [None,1, 1.1, 'string', {'a':'dict'},['a','list'],{'set'},('a','tuple'),uuid.uuid4(), TimeUUID()]
        t = TimeUUID()
        value = 'something'
        for metric in metrics:
            with self.assertRaises(TypeError) as cm:
                ms.insert(metric, t, value)
            self.assertEqual(str(cm.exception), 'Invalid metric parameter')

    def test_insert_failure_invalid_t(self):
        ''' insert should fail if t is not a valid TimeUUID Object '''
        ms = MetricStore()
        ts = [None,1, 1.1, 'string', {'a':'dict'},['a','list'],{'set'},('a','tuple'),uuid.uuid4()]
        metric = Datasource('uri')
        value = 'something'
        for t in ts:
            with self.assertRaises(TypeError) as cm:
                ms.insert(metric, t, value)
            self.assertEqual(str(cm.exception), 'value is not a valid TimeUUID: '+str(t))

    def test_insert_failure_invalid_value_datasource(self):
        ''' insert should fail if value is not a valid datasource value '''
        ms = MetricStore()
        values = [None,1, 1.1, {'a':'dict'},['a','list'],{'set'},('a','tuple'),uuid.uuid4(), TimeUUID()]
        metric = Datasource('uri')
        t = TimeUUID()
        for value in values:
            with self.assertRaises(TypeError) as cm:
                ms.insert(metric, t, value)

    def test_insert_failure_invalid_value_datapoint(self):
        ''' insert should fail if value is not a valid datapoint value '''
        ms = MetricStore()
        values = [None,'string', {'a':'dict'},['a','list'],{'set'},('a','tuple'),uuid.uuid4(), TimeUUID()]
        metric = Datapoint('uri')
        t = TimeUUID()
        for value in values:
            with self.assertRaises(TypeError) as cm:
                ms.insert(metric, t, value)

    def test_insert_failure_non_running_in_a_TransactionTask(self):
        ''' insert should fail if the function is not running in a TransactionTask '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        value = 1
        with self.assertRaises(AttributeError) as cm:
            ms.insert(metric, t, value)
        self.assertEqual(str(cm.exception),"'NoneType' object has no attribute 'get_tr'")

    @test.sync(loop)
    async def test_insert_success_within_active_transaction_datasource(self):
        ''' insert data within an active transaction should store temporarily the results and activate the transaction dirty flag '''
        t = TimeUUID()
        tr = Transaction(t)
        ms = MetricStore()
        async def f():
            nonlocal t
            nonlocal ms
            metric = Datasource('uri')
            t = t
            value = 'content'
            ms.insert(metric, t, value)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),1)
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].t, t)
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].value, value)
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].op, 'i')
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].value_orig, value)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, {ms})
            tr.discard()
            self.assertEqual(tr._dirty, set())
            self.assertFalse(tr.tid in ms._tr_dfs)

    @test.sync(loop)
    async def test_insert_success_within_active_transaction_datasource_multiple_rows(self):
        ''' insert data within an active transaction should store temporarily the results and activate the transaction dirty flag '''
        t = TimeUUID()
        tr = Transaction(t)
        ms = MetricStore()
        async def f():
            nonlocal ms
            nonlocal t
            metric = Datasource('uri')
            value = 'content'
            for i in range(1,1001):
                ms.insert(metric, t, value)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),1000)
            self.assertTrue(all([r.t == t for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
            self.assertTrue(all([r.value == value for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
            self.assertTrue(all([r.op == 'i' for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
            self.assertTrue(all([r.value_orig == value for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, {ms})
            tr.discard()
            self.assertEqual(tr._dirty, set())
            self.assertFalse(tr.tid in ms._tr_dfs)

    @test.sync(loop)
    async def test_insert_success_within_active_transaction_datapoint(self):
        ''' insert data within an active transaction should store temporarily the results and activate the transaction dirty flag '''
        t = TimeUUID()
        tr = Transaction(t)
        ms = MetricStore()
        async def f():
            nonlocal t
            nonlocal ms
            metric = Datapoint('uri')
            t = t
            value = decimal.Decimal("33")
            ms.insert(metric, t, value)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),1)
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].t, t)
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].value, int(value))
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].op, 'i')
            self.assertEqual(ms._tr_dfs[tr.tid][metric].iloc[0].value_orig, value)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, {ms})
            tr.discard()
            self.assertEqual(tr._dirty, set())
            self.assertFalse(tr.tid in ms._tr_dfs)

    @test.sync(loop)
    async def test_insert_success_within_active_transaction_datapoint_multiple_rows(self):
        ''' insert data within an active transaction should store temporarily the results and activate the transaction dirty flag '''
        t = TimeUUID()
        tr = Transaction(t)
        ms = MetricStore()
        async def f():
            nonlocal ms
            nonlocal t
            metric = Datapoint('uri')
            value = decimal.Decimal('33')
            for i in range(1,1001):
                ms.insert(metric, t, value)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),1000)
            self.assertTrue(all([r.t == t for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
            self.assertTrue(all([r.value == int(value) for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
            self.assertTrue(all([r.op == 'i' for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
            self.assertTrue(all([r.value_orig == value for index,r in ms._tr_dfs[tr.tid][metric].iterrows()]))
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, {ms})
            tr.discard()
            self.assertEqual(tr._dirty, set())
            self.assertFalse(tr.tid in ms._tr_dfs)

    @test.sync(loop)
    async def test_insert_success_no_active_transaction_datasource(self):
        ''' insert data outside an active transaction should store the contents in the store '''
        ms = MetricStore()
        metric = Datasource('uri')
        t = TimeUUID()
        value = 'content'
        ms.insert(metric, t, value)
        self.assertTrue(metric in ms._dfs)
        self.assertEqual(len(ms._dfs[metric]),1)
        self.assertEqual(ms._dfs[metric].iloc[0].t, t)
        self.assertEqual(ms._dfs[metric].iloc[0].value, value)

    @test.sync(loop)
    async def test_insert_success_no_active_transaction_datapoint(self):
        ''' insert data outside an active transaction should store the contents in the store '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        value = decimal.Decimal(44)
        ms.insert(metric, t, value)
        self.assertTrue(metric in ms._dfs)
        self.assertEqual(len(ms._dfs[metric]),1)
        self.assertEqual(ms._dfs[metric].iloc[0].t, t)
        self.assertEqual(ms._dfs[metric].iloc[0].value, value)

    @test.sync(loop)
    async def test_get_error_requesting_data(self):
        ''' get data should not mask any session exception '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        sessionIndex.sessions = []
        with self.assertRaises(exceptions.SessionNotFoundException) as cm:
            await ms.get(metric, t=t)

    @test.sync(loop)
    async def test_get_no_data_no_hooked(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
        self.assertIsNone(await ms.get(metric, t=t))
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,t,t,None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric],[])
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_no_data_no_hooked_start_none(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        start = None
        end = TimeUUID()
        with self.assertRaises(ValueError) as cm:
            await ms.get(metric, start=start, end=end)
        self.assertTrue(metric not in ms._synced_ranges)

    @test.sync(loop)
    async def test_get_no_data_no_hooked_end_none(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        start = None
        end = TimeUUID()
        with self.assertRaises(ValueError) as cm:
            await ms.get(metric, start=start, end=end)
        self.assertTrue(metric not in ms._synced_ranges)

    @test.sync(loop)
    async def test_get_no_data_no_interval_passed(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        start = None
        end = None
        with self.assertRaises(ValueError) as cm:
            await ms.get(metric, start=start, end=end)
        self.assertTrue(metric not in ms._synced_ranges)

    @test.sync(loop)
    async def test_get_no_data_but_hooked(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        ms._hooked.add(metric)
        t = TimeUUID()
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
        self.assertIsNone(await ms.get(metric, t=t))
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,t,t,None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],t)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],t)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_no_data_but_hooked_start_MIN_TIMEUUID(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        ms._hooked.add(metric)
        start = MIN_TIMEUUID
        end = TimeUUID()
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
        self.assertIsNone(await ms.get(metric, start=start, end=end))
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,start,end,None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],MIN_TIMEUUID)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],end)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_no_data_but_hooked_end_MAX_TIMEUUID(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        ms._hooked.add(metric)
        start = TimeUUID()
        end = MAX_TIMEUUID
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
        self.assertIsNone(await ms.get(metric, start=start, end=end))
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,start,end,None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],start)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],MAX_TIMEUUID)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_some_data_but_hooked_reach_count(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        ms._hooked.add(metric)
        data_t = TimeUUID()
        data_v = 332
        count = 1
        start = MIN_TIMEUUID
        end = MAX_TIMEUUID
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[(data_t,data_v)],'error':None})
        data = await ms.get(metric, start=start, end=end, count=count)
        self.assertTrue(len(data)==1)
        self.assertEqual(data.index[0], data_t)
        self.assertEqual(data[0], data_v)
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,MIN_TIMEUUID,MAX_TIMEUUID,1)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],data_t)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],data_t)
        self.assertTrue(metric in ms._dfs)
        self.assertEqual(len(ms._dfs[metric]),1)
        self.assertEqual(ms._dfs[metric].iloc[0].t, data_t)
        self.assertEqual(ms._dfs[metric].iloc[0].value,decimal.Decimal(data_v))
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_some_data_but_hooked_count_not_reached(self):
        ''' get data should return as much items as available '''
        ms = MetricStore()
        metric = Datapoint('uri')
        ms._hooked.add(metric)
        data_t = TimeUUID()
        data_v = 332
        count = 2
        start = MIN_TIMEUUID
        end = MAX_TIMEUUID
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[(data_t,data_v)],'error':None})
        data = await ms.get(metric, start=start, end=end, count=count)
        self.assertEqual(len(data),1)
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,MIN_TIMEUUID,MAX_TIMEUUID,2)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],MIN_TIMEUUID)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],MAX_TIMEUUID)
        self.assertTrue(metric in ms._dfs)
        self.assertEqual(len(ms._dfs[metric]),1)
        self.assertEqual(ms._dfs[metric].iloc[0].t, data_t)
        self.assertEqual(ms._dfs[metric].iloc[0].value,decimal.Decimal(data_v))
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_no_data_no_hooked_within_an_active_transaction(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        bck = prproc.request_data
        async def f():
            nonlocal ms, t
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
            self.assertIsNone(await ms.get(metric, t=t))
            self.assertEqual(prproc.request_data.call_count, 1)
            prproc.request_data.assert_called_with(metric,t,t,None)
            self.assertFalse(metric in ms._synced_ranges)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertTrue(metric in ms._tr_synced_ranges[tr.tid])
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid][metric]),1)
            self.assertEqual(ms._tr_synced_ranges[tr.tid][metric][0]['its'],t)
            self.assertEqual(ms._tr_synced_ranges[tr.tid][metric][0]['ets'],t)
            self.assertTrue(ms._tr_synced_ranges[tr.tid][metric][0]['t'] < time.monotonic())
            self.assertTrue(ms._tr_synced_ranges[tr.tid][metric][0]['t'] > tr.tm)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_get_no_data_but_hooked_within_an_active_transaction(self):
        ''' get data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('uri')
        ms._hooked.add(metric)
        t = TimeUUID()
        bck = prproc.request_data
        async def f():
            nonlocal ms, t
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
            self.assertIsNone(await ms.get(metric, t=t))
            self.assertEqual(prproc.request_data.call_count, 1)
            prproc.request_data.assert_called_with(metric,t,t,None)
            self.assertFalse(metric in ms._synced_ranges)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertTrue(metric in ms._tr_synced_ranges[tr.tid])
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid][metric]),1)
            self.assertEqual(ms._tr_synced_ranges[tr.tid][metric][0]['its'],t)
            self.assertEqual(ms._tr_synced_ranges[tr.tid][metric][0]['ets'],t)
            self.assertTrue(ms._tr_synced_ranges[tr.tid][metric][0]['t'] < time.monotonic())
            self.assertTrue(ms._tr_synced_ranges[tr.tid][metric][0]['t'] > tr.tm)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_request_data_range_failure_no_session_found(self):
        ''' _request_data_range should fail if no session is found '''
        ms = MetricStore()
        metric = Datapoint('test_request_data_range_failure_no_session_found')
        its = MIN_TIMEUUID
        ets = MAX_TIMEUUID
        count = None
        sessionIndex.sessions = []
        with self.assertRaises(exceptions.SessionNotFoundException) as cm:
            await ms._request_data_range(metric, its, ets, count)

    @test.sync(loop)
    async def test_request_data_range_success_no_data_within_a_transaction(self):
        ''' _request_data_range should request to Komlog the data range and store it in the MetricStore '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        its = None
        ets = None
        count = None
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
        ms._store = test.Mock(result_value = True)
        ms._add_synced_range = test.Mock(result_value = True)
        async def f():
            nonlocal ms, its, ets, count
            resp = await ms._request_data_range(metric, its, ets, count)
            self.assertEqual(resp['count'],0)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(ms in tr._dirty)
            self.assertEqual(prproc.request_data.call_count, 1)
            prproc.request_data.assert_called_with(metric,None,None,None)
            self.assertEqual(ms._store.call_count,0)
            self.assertEqual(ms._add_synced_range.call_count,1)
            self.assertEqual(ms._add_synced_range.call_args[0][0], metric)
            self.assertEqual(ms._add_synced_range.call_args[0][2], MIN_TIMEUUID)
            self.assertEqual(ms._add_synced_range.call_args[0][3], MAX_TIMEUUID)
            self.assertEqual(ms._add_synced_range.call_args[0][4], tr.tid)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_request_data_range_success_no_data_outside_a_transaction(self):
        ''' _request_data_range should request to Komlog the data range and store it in the MetricStore '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        its = None
        ets = None
        count = None
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':[],'error':None})
        ms._store = test.Mock(result_value = True)
        ms._add_synced_range = test.Mock(result_value = True)
        resp = await ms._request_data_range(metric, its, ets, count)
        self.assertEqual(resp['count'],0)
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,None,None,None)
        self.assertEqual(ms._store.call_count,0)
        self.assertEqual(ms._add_synced_range.call_count,1)
        self.assertEqual(ms._add_synced_range.call_args[0][0], metric)
        self.assertEqual(ms._add_synced_range.call_args[0][2], MIN_TIMEUUID)
        self.assertEqual(ms._add_synced_range.call_args[0][3], MAX_TIMEUUID)
        self.assertEqual(ms._add_synced_range.call_args[0][4], None)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_request_data_range_success_some_data_within_a_transaction(self):
        ''' _request_data_range should request to Komlog the data range and store it in the MetricStore '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        data = [(TimeUUID(),"4.5"),(TimeUUID(),"2.3"),(TimeUUID(),"1.4")]
        its = None
        ets = None
        count = None
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':data,'error':None})
        ms._store = test.Mock(result_value = None)
        ms._add_synced_range = test.Mock(result_value = None)
        async def f():
            nonlocal ms, its, ets, count
            resp = await ms._request_data_range(metric, its, ets, count)
            self.assertEqual(resp['count'],3)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(ms in tr._dirty)
            self.assertEqual(prproc.request_data.call_count, 1)
            prproc.request_data.assert_called_with(metric,None,None,None)
            self.assertEqual(ms._store.call_count,3)
            for i,call in enumerate(ms._store.call_args_list):
                self.assertEqual(call[0][0], metric)
                self.assertEqual(call[0][1], data[i][0])
                self.assertEqual(call[0][2], decimal.Decimal(data[i][1]))
                self.assertEqual(call[1]['op'], 'g')
                self.assertEqual(call[1]['tid'], tr.tid)
            self.assertEqual(ms._add_synced_range.call_count,1)
            self.assertEqual(ms._add_synced_range.call_args[0][0], metric)
            self.assertEqual(ms._add_synced_range.call_args[0][2], min(t[0] for t in data))
            self.assertEqual(ms._add_synced_range.call_args[0][3], max(t[0] for t in data))
            self.assertEqual(ms._add_synced_range.call_args[0][4], tr.tid)
        prproc.request_data = bck

    @test.sync(loop)
    async def test_request_data_range_success_some_data_outside_a_transaction(self):
        ''' _request_data_range should request to Komlog the data range and store it in the MetricStore '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        data = [(TimeUUID(),"4.5"),(TimeUUID(),"2.3"),(TimeUUID(),"1.4")]
        its = None
        ets = None
        count = None
        bck = prproc.request_data
        prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':data,'error':None})
        ms._store = test.Mock(result_value = None)
        ms._add_synced_range = test.Mock(result_value = None)
        resp = await ms._request_data_range(metric, its, ets, count)
        self.assertEqual(resp['count'],3)
        self.assertEqual(prproc.request_data.call_count, 1)
        prproc.request_data.assert_called_with(metric,None,None,None)
        self.assertEqual(ms._store.call_count,3)
        for i,call in enumerate(ms._store.call_args_list):
            self.assertEqual(call[0][0], metric)
            self.assertEqual(call[0][1], data[i][0])
            self.assertEqual(call[0][2], decimal.Decimal(data[i][1]))
            self.assertEqual(call[1]['op'], None)
            self.assertEqual(call[1]['tid'], None)
        self.assertEqual(ms._add_synced_range.call_count,1)
        self.assertEqual(ms._add_synced_range.call_args[0][0], metric)
        self.assertEqual(ms._add_synced_range.call_args[0][2], min(t[0] for t in data))
        self.assertEqual(ms._add_synced_range.call_args[0][3], max(t[0] for t in data))
        self.assertEqual(ms._add_synced_range.call_args[0][4], None)
        prproc.request_data = bck

    def test_store_success_non_existent_metric_no_tid(self):
        ''' store should create a DataFrame with the contents for the new metric '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        self.assertTrue(metric not in ms._dfs)
        value = decimal.Decimal(4.1)
        self.assertIsNone(ms._store(metric, t, value, tm=time.monotonic()))
        self.assertTrue(metric in ms._dfs)
        self.assertTrue(all(ms._dfs[metric].iloc[0] == [t,float(value)]))

    def test_store_success_previously_existent_metric_no_tid(self):
        ''' store should store the new value on the existent metric DataFrame '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        self.assertTrue(metric not in ms._dfs)
        value = decimal.Decimal(4)
        self.assertIsNone(ms._store(metric, t, value, tm=time.monotonic()))
        self.assertTrue(metric in ms._dfs)
        self.assertTrue(all(ms._dfs[metric].iloc[0] == [t,int(value)]))
        t2 = TimeUUID()
        value2 = decimal.Decimal(22)
        self.assertIsNone(ms._store(metric, t2, value2, tm=time.monotonic()))
        self.assertTrue(all(ms._dfs[metric].iloc[-1] == [t2,int(value2)]))

    def test_store_success_non_existent_metric_with_tid(self):
        ''' store should create a DataFrame with the contents for the new metric '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        op = 'g'
        tid = uuid.uuid4()
        self.assertFalse(metric in ms._dfs)
        self.assertFalse(tid in ms._tr_dfs)
        value = decimal.Decimal(4.1)
        self.assertIsNone(ms._store(metric, t, value, tm=time.monotonic(), op=op, tid=tid))
        self.assertFalse(metric in ms._dfs)
        self.assertTrue(tid in ms._tr_dfs)
        self.assertTrue(metric in ms._tr_dfs[tid])
        self.assertTrue(all(ms._tr_dfs[tid][metric].iloc[0] == [t,float(value),op,value]))

    def test_store_success_previously_existent_metric_with_tid(self):
        ''' store should store the new value on the existent metric DataFrame '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('uri')
        op = 'g'
        tid = uuid.uuid4()
        self.assertFalse(metric in ms._dfs)
        self.assertFalse(tid in ms._tr_dfs)
        value = decimal.Decimal(4.1)
        self.assertIsNone(ms._store(metric, t, value, tm=time.monotonic(), op=op, tid=tid))
        self.assertFalse(metric in ms._dfs)
        self.assertTrue(tid in ms._tr_dfs)
        self.assertTrue(metric in ms._tr_dfs[tid])
        self.assertTrue(all(ms._tr_dfs[tid][metric].iloc[0] == [t,float(value),op,value]))
        t2 = TimeUUID()
        op2 = 'i'
        value2 = decimal.Decimal(4.2)
        self.assertIsNone(ms._store(metric, t2, value2, tm=time.monotonic(), op=op2, tid=tid))
        self.assertFalse(metric in ms._dfs)
        self.assertTrue(tid in ms._tr_dfs)
        self.assertTrue(metric in ms._tr_dfs[tid])
        self.assertFalse(any(ms._tr_dfs[tid][metric].iloc[0] == [t2,float(value2),op2,value2]))
        self.assertTrue(all(ms._tr_dfs[tid][metric].iloc[-1] == [t2,float(value2),op2,value2]))

    @test.sync(loop)
    async def test_get_missing_ranges_success_no_synced_range_no_tr(self):
        ''' get_missing_ranges should return the full range if no previous range is synced '''
        its = TimeUUID(10)
        ets = TimeUUID(100)
        ms = MetricStore()
        metric = Datapoint('uri')
        count = None
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(missing,[{'its':its, 'ets':ets}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_inner_synced_range_no_tr(self):
        ''' get_missing_ranges should return the ranges beside the synced one '''
        its = TimeUUID(10)
        ets = TimeUUID(100)
        inner_its = TimeUUID(50)
        inner_ets = TimeUUID(75)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its, ets=inner_ets)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]),1)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],inner_its)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],inner_ets)
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(sorted(missing, key=lambda x:x['its']),[{'its':its, 'ets':inner_its},{'its':inner_ets,'ets':ets}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_overlapped_sup_synced_range_no_tr(self):
        ''' get_missing_ranges should return the ranges not synced '''
        its = TimeUUID(10)
        ets = TimeUUID(100)
        synced_its = TimeUUID(50)
        synced_ets = TimeUUID(175)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=synced_its, ets=synced_ets)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]),1)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],synced_its)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],synced_ets)
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(missing,[{'its':its, 'ets':synced_its}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_overlapped_inf_synced_range_no_tr(self):
        ''' get_missing_ranges should return the ranges not synced '''
        its = TimeUUID(10)
        ets = TimeUUID(100)
        synced_its = TimeUUID(1)
        synced_ets = TimeUUID(15)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=synced_its, ets=synced_ets)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]),1)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],synced_its)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],synced_ets)
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(missing,[{'its':synced_ets, 'ets':ets}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_inner_synced_range_no_tr(self):
        ''' get_missing_ranges should return the ranges beside the synced one '''
        its = TimeUUID(10)
        ets = TimeUUID(100)
        inner_its1 = TimeUUID(20)
        inner_ets1 = TimeUUID(35)
        inner_its2 = TimeUUID(60)
        inner_ets2 = TimeUUID(85)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its1, ets=inner_ets1)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its2, ets=inner_ets2)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]),2)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],inner_its1)
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],inner_ets1)
        self.assertEqual(ms._synced_ranges[metric][1]['its'],inner_its2)
        self.assertEqual(ms._synced_ranges[metric][1]['ets'],inner_ets2)
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(sorted(missing, key=lambda x:x['its']),[{'its':its, 'ets':inner_its1},{'its':inner_ets1,'ets':inner_its2},{'its':inner_ets2, 'ets':ets}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_multiple_synced_range_no_tr(self):
        ''' get_missing_ranges should return the ranges beside the synced one '''
        its = TimeUUID(10)
        ets = TimeUUID(100)
        inner_its1 = TimeUUID(20)
        inner_ets1 = TimeUUID(35)
        inner_its2 = TimeUUID(60)
        inner_ets2 = TimeUUID(85)
        others =[(TimeUUID(1),TimeUUID(3)),(TimeUUID(8),TimeUUID(10, lowest=True)),(TimeUUID(100, highest=True),TimeUUID(101))]
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its1, ets=inner_ets1)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its2, ets=inner_ets2)
        for interv in others:
            ms._add_synced_range(metric=metric, t=time.monotonic(), its=interv[0], ets=interv[1])
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]),5)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],others[0][0])
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],others[0][1])
        self.assertEqual(ms._synced_ranges[metric][1]['its'],others[1][0])
        self.assertEqual(ms._synced_ranges[metric][1]['ets'],others[1][1])
        self.assertEqual(ms._synced_ranges[metric][2]['its'],inner_its1)
        self.assertEqual(ms._synced_ranges[metric][2]['ets'],inner_ets1)
        self.assertEqual(ms._synced_ranges[metric][3]['its'],inner_its2)
        self.assertEqual(ms._synced_ranges[metric][3]['ets'],inner_ets2)
        self.assertEqual(ms._synced_ranges[metric][4]['its'],others[2][0])
        self.assertEqual(ms._synced_ranges[metric][4]['ets'],others[2][1])
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(sorted(missing, key=lambda x:x['its']),[{'its':its, 'ets':inner_its1},{'its':inner_ets1,'ets':inner_its2},{'its':inner_ets2, 'ets':ets}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_multiple_synced_range_its_equals_ets_no_tr(self):
        ''' get_missing_ranges should return the ranges beside the synced one '''
        its = TimeUUID(11,random=False)
        ets = TimeUUID(11,random=False)
        inner_its1 = TimeUUID(20)
        inner_ets1 = TimeUUID(35)
        inner_its2 = TimeUUID(60)
        inner_ets2 = TimeUUID(85)
        others =[(TimeUUID(1),TimeUUID(3)),(TimeUUID(8),TimeUUID(10, lowest=True)),(TimeUUID(100, highest=True),TimeUUID(101))]
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its1, ets=inner_ets1)
        ms._add_synced_range(metric=metric, t=time.monotonic(), its=inner_its2, ets=inner_ets2)
        for interv in others:
            ms._add_synced_range(metric=metric, t=time.monotonic(), its=interv[0], ets=interv[1])
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]),5)
        self.assertEqual(ms._synced_ranges[metric][0]['its'],others[0][0])
        self.assertEqual(ms._synced_ranges[metric][0]['ets'],others[0][1])
        self.assertEqual(ms._synced_ranges[metric][1]['its'],others[1][0])
        self.assertEqual(ms._synced_ranges[metric][1]['ets'],others[1][1])
        self.assertEqual(ms._synced_ranges[metric][2]['its'],inner_its1)
        self.assertEqual(ms._synced_ranges[metric][2]['ets'],inner_ets1)
        self.assertEqual(ms._synced_ranges[metric][3]['its'],inner_its2)
        self.assertEqual(ms._synced_ranges[metric][3]['ets'],inner_ets2)
        self.assertEqual(ms._synced_ranges[metric][4]['its'],others[2][0])
        self.assertEqual(ms._synced_ranges[metric][4]['ets'],others[2][1])
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(missing,[{'its':ets, 'ets':its}])

    @test.sync(loop)
    async def test_get_missing_ranges_success_no_synced_range_inside_tr(self):
        ''' get_missing_ranges should return the entire interval if no previous interval is synced '''
        t = TimeUUID()
        its = TimeUUID(10)
        ets = TimeUUID(100)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        async def f():
            nonlocal its, ets, metric, ms
            missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
            self.assertEqual(missing,[{'its':its, 'ets':ets}])
            self.assertFalse(metric in ms._synced_ranges)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, set())
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertTrue(metric in ms._tr_synced_ranges[tr.tid])
            self.assertEqual(ms._tr_synced_ranges[tr.tid][metric], [])

    @test.sync(loop)
    async def test_get_missing_ranges_success_no_synced_range_before_t_in_tr(self):
        ''' get_missing_ranges should return the entire interval if no previous interval is synced before t '''
        t = TimeUUID()
        its = TimeUUID(1)
        ets = TimeUUID(100)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        async def f():
            nonlocal its, ets, metric, ms
            missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
            self.assertEqual(missing,[{'its':its, 'ets':ets}])
            self.assertTrue(metric in ms._synced_ranges)
        async with Transaction(t) as tr:
            # add some synced ranges AFTER the transaction has begun, this has no effect to the synced ranges in tr
            ranges =[(TimeUUID(1),TimeUUID(3)),(TimeUUID(8),TimeUUID(10, lowest=True)),(TimeUUID(100, highest=True),TimeUUID(101))]
            for r in ranges:
                ms._add_synced_range(metric=metric, t=time.monotonic(), its=r[0], ets=r[1])
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, set())
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertTrue(metric in ms._tr_synced_ranges[tr.tid])
            self.assertEqual(ms._tr_synced_ranges[tr.tid][metric], [])
            self.assertTrue(metric in ms._synced_ranges)
            self.assertNotEqual(ms._synced_ranges[metric], [])

    @test.sync(loop)
    async def test_get_missing_ranges_success_load_already_synced_ranges_before_t_in_tr(self):
        ''' get_missing_ranges should return an empty list '''
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(2, highest=True)
        metric = Datapoint('uri')
        ms = MetricStore()
        count = None
        ms._hooked.add(metric)
        # add some synced ranges BEFORE the transaction has begun, this has effect to the synced ranges in tr
        ranges =[(TimeUUID(1),TimeUUID(3)),(TimeUUID(8),TimeUUID(10, lowest=True)),(TimeUUID(100, highest=True),TimeUUID(101))]
        for r in ranges:
            ms._add_synced_range(metric=metric, t=time.monotonic(), its=r[0], ets=r[1])
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]), 3)
        t = TimeUUID()
        async def f():
            nonlocal its, ets, metric, ms
            missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
            self.assertEqual(missing,[])
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, set())
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertTrue(metric in ms._tr_synced_ranges[tr.tid])
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid][metric]), 3)

    @test.sync(loop)
    async def test_get_missing_ranges_success_load_already_synced_ranges_before_t_in_tr_but_not_the_one_needed(self):
        ''' get_missing_ranges should return the requested range '''
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(2, highest=True)
        metric = Datapoint('uri')
        count = None
        ms = MetricStore()
        ms._hooked.add(metric)
        # add some synced ranges BEFORE the transaction has begun, this has effect to the synced ranges in tr
        ranges =[(TimeUUID(8),TimeUUID(10, lowest=True)),(TimeUUID(100, highest=True),TimeUUID(101))]
        for r in ranges:
            ms._add_synced_range(metric=metric, t=time.monotonic(), its=r[0], ets=r[1])
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(len(ms._synced_ranges[metric]), 2)
        t = TimeUUID()
        async def f():
            nonlocal its, ets, metric, ms
            missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
            self.assertEqual(missing,[{'its':its,'ets':ets}])
        async with Transaction(t) as tr:
            ranges =[(TimeUUID(1),TimeUUID(3))]
            for r in ranges:
                ms._add_synced_range(metric=metric, t=time.monotonic(), its=r[0], ets=r[1])
            await TransactionTask(coro=f(), tr=tr)
            self.assertEqual(tr._dirty, set())
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertTrue(metric in ms._tr_synced_ranges[tr.tid])
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid][metric]), 2)
            self.assertEqual(len(ms._synced_ranges[metric]), 3)

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none(self):
        ''' get_missing_ranges should return the range (MIN_TIMEUUID - ets) '''
        its = None
        ets = TimeUUID()
        metric = Datapoint('uri')
        count = 100
        ms = MetricStore()
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(missing,[{'its':MIN_TIMEUUID,'ets':ets}])

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_ets_none(self):
        ''' get_missing_ranges should return the range (its, MAX_TIMEUUID) '''
        its = TimeUUID()
        ets = None
        metric = Datapoint('uri')
        count = 100
        ms = MetricStore()
        missing = ms._get_missing_ranges(metric=metric, its=its, ets=ets, count=count)
        self.assertEqual(missing,[{'its':its,'ets':MAX_TIMEUUID}])

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_range_with_less_than_count_elem(self):
        ''' get_missing_ranges should return the range til the interleaved synced range, and the range to MIN_TIMEUUID '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range
            i_its = TimeUUID(100, lowest=True)
            i_ets = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its, i_ets, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),1)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced one
            t = TimeUUID()
            count = 1000
            missing = ms._get_missing_ranges(metric=metric, its=None, ets=t, count=count)
            # we should receive two intervals, the one till the synced one and the one from the synced one to MIN_T
            self.assertEqual(len(missing),2)
            self.assertEqual(missing[0],{'its':i_ets, 'ets':t})
            self.assertEqual(missing[1],{'its':MIN_TIMEUUID, 'ets':i_its})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_range_with_more_than_count_elem(self):
        ''' get_missing_ranges should return the range til the interleaved synced range with enought count elem '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range
            i_its = TimeUUID(100, lowest=True)
            i_ets = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its, i_ets, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),1)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced one
            t = TimeUUID()
            count = 10
            missing = ms._get_missing_ranges(metric=metric, its=None, ets=t, count=count)
            # we should receive two intervals, the one till the synced one and the one from the synced one to MIN_T
            self.assertEqual(len(missing),1)
            self.assertEqual(missing[0],{'its':i_ets, 'ets':t})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_ranges_return_all(self):
        ''' get_missing_ranges should return the all ranges because not enough elements '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(300, lowest=True)
            i_ets2 = TimeUUID(400, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID()
            count = 1000
            missing = ms._get_missing_ranges(metric=metric, its=None, ets=t, count=count)
            # we should receive three intervals:
            # t -> i_ets2
            # i_its2 -> i_ets1
            # i_its1 -> MIN_TIMEUUID
            self.assertEqual(len(missing),3)
            self.assertEqual(missing[0],{'its':i_ets2, 'ets':t})
            self.assertEqual(missing[1],{'its':i_ets1, 'ets':i_its2})
            self.assertEqual(missing[2],{'its':MIN_TIMEUUID, 'ets':i_its1})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_ranges_return_closest(self):
        ''' get_missing_ranges should return the only ranges to sum requested elements '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(300, lowest=True)
            i_ets2 = TimeUUID(400, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID()
            count = 10
            missing = ms._get_missing_ranges(metric=metric, its=None, ets=t, count=count)
            # we should receive one interval:
            # t -> i_ets2
            self.assertEqual(len(missing),1)
            self.assertEqual(missing[0],{'its':i_ets2, 'ets':t})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_ranges_return_two(self):
        ''' get_missing_ranges should return the two ranges until elements sum count '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(300, lowest=True)
            i_ets2 = TimeUUID(400, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID()
            count = 150
            missing = ms._get_missing_ranges(metric=metric, its=None, ets=t, count=count)
            # we should receive two intervals:
            # t -> i_ets2
            # i_its2 -> i_ets1
            self.assertEqual(len(missing),2)
            self.assertEqual(missing[0],{'its':i_ets2, 'ets':t})
            self.assertEqual(missing[1],{'its':i_ets1, 'ets':i_its2})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_ets_none_interleaved_synced_range_with_less_than_count_elem(self):
        ''' get_missing_ranges should return the range til the interleaved synced range, and the range to MAX_TIMEUUID '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range
            i_its = TimeUUID(100, lowest=True)
            i_ets = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its, i_ets, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),1)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced one
            t = TimeUUID(1)
            count = 1000
            missing = ms._get_missing_ranges(metric=metric, its=t, ets=None, count=count)
            # we should receive two intervals, the one till the synced one and the one from the synced one to MIN_T
            self.assertEqual(len(missing),2)
            self.assertEqual(missing[0],{'its':t, 'ets':i_its})
            self.assertEqual(missing[1],{'its':i_ets, 'ets':MAX_TIMEUUID})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_range_with_more_than_count_elem(self):
        ''' get_missing_ranges should return the range til the interleaved synced range with enought count elem '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range
            i_its = TimeUUID(100, lowest=True)
            i_ets = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its, i_ets, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),1)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced one
            t = TimeUUID(1)
            count = 10
            missing = ms._get_missing_ranges(metric=metric, its=t, ets=None, count=count)
            # we should receive two intervals, the one till the synced one and the one from the synced one to MIN_T
            self.assertEqual(len(missing),1)
            self.assertEqual(missing[0],{'its':t, 'ets':i_its})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_ets_none_interleaved_synced_ranges_return_all(self):
        ''' get_missing_ranges should return the all ranges because not enough elements '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(300, lowest=True)
            i_ets2 = TimeUUID(400, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID(1)
            count = 1000
            missing = ms._get_missing_ranges(metric=metric, its=t, ets=None, count=count)
            # we should receive three intervals:
            # t -> i_its1
            # i_ets1 -> i_its2
            # i_ets2 -> MAX_TIMEUUID
            self.assertEqual(len(missing),3)
            self.assertEqual(missing[0],{'its':t, 'ets':i_its1})
            self.assertEqual(missing[1],{'its':i_ets1, 'ets':i_its2})
            self.assertEqual(missing[2],{'its':i_ets2, 'ets':MAX_TIMEUUID})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_ranges_return_closest(self):
        ''' get_missing_ranges should return the only ranges to sum requested elements '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(300, lowest=True)
            i_ets2 = TimeUUID(400, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID(1)
            count = 10
            missing = ms._get_missing_ranges(metric=metric, its=t, ets=None, count=count)
            # we should receive one interval:
            # t -> i_its1
            self.assertEqual(len(missing),1)
            self.assertEqual(missing[0],{'its':t, 'ets':i_its1})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_missing_ranges_open_interval_its_none_interleaved_synced_ranges_return_two(self):
        ''' get_missing_ranges should return the two ranges until elements sum count '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(300, lowest=True)
            i_ets2 = TimeUUID(400, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID(1)
            count = 150
            missing = ms._get_missing_ranges(metric=metric, its=t, ets=None, count=count)
            # we should receive two intervals:
            # t -> i_its1
            # i_ets1 -> i_its1
            self.assertEqual(len(missing),2)
            self.assertEqual(missing[0],{'its':t, 'ets':i_its1})
            self.assertEqual(missing[1],{'its':i_ets1, 'ets':i_its2})
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_get_with_count_param_dont_sync_more_intervals_if_requested_items_reach_count(self):
        ''' get_missing_ranges should return the two ranges until elements sum count '''
        try:
            bck = prproc.request_data
            metric = Datapoint('uri')
            ms = MetricStore()
            ms._hooked.add(metric)
            # We are going to sync an interleaved range (first)
            i_its1 = TimeUUID(100, lowest=True)
            i_ets1 = TimeUUID(200, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(100+i),100+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its1, i_ets1, None)
            # We are going to sync an interleaved range (second)
            i_its2 = TimeUUID(500, lowest=True)
            i_ets2 = TimeUUID(600, highest=True)
            i_data = []
            for i in range(1,100):
                i_data.append((TimeUUID(500+i),500+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            await ms._request_data_range(metric, i_its2, i_ets2, None)
            # the interval should be synced already
            self.assertTrue(metric in ms._synced_ranges)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            # prepare the data we are going to return when we request data from Timeuuid(200) to TimeUUID(400)
            i_data = []
            for i in range(1,300):
                i_data.append((TimeUUID(300+i),300+i))
            prproc.request_data = test.AsyncMock(return_value = {'success':True,'data':i_data,'error':None})
            # Now, lets get missing ranges in an open interval, with count higher than the num elements in synced ranges
            t = TimeUUID(600, highest=True)
            count = 250
            missing = ms._get_missing_ranges(metric=metric, its=None, ets=t, count=count)
            # we should receive two intervals theorically before requesting data:
            # t -> i_its1
            # i_ets1 -> i_its1
            self.assertEqual(len(missing),2)
            self.assertEqual(missing[0],{'its':i_ets1, 'ets':i_its2})
            self.assertEqual(missing[1],{'its':MIN_TIMEUUID, 'ets':i_its1})
            data = await ms.get(metric, end=t, count=20)
            self.assertEqual(len(data),20)
            self.assertEqual(len(ms._synced_ranges[metric]),2)
            data = await ms.get(metric, end=t, count=200)
            self.assertEqual(len(data),200)
            self.assertEqual(len(ms._synced_ranges[metric]),3)
            data = await ms.get(metric, end=t, count=2000)
            self.assertEqual(len(data),497)
            self.assertEqual(len(ms._synced_ranges[metric]),4)
        except:
            raise
        finally:
            prproc.request_data = bck

    @test.sync(loop)
    async def test_add_synced_range_failure_no_tr_no_hooked(self):
        ''' add_synced_range should not add anything if there is no associated tr and metric is not hooked '''
        its = TimeUUID(5)
        ets = TimeUUID(20)
        t = time.monotonic()
        metric = Datapoint('datapoint.uri')
        tid = None
        ms = MetricStore()
        ms._add_synced_range(metric=metric, t=t, its=its, ets=ets, tid=None)
        self.assertFalse(metric in ms._synced_ranges)
        self.assertEqual(len(ms._tr_synced_ranges),0)

    @test.sync(loop)
    async def test_add_synced_range_success_no_tr_one_range(self):
        ''' add_synced_range should add the range to the synced ranges list '''
        its = TimeUUID(5)
        ets = TimeUUID(20)
        t = time.monotonic()
        metric = Datapoint('datapoint.uri')
        tid = None
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=t, its=its, ets=ets, tid=None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric], [{'t':t, 'its':its, 'ets':ets}])

    @test.sync(loop)
    async def test_add_synced_range_success_no_tr_multiple_ranges_no_overlaps(self):
        ''' add_synced_range should add the ranges to the synced ranges list '''
        ranges = [
            {'its':TimeUUID(1),'ets':TimeUUID(2)},
            {'its':TimeUUID(3),'ets':TimeUUID(4)},
            {'its':TimeUUID(5),'ets':TimeUUID(6)},
            {'its':TimeUUID(7),'ets':TimeUUID(8)},
            {'its':TimeUUID(9),'ets':TimeUUID(10)},
            {'its':TimeUUID(11),'ets':TimeUUID(12)},
            {'its':TimeUUID(13),'ets':TimeUUID(14)}
        ]
        t = time.monotonic()
        metric = Datapoint('datapoint.uri')
        tid = None
        ms = MetricStore()
        ms._hooked.add(metric)
        for r in ranges:
            ms._add_synced_range(metric=metric, t=t, its=r['its'], ets=r['ets'], tid=None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric], [{'t':t,'its':r['its'],'ets':r['ets']} for r in ranges])

    @test.sync(loop)
    async def test_add_synced_range_success_no_tr_multiple_ranges_one_overlap(self):
        ''' add_synced_range should add the ranges to the synced ranges list '''
        t = time.monotonic()
        ranges = [
            {'its':TimeUUID(4, random=False),'ets':TimeUUID(9, random=False)},
            {'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
            {'its':TimeUUID(2, random=False),'ets':TimeUUID(11, random=False)},
        ]
        expected = [
            {'t':t, 'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'t':t, 'its':TimeUUID(5, random=False),'ets':TimeUUID(6, random=False)},
            {'t':t, 'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'t':t, 'its':TimeUUID(7, random=False),'ets':TimeUUID(8, random=False)},
            {'t':t, 'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
            {'t':t, 'its':TimeUUID(2, random=False),'ets':TimeUUID(11, random=False)},
        ]
        metric = Datapoint('datapoint.uri')
        tid = None
        ms = MetricStore()
        ms._hooked.add(metric)
        for r in ranges:
            ms._add_synced_range(metric=metric, t=t, its=r['its'], ets=r['ets'], tid=None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric], [{'t':t,'its':TimeUUID(2, random=False),'ets':TimeUUID(11, random=False)}])

    @test.sync(loop)
    async def test_add_synced_range_success_no_tr_multiple_ranges_some_overlaps(self):
        ''' add_synced_range should add the ranges to the synced ranges list '''
        t = time.monotonic()
        ranges = [
            {'its':TimeUUID(4, random=False),'ets':TimeUUID(9, random=False)},
            {'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
        ]
        expected = [
            {'t':t, 'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'t':t, 'its':TimeUUID(5, random=False),'ets':TimeUUID(6, random=False)},
            {'t':t, 'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'t':t, 'its':TimeUUID(7, random=False),'ets':TimeUUID(8, random=False)},
            {'t':t, 'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
        ]
        metric = Datapoint('datapoint.uri')
        tid = None
        ms = MetricStore()
        ms._hooked.add(metric)
        for r in ranges:
            ms._add_synced_range(metric=metric, t=t, its=r['its'], ets=r['ets'], tid=None)
        self.assertTrue(metric in ms._synced_ranges)
        self.assertEqual(ms._synced_ranges[metric], sorted(expected, key=lambda x:x['its']))

    @test.sync(loop)
    async def test_add_synced_range_success_tr_one_range_no_hooked(self):
        ''' add_synced_range should add the range to the transaction synced ranges list '''
        its = TimeUUID(5)
        ets = TimeUUID(20)
        t = time.monotonic()
        tid = uuid.uuid4()
        metric = Datapoint('datapoint.uri')
        ms = MetricStore()
        ms._add_synced_range(metric=metric, t=t, its=its, ets=ets, tid=tid)
        self.assertFalse(metric in ms._synced_ranges)
        self.assertTrue(tid in ms._tr_synced_ranges)
        self.assertTrue(metric in ms._tr_synced_ranges[tid])
        self.assertEqual(ms._tr_synced_ranges[tid][metric], [{'t':t, 'its':its, 'ets':ets}])

    @test.sync(loop)
    async def test_add_synced_range_success_tr_one_range_hooked(self):
        ''' add_synced_range should add the range to the transaction synced ranges list '''
        its = TimeUUID(5)
        ets = TimeUUID(20)
        t = time.monotonic()
        tid = uuid.uuid4()
        metric = Datapoint('datapoint.uri')
        ms = MetricStore()
        ms._hooked.add(metric)
        ms._add_synced_range(metric=metric, t=t, its=its, ets=ets, tid=tid)
        self.assertFalse(metric in ms._synced_ranges)
        self.assertTrue(tid in ms._tr_synced_ranges)
        self.assertTrue(metric in ms._tr_synced_ranges[tid])
        self.assertEqual(ms._tr_synced_ranges[tid][metric], [{'t':t, 'its':its, 'ets':ets}])

    @test.sync(loop)
    async def test_add_synced_range_success_tr_multiple_ranges_no_overlaps(self):
        ''' add_synced_range should add the ranges to the synced ranges list '''
        ranges = [
            {'its':TimeUUID(1),'ets':TimeUUID(2)},
            {'its':TimeUUID(3),'ets':TimeUUID(4)},
            {'its':TimeUUID(5),'ets':TimeUUID(6)},
            {'its':TimeUUID(7),'ets':TimeUUID(8)},
            {'its':TimeUUID(9),'ets':TimeUUID(10)},
            {'its':TimeUUID(11),'ets':TimeUUID(12)},
            {'its':TimeUUID(13),'ets':TimeUUID(14)}
        ]
        t = time.monotonic()
        metric = Datapoint('datapoint.uri')
        tid = uuid.uuid4()
        ms = MetricStore()
        ms._hooked.add(metric)
        for r in ranges:
            ms._add_synced_range(metric=metric, t=t, its=r['its'], ets=r['ets'], tid=tid)
        self.assertFalse(metric in ms._synced_ranges)
        self.assertTrue(tid in ms._tr_synced_ranges)
        self.assertTrue(metric in ms._tr_synced_ranges[tid])
        self.assertEqual(ms._tr_synced_ranges[tid][metric], [{'t':t,'its':r['its'],'ets':r['ets']} for r in ranges])

    @test.sync(loop)
    async def test_add_synced_range_success_tr_multiple_ranges_one_overlap(self):
        ''' add_synced_range should add the ranges to the synced ranges list '''
        t = time.monotonic()
        ranges = [
            {'its':TimeUUID(4, random=False),'ets':TimeUUID(9, random=False)},
            {'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
            {'its':TimeUUID(2, random=False),'ets':TimeUUID(11, random=False)},
        ]
        expected = [
            {'t':t, 'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'t':t, 'its':TimeUUID(5, random=False),'ets':TimeUUID(6, random=False)},
            {'t':t, 'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'t':t, 'its':TimeUUID(7, random=False),'ets':TimeUUID(8, random=False)},
            {'t':t, 'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
            {'t':t, 'its':TimeUUID(2, random=False),'ets':TimeUUID(11, random=False)},
        ]
        metric = Datapoint('datapoint.uri')
        tid = uuid.uuid4()
        ms = MetricStore()
        ms._hooked.add(metric)
        for r in ranges:
            ms._add_synced_range(metric=metric, t=t, its=r['its'], ets=r['ets'], tid=tid)
        self.assertFalse(metric in ms._synced_ranges)
        self.assertTrue(tid in ms._tr_synced_ranges)
        self.assertTrue(metric in ms._tr_synced_ranges[tid])
        self.assertEqual(ms._tr_synced_ranges[tid][metric], [{'t':t,'its':TimeUUID(2, random=False),'ets':TimeUUID(11, random=False)}])

    @test.sync(loop)
    async def test_add_synced_range_success_tr_multiple_ranges_some_overlaps(self):
        ''' add_synced_range should add the ranges to the synced ranges list '''
        t = time.monotonic()
        ranges = [
            {'its':TimeUUID(4, random=False),'ets':TimeUUID(9, random=False)},
            {'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
        ]
        expected = [
            {'t':t, 'its':TimeUUID(3, random=False),'ets':TimeUUID(5, random=False)},
            {'t':t, 'its':TimeUUID(5, random=False),'ets':TimeUUID(6, random=False)},
            {'t':t, 'its':TimeUUID(6, random=False),'ets':TimeUUID(7, random=False)},
            {'t':t, 'its':TimeUUID(7, random=False),'ets':TimeUUID(8, random=False)},
            {'t':t, 'its':TimeUUID(8, random=False),'ets':TimeUUID(10, random=False)},
        ]
        metric = Datapoint('datapoint.uri')
        tid = uuid.uuid4()
        ms = MetricStore()
        for r in ranges:
            ms._add_synced_range(metric=metric, t=t, its=r['its'], ets=r['ets'], tid=tid)
        self.assertFalse(metric in ms._synced_ranges)
        self.assertTrue(tid in ms._tr_synced_ranges)
        self.assertTrue(metric in ms._tr_synced_ranges[tid])
        self.assertEqual(ms._tr_synced_ranges[tid][metric], sorted(expected, key=lambda x:x['its']))

    @test.sync(loop)
    async def test_get_metric_data_no_tr_no_data_found(self):
        ''' get_metric_data should return None if no data is found '''
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID()
        ets = TimeUUID()
        count = None
        self.assertIsNone(ms._get_metric_data(metric, its, ets, count))

    @test.sync(loop)
    async def test_get_metric_data_no_tr_no_data_found_in_interval(self):
        ''' get_metric_data should return None if no data is found in the range requested '''
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(10, lowest=True)
        ets = TimeUUID(11)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        self.assertIsNone(ms._get_metric_data(metric, its, ets, count))

    @test.sync(loop)
    async def test_get_metric_data_no_tr_some_data_found_in_interval(self):
        ''' get_metric_data should return None if no data is found in the range requested '''
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data)
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data.iloc[i],regs[i+1]['value'])
            self.assertEqual(data.index[i],regs[i+1]['t'])

    @test.sync(loop)
    async def test_get_metric_data_no_tr_some_data_found_in_interval_count(self):
        ''' get_metric_data should return the data in the interval requested and max count '''
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = 2
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertEqual(len(data),2)
        self.assertEqual(data.iloc[0], 4)
        self.assertEqual(data.iloc[1], 5)
        self.assertEqual(data.index[0], regs[3]['t'])
        self.assertEqual(data.index[1], regs[4]['t'])

    @test.sync(loop)
    async def test_get_metric_data_no_tr_some_data_found_in_interval_count_higher_than_data_length(self):
        ''' get_metric_data should return the data in the interval requested and max count '''
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = 100
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data)
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data.iloc[i],regs[i+1]['value'])
            self.assertEqual(data.index[i],regs[i+1]['t'])
        self.assertEqual(len(data),4)

    @test.sync(loop)
    async def test_get_metric_data_no_tr_some_data_found_in_interval_cannot_modify_store(self):
        ''' get_metric_data should return a copy of the df data, so modifying it does not modify the store '''
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data)
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data.iloc[i],regs[i+1]['value'])
            self.assertEqual(data.index[i],regs[i+1]['t'])
        for i,row in enumerate(data):
            data.iloc[i] = row + 1
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data.iloc[i],regs[i+1]['value']+1)
            self.assertEqual(data.index[i],regs[i+1]['t'])
        data2 = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data2)
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data2.iloc[i],regs[i+1]['value'])
            self.assertEqual(data2.index[i],regs[i+1]['t'])

    @test.sync(loop)
    async def test_get_metric_data_no_tr_some_data_found_in_interval_cannot_modify_store_datasource(self):
        ''' get_metric_data should return a copy of the data, so modifying it does not modify the store '''
        ms = MetricStore()
        metric = Datasource('datasource.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':'value1'},
            {'t':TimeUUID(2),'value':'value2'},
            {'t':TimeUUID(3),'value':'value3'},
            {'t':TimeUUID(4),'value':'value4'},
            {'t':TimeUUID(5),'value':'value5'},
            {'t':TimeUUID(6),'value':'value6'},
            {'t':TimeUUID(7),'value':'value7'},
            {'t':TimeUUID(8),'value':'value8'},
            {'t':TimeUUID(9),'value':'value9'},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data)
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data.iloc[i],regs[i+1]['value'])
            self.assertEqual(data.index[i],regs[i+1]['t'])
        for i,row in enumerate(data):
            data.iloc[i] = row + '1'
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data.iloc[i],regs[i+1]['value']+'1')
            self.assertEqual(data.index[i],regs[i+1]['t'])
        data2 = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data2)
        for i,reg in enumerate(regs[1:5]):
            self.assertEqual(data2.iloc[i],regs[i+1]['value'])
            self.assertEqual(data2.index[i],regs[i+1]['t'])

    @test.sync(loop)
    async def test_get_metric_data_no_tr_some_data_found_in_interval_manage_dups(self):
        ''' get_metric_data should return a copy of the last data, keeping the last reg if some row is dup '''
        ms = MetricStore()
        metric = Datasource('datasource.uri')
        its = TimeUUID(1, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':5},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        expected = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
            {'t':TimeUUID(5, random=False),'value':5},
        ]
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data)
        for i,reg in enumerate(expected):
            self.assertEqual(data.iloc[i],reg['value'])
            self.assertEqual(data.index[i],reg['t'])

    @test.sync(loop)
    async def test_get_metric_data_in_tr_no_data_found(self):
        ''' get_metric_data should return None if no data is found '''
        t = TimeUUID()
        async def f():
            ms = MetricStore()
            metric = Datapoint('datapoint.uri')
            its = TimeUUID()
            ets = TimeUUID()
            count = None
            self.assertIsNone(ms._get_metric_data(metric, its, ets, count))
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)

    @test.sync(loop)
    async def test_get_metric_data_in_tr_no_data_found_in_interval(self):
        ''' get_metric_data should return None if no data is found in the range requested '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(10, lowest=True)
        ets = TimeUUID(11)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            self.assertIsNone(ms._get_metric_data(metric, its, ets, count))
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_found_in_interval(self):
        ''' get_metric_data should return the data found in the range requested '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data)
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data.iloc[i],regs[i+1]['value'])
                self.assertEqual(data.index[i],regs[i+1]['t'])
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_found_in_interval_count(self):
        ''' get_metric_data should return the data in the interval requested and max count '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = 2
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertEqual(len(data),count)
            self.assertEqual(data.iloc[0], 4)
            self.assertEqual(data.iloc[1], 5)
            self.assertEqual(data.index[0], regs[3]['t'])
            self.assertEqual(data.index[1], regs[4]['t'])
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_found_in_interval_count_higher_than_data_length(self):
        ''' get_metric_data should return the data in the interval requested and max count '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = 100
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data)
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data.iloc[i],regs[i+1]['value'])
                self.assertEqual(data.index[i],regs[i+1]['t'])
            self.assertEqual(len(data),4)
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_found_in_interval_cannot_modify_store(self):
        ''' get_metric_data should return a copy of the df data, so modifying it does not modify the store '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datapoint('datapoint.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':1},
            {'t':TimeUUID(2),'value':2},
            {'t':TimeUUID(3),'value':3},
            {'t':TimeUUID(4),'value':4},
            {'t':TimeUUID(5),'value':5},
            {'t':TimeUUID(6),'value':6},
            {'t':TimeUUID(7),'value':7},
            {'t':TimeUUID(8),'value':8},
            {'t':TimeUUID(9),'value':9},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data)
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data.iloc[i],regs[i+1]['value'])
                self.assertEqual(data.index[i],regs[i+1]['t'])
            for i,row in enumerate(data):
                data.iloc[i] = row + 1
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data.iloc[i],regs[i+1]['value']+1)
                self.assertEqual(data.index[i],regs[i+1]['t'])
            data2 = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data2)
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data2.iloc[i],regs[i+1]['value'])
                self.assertEqual(data2.index[i],regs[i+1]['t'])
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_found_in_interval_cannot_modify_store_datasource(self):
        ''' get_metric_data should return a copy of the data, so modifying it does not modify the store '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datasource('datasource.uri')
        its = TimeUUID(2, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1),'value':'value1'},
            {'t':TimeUUID(2),'value':'value2'},
            {'t':TimeUUID(3),'value':'value3'},
            {'t':TimeUUID(4),'value':'value4'},
            {'t':TimeUUID(5),'value':'value5'},
            {'t':TimeUUID(6),'value':'value6'},
            {'t':TimeUUID(7),'value':'value7'},
            {'t':TimeUUID(8),'value':'value8'},
            {'t':TimeUUID(9),'value':'value9'},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data)
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data.iloc[i],regs[i+1]['value'])
                self.assertEqual(data.index[i],regs[i+1]['t'])
            for i,row in enumerate(data):
                data.iloc[i] = row + '1'
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data.iloc[i],regs[i+1]['value']+'1')
                self.assertEqual(data.index[i],regs[i+1]['t'])
            data2 = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data2)
            for i,reg in enumerate(regs[1:5]):
                self.assertEqual(data2.iloc[i],regs[i+1]['value'])
                self.assertEqual(data2.index[i],regs[i+1]['t'])
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_found_in_interval_manage_dups(self):
        ''' get_metric_data should return a copy of the last data, keeping the last reg if some row is dup '''
        t = TimeUUID()
        ms = MetricStore()
        metric = Datasource('datasource.uri')
        its = TimeUUID(1, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        regs = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':5},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, regs
            tr = asyncio.Task.current_task().get_tr()
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g', tid=tr.tid))
            expected = [
                {'t':TimeUUID(1, random=False),'value':1},
                {'t':TimeUUID(2, random=False),'value':2},
                {'t':TimeUUID(3, random=False),'value':3},
                {'t':TimeUUID(4, random=False),'value':4},
                {'t':TimeUUID(5, random=False),'value':5},
            ]
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data)
            self.assertEqual(data.name, metric)
            for i,reg in enumerate(expected):
                self.assertEqual(data.iloc[i],reg['value'])
                self.assertEqual(data.index[i],reg['t'])
        async with Transaction(t) as tr:
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(metric in ms._tr_dfs[tr.tid])
            self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(regs))

    @test.sync(loop)
    async def test_get_metric_data_in_tr_some_data_in_df_and_other_in_tr_df_manage_dups(self):
        ''' get_metric_data should return a copy of the last data, keeping the last reg if some row is dup '''
        ms = MetricStore()
        metric = Datasource('datasource.uri')
        its = TimeUUID(1, lowest=True)
        ets = TimeUUID(5, highest=True)
        count = None
        df_regs = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':5},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
        ]
        tr_df_regs = [
            {'t':TimeUUID(1, random=False),'value':10},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':50},
            {'t':TimeUUID(2, random=False),'value':20},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':30},
            {'t':TimeUUID(4, random=False),'value':40},
        ]
        for reg in df_regs:
            # df regs will be available because are inserted before transaction is created
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        t = TimeUUID()
        tr = Transaction(t)
        for reg in tr_df_regs:
            # transaction regs will be available in the transaction
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic(), op='g',tid=tr.tid))
        # this regs are not going to be available to the transaction because are inserted after the tr is created
        regs = [
            {'t':TimeUUID(1, random=False),'value':100},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':500},
            {'t':TimeUUID(2, random=False),'value':200},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':300},
            {'t':TimeUUID(4, random=False),'value':400},
        ]
        async def f():
            nonlocal ms, metric, its, ets, count, tr, regs
            for reg in regs:
                self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
            expected = [
                {'t':TimeUUID(1, random=False),'value':10},
                {'t':TimeUUID(2, random=False),'value':20},
                {'t':TimeUUID(3, random=False),'value':30},
                {'t':TimeUUID(4, random=False),'value':40},
                {'t':TimeUUID(5, random=False),'value':50},
            ]
            data = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data)
            self.assertEqual(data.name, metric)
            for i,reg in enumerate(expected):
                self.assertEqual(data.iloc[i],reg['value'])
                self.assertEqual(data.index[i],reg['t'])
            for i,row in enumerate(data):
                data.iloc[i] = row + 1
            for i,reg in enumerate(expected):
                self.assertEqual(data.iloc[i],reg['value']+1)
                self.assertEqual(data.index[i],reg['t'])
            data2 = ms._get_metric_data(metric, its, ets, count)
            self.assertIsNotNone(data2)
            for i,reg in enumerate(expected):
                self.assertEqual(data2.iloc[i],reg['value'])
                self.assertEqual(data2.index[i],reg['t'])
        await TransactionTask(coro=f(), tr=tr)
        self.assertTrue(tr.tid in ms._tr_dfs)
        self.assertTrue(metric in ms._tr_dfs[tr.tid])
        self.assertEqual(len(ms._tr_dfs[tr.tid][metric]),len(tr_df_regs))
        self.assertEqual(len(ms._dfs[metric]),len(df_regs)+len(regs))
        # out of the transaction, we should get the last snapshot of the data
        expected = [
            {'t':TimeUUID(1, random=False),'value':100},
            {'t':TimeUUID(2, random=False),'value':200},
            {'t':TimeUUID(3, random=False),'value':300},
            {'t':TimeUUID(4, random=False),'value':400},
            {'t':TimeUUID(5, random=False),'value':500},
        ]
        data = ms._get_metric_data(metric, its, ets, count)
        self.assertIsNotNone(data)
        self.assertEqual(data.name, metric)
        for i,reg in enumerate(expected):
            self.assertEqual(data.iloc[i],reg['value'])
            self.assertEqual(data.index[i],reg['t'])

    @test.sync(loop)
    async def test_hook_failure_not_hooked(self):
        ''' if hook fails, no data nor range should be added '''
        try:
            ms = MetricStore()
            metric = Datapoint('uri')
            t = TimeUUID()
            bck = prproc.hook_to_metric
            prproc.hook_to_metric = test.AsyncMock(return_value = {'hooked':False,'exists':False})
            ms.get = test.AsyncMock(return_value = None)
            ms._add_synced_range = Mock(return_value = None)
            result = await ms.hook(metric)
            self.assertEqual(result, {'hooked':False, 'exists':False})
            ms.get.assert_not_called()
            ms._add_synced_range.assert_not_called()
            prproc.hook_to_metric = bck
        except:
            prproc.hook_to_metric = bck
            raise

    @test.sync(loop)
    async def test_hook_succeed_metric_does_not_exist(self):
        ''' if hook succeed and metric does not exist yet, the synced range is all available '''
        try:
            ms = MetricStore()
            metric = Datapoint('uri')
            t = TimeUUID()
            bck = prproc.hook_to_metric
            prproc.hook_to_metric = test.AsyncMock(return_value = {'hooked':True,'exists':False})
            ms.get = test.AsyncMock(return_value = None)
            ms._add_synced_range = Mock(return_value = None)
            result = await ms.hook(metric)
            self.assertEqual(result, {'hooked':True, 'exists':False})
            ms.get.assert_not_called()
            self.assertEqual(ms._add_synced_range.call_count,1)
            self.assertEqual(ms._add_synced_range.call_args[0][0], metric)
            self.assertEqual(ms._add_synced_range.call_args[1]['its'], MIN_TIMEUUID)
            self.assertEqual(ms._add_synced_range.call_args[1]['ets'], MAX_TIMEUUID)
            prproc.hook_to_metric = bck
        except:
            prproc.hook_to_metric = bck
            raise

    @test.sync(loop)
    async def test_hook_succeed_metric_exists(self):
        ''' if hook succeed and metric exists we should try to sync a range in the future '''
        try:
            ms = MetricStore()
            metric = Datapoint('uri')
            t = TimeUUID()
            bck = prproc.hook_to_metric
            prproc.hook_to_metric = test.AsyncMock(return_value = {'hooked':True,'exists':True})
            ms.get = test.AsyncMock(return_value = None)
            ms._add_synced_range = Mock(return_value = None)
            result = await ms.hook(metric)
            self.assertEqual(result, {'hooked':True, 'exists':True})
            ms._add_synced_range.assert_not_called()
            self.assertEqual(ms.get.call_count,1)
            self.assertEqual(ms.get.call_args[0][0], metric)
            self.assertEqual(ms.get.call_args[1]['end'], MAX_TIMEUUID)
            self.assertEqual(ms.get.call_args[1]['count'], 200)
            prproc.hook_to_metric = bck
        except:
            prproc.hook_to_metric = bck
            raise

    def test_is_in_failure_metric_not_in_dfs(self):
        ''' is_in should return False if metric is not in the store '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        t = TimeUUID()
        value = decimal.Decimal('3')
        self.assertFalse(ms.is_in(metric, t, value))

    def test_is_in_failure_t_not_in_store(self):
        ''' is_in should return False if (metric,t) is not in the store '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        t = TimeUUID()
        value = decimal.Decimal('3')
        regs = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':5},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        self.assertFalse(ms.is_in(metric, t, value))

    def test_is_in_failure_value_not_in_store(self):
        ''' is_in should return False if (metric,t,value) is not in the store '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        value = decimal.Decimal('3')
        regs = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':5},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        self.assertFalse(ms.is_in(metric, regs[0]['t'], value))

    def test_is_in_success_data_in_store(self):
        ''' is_in should return True if (metric,t,value) is in the store '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        regs = [
            {'t':TimeUUID(1, random=False),'value':decimal.Decimal(1)},
            {'t':TimeUUID(2, random=False),'value':decimal.Decimal(12)},
            {'t':TimeUUID(3, random=False),'value':decimal.Decimal(23)},
            {'t':TimeUUID(2, random=False),'value':decimal.Decimal(14)},
            {'t':TimeUUID(5, random=False),'value':decimal.Decimal(5)},
            {'t':TimeUUID(2, random=False),'value':"some text"},
            {'t':TimeUUID(4, random=False),'value':decimal.Decimal(94)},
            {'t':TimeUUID(3, random=False),'value':decimal.Decimal(1.1)},
            {'t':TimeUUID(4, random=False),'value':decimal.Decimal("4")},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        # is_in() only checks the last value, if the last one is equal returns False, else True
        self.assertTrue(ms.is_in(metric, regs[0]['t'], regs[0]['value']))
        self.assertFalse(ms.is_in(metric, regs[1]['t'], regs[1]['value']))
        self.assertFalse(ms.is_in(metric, regs[2]['t'], regs[2]['value']))
        self.assertFalse(ms.is_in(metric, regs[3]['t'], regs[3]['value']))
        self.assertTrue(ms.is_in(metric, regs[4]['t'], regs[4]['value']))
        self.assertTrue(ms.is_in(metric, regs[5]['t'], regs[5]['value']))
        self.assertFalse(ms.is_in(metric, regs[6]['t'], regs[6]['value']))
        self.assertTrue(ms.is_in(metric, regs[7]['t'], regs[7]['value']))
        self.assertTrue(ms.is_in(metric, regs[8]['t'], regs[8]['value']))

    def test_has_updates_failure_metric_not_in_dfs(self):
        ''' has_updates should return False if metric is not in the store '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        t = TimeUUID()
        tm = time.monotonic()
        self.assertFalse(ms.has_updates(metric, t, tm))

    def test_has_updates_failure_t_not_in_store(self):
        ''' has_updates should return False if (metric,t) is not in the store '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        t = TimeUUID()
        tm = time.monotonic()
        regs = [
            {'t':TimeUUID(1, random=False),'value':1},
            {'t':TimeUUID(2, random=False),'value':12},
            {'t':TimeUUID(3, random=False),'value':23},
            {'t':TimeUUID(2, random=False),'value':14},
            {'t':TimeUUID(5, random=False),'value':5},
            {'t':TimeUUID(2, random=False),'value':2},
            {'t':TimeUUID(4, random=False),'value':94},
            {'t':TimeUUID(3, random=False),'value':3},
            {'t':TimeUUID(4, random=False),'value':4},
        ]
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        self.assertFalse(ms.has_updates(metric, t, tm))

    def test_has_updates_success_metric_has_been_updated(self):
        ''' has_updates should return True if (metric,t) has rows newer than tm '''
        ms = MetricStore()
        metric = Datapoint('dp.uri')
        regs = [
            {'t':TimeUUID(1, random=False),'value':decimal.Decimal(1)},
            {'t':TimeUUID(2, random=False),'value':decimal.Decimal(12)},
            {'t':TimeUUID(3, random=False),'value':decimal.Decimal(23)},
            {'t':TimeUUID(2, random=False),'value':decimal.Decimal(14)},
            {'t':TimeUUID(5, random=False),'value':decimal.Decimal(5)},
            {'t':TimeUUID(2, random=False),'value':"some text"},
            {'t':TimeUUID(4, random=False),'value':decimal.Decimal(94)},
            {'t':TimeUUID(3, random=False),'value':decimal.Decimal(1.1)},
            {'t':TimeUUID(4, random=False),'value':decimal.Decimal("4")},
        ]
        tm_before = time.monotonic()
        for reg in regs:
            self.assertIsNone(ms._store(metric, reg['t'], reg['value'], tm=time.monotonic()))
        tm_after = time.monotonic()
        # is_in() only checks the last value, if the last one is equal returns False, else True
        for reg in regs:
            self.assertTrue(ms.has_updates(metric, reg['t'], tm=tm_before))
            self.assertFalse(ms.has_updates(metric, reg['t'], tm=tm_after))

    @test.sync(loop)
    async def test_tr_discard_tr_does_not_exist(self):
        ''' tr_discard should try to delete the transaction regs, existing or not '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        tr = Transaction(t)
        self.assertIsNone(ms._tr_discard(tr))
        self.assertFalse(tr.tid in ms._tr_dfs)
        self.assertFalse(tr.tid in ms._tr_synced_ranges)

    @test.sync(loop)
    async def test_tr_discard_tr_exists(self):
        ''' tr_discard should try to delete the transaction regs, existing or not '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        tr = Transaction(t)
        tr2 = Transaction(t)
        ms._tr_dfs[tr.tid]='whatever'
        ms._tr_synced_ranges[tr.tid]='whatever'
        ms._tr_dfs[tr2.tid]='whatever'
        ms._tr_synced_ranges[tr2.tid]='whatever'
        self.assertTrue(tr.tid in ms._tr_dfs)
        self.assertTrue(tr.tid in ms._tr_synced_ranges)
        self.assertTrue(tr2.tid in ms._tr_dfs)
        self.assertTrue(tr2.tid in ms._tr_synced_ranges)
        self.assertIsNone(ms._tr_discard(tr))
        self.assertFalse(tr.tid in ms._tr_dfs)
        self.assertFalse(tr.tid in ms._tr_synced_ranges)
        self.assertTrue(tr2.tid in ms._tr_dfs)
        self.assertTrue(tr2.tid in ms._tr_synced_ranges)
        self.assertIsNone(ms._tr_discard(tr2))
        self.assertFalse(tr.tid in ms._tr_dfs)
        self.assertFalse(tr.tid in ms._tr_synced_ranges)
        self.assertFalse(tr2.tid in ms._tr_dfs)
        self.assertFalse(tr2.tid in ms._tr_synced_ranges)

    @test.sync(loop)
    async def test_tr_commit_tr_does_not_exist(self):
        ''' tr_commit should do nothing if transaction does not exist '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        ms.has_updates= Mock(return_value = False)
        ms._store = Mock(return_value = None)
        ms._add_synced_range = test.AsyncMock(return_value = None)
        tr = Transaction(t)
        self.assertIsNone(await ms._tr_commit(tr))
        ms.has_updates.assert_not_called()
        ms._store.assert_not_called()
        ms._add_synced_range.assert_not_called()

    @test.sync(loop)
    async def test_tr_commit_tr_exists_but_no_data(self):
        ''' tr_commit should do nothing if transaction has no tmp data '''
        ms = MetricStore()
        metric = Datapoint('uri')
        t = TimeUUID()
        ms.has_updates= Mock(return_value = False)
        ms._store = Mock(return_value = None)
        ms._add_synced_range = test.AsyncMock(return_value = None)
        tr = Transaction(t)
        ms._tr_dfs[tr.tid]={}
        ms._tr_synced_ranges[tr.tid]={}
        self.assertIsNone(await ms._tr_commit(tr))
        ms.has_updates.assert_not_called()
        ms._store.assert_not_called()
        ms._add_synced_range.assert_not_called()

    @test.sync(loop)
    async def test_tr_commit_tr_exists_some_data(self):
        ''' tr_commit should write data to store and send samples to Komlog '''
        try:
            bck = prproc.send_samples
            prproc.send_samples = test.AsyncMock(return_value = {'success':True})
            ms = MetricStore()
            metrics = [Datapoint(uri=str(i)) for i in range(1,10)]
            for metric in metrics:
                ms._hooked.add(metric)
            tr_df_inserts = [{'metric':metric, 't':TimeUUID(),'value':int(metric.uri)+10} for metric in metrics]
            tr_df_gets = [{'metric':metric, 't':TimeUUID(),'value':int(metric.uri)+20} for metric in metrics]
            t = TimeUUID()
            tr = Transaction(t)
            async def f():
                nonlocal ms, tr, tr_df_inserts, tr_df_gets
                for reg in tr_df_inserts:
                    ms.insert(reg['metric'], reg['t'], reg['value'])
                for reg in tr_df_gets:
                    self.assertIsNone(ms._store(reg['metric'], reg['t'], reg['value'], tm=time.monotonic(), op='g',tid=tr.tid))
                    self.assertIsNone(ms._add_synced_range(reg['metric'], time.monotonic(), MIN_TIMEUUID, MAX_TIMEUUID, tr.tid))
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid].keys()),len(tr_df_gets))
            self.assertEqual(len(ms._tr_dfs[tr.tid].keys()),len(tr_df_gets))
            for reg in tr_df_gets:
                self.assertFalse(reg['metric'] in ms._dfs)
                self.assertFalse(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._tr_synced_ranges[tr.tid][reg['metric']]),1)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertIsNone(await ms._tr_commit(tr))
            for reg in tr_df_gets:
                self.assertTrue(reg['metric'] in ms._dfs)
                self.assertTrue(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._dfs[reg['metric']]),1)
                self.assertEqual(len(ms._synced_ranges[reg['metric']]),1)
                self.assertEqual(ms._dfs[reg['metric']].iloc[0].value, reg['value'])
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertEqual(prproc.send_samples.call_count,1)
            self.assertEqual(len(prproc.send_samples.call_args[0][0]), len([Sample(r['metric'],r['t'],r['value']) for r in tr_df_inserts]))
            prproc.send_samples = bck
        except:
            prproc.send_samples = bck
            raise

    @test.sync(loop)
    async def test_tr_commit_tr_exists_some_data_remove_duplicates(self):
        ''' tr_commit should write data to store and send samples to Komlog avoiding duplicates '''
        try:
            bck = prproc.send_samples
            prproc.send_samples = test.AsyncMock(return_value = {'success':True})
            ms = MetricStore()
            metrics = [Datapoint(uri=str(i)) for i in range(1,10)]
            for metric in metrics:
                ms._hooked.add(metric)
            tr_df_inserts = [{'metric':metric, 't':TimeUUID(t=int(metric.uri), random=False),'value':int(metric.uri)+10} for metric in metrics]
            tr_df_gets = [{'metric':metric, 't':TimeUUID(t=int(metric.uri), random=False),'value':int(metric.uri)+20} for metric in metrics]
            tr_df_inserts_f = [{'metric':metric, 't':TimeUUID(t=int(metric.uri), random=False),'value':int(metric.uri)+50} for metric in metrics]
            tr_df_gets_f = [{'metric':metric, 't':TimeUUID(t=int(metric.uri), random=False),'value':int(metric.uri)+60} for metric in metrics]
            t = TimeUUID()
            tr = Transaction(t)
            async def f():
                nonlocal ms, tr, tr_df_inserts, tr_df_gets
                for reg in tr_df_inserts:
                    ms.insert(reg['metric'], reg['t'], reg['value'])
                for reg in tr_df_inserts_f:
                    ms.insert(reg['metric'], reg['t'], reg['value'])
                for reg in tr_df_gets:
                    self.assertIsNone(ms._store(reg['metric'], reg['t'], reg['value'], tm=time.monotonic(), op='g',tid=tr.tid))
                    self.assertIsNone(ms._add_synced_range(reg['metric'], time.monotonic(), MIN_TIMEUUID, MAX_TIMEUUID, tr.tid))
                for reg in tr_df_gets_f:
                    self.assertIsNone(ms._store(reg['metric'], reg['t'], reg['value'], tm=time.monotonic(), op='g',tid=tr.tid))
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid].keys()),len(tr_df_gets_f))
            self.assertEqual(len(ms._tr_dfs[tr.tid].keys()),len(tr_df_gets))
            for reg in tr_df_gets:
                self.assertFalse(reg['metric'] in ms._dfs)
                self.assertFalse(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),4)
                self.assertEqual(len(ms._tr_synced_ranges[tr.tid][reg['metric']]),1)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertIsNone(await ms._tr_commit(tr))
            for reg in tr_df_gets_f:
                self.assertTrue(reg['metric'] in ms._dfs)
                self.assertTrue(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),4)
                self.assertEqual(len(ms._dfs[reg['metric']]),1)
                self.assertEqual(len(ms._synced_ranges[reg['metric']]),1)
                self.assertEqual(ms._dfs[reg['metric']].iloc[0].value, reg['value'])
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertEqual(prproc.send_samples.call_count,1)
            expected_send = [Sample(r['metric'],r['t'],r['value']) for r in tr_df_inserts_f]
            self.assertEqual(len(prproc.send_samples.call_args[0][0]), len(expected_send))
            samples_sent = prproc.send_samples.call_args[0][0]
            samples_sent = sorted(samples_sent, key = lambda x: x.metric.uri)
            for i,reg in enumerate(tr_df_inserts_f):
                self.assertEqual(reg['metric'],samples_sent[i].metric)
                self.assertEqual(reg['t'],samples_sent[i].t)
                self.assertEqual(reg['value'],samples_sent[i].value)
            prproc.send_samples = bck
        except:
            prproc.send_samples = bck
            raise

    @test.sync(loop)
    async def test_tr_commit_tr_exists_some_data_and_send_info_too(self):
        ''' tr_commit should write data to store and send samples to Komlog, including ds info '''
        try:
            bck_send_samples = prproc.send_samples
            bck_send_info = prproc.send_info
            prproc.send_samples = test.AsyncMock(return_value = {'success':True})
            prproc.send_info = test.AsyncMock(return_value = {'success':True})
            ms = MetricStore()
            metrics = [Datasource(uri=str(i),supplies=['var.'+str(i)]) for i in range(1,10)]
            for metric in metrics:
                ms._hooked.add(metric)
            tr_df_inserts = [{'metric':metric, 't':TimeUUID(),'value':metric.uri+'i'} for metric in metrics]
            tr_df_gets = [{'metric':metric, 't':TimeUUID(),'value':metric.uri+'g'} for metric in metrics]
            t = TimeUUID()
            tr = Transaction(t)
            async def f():
                nonlocal ms, tr, tr_df_inserts, tr_df_gets
                for reg in tr_df_inserts:
                    ms.insert(reg['metric'], reg['t'], reg['value'])
                for reg in tr_df_gets:
                    self.assertIsNone(ms._store(reg['metric'], reg['t'], reg['value'], tm=time.monotonic(), op='g',tid=tr.tid))
                    self.assertIsNone(ms._add_synced_range(reg['metric'], time.monotonic(), MIN_TIMEUUID, MAX_TIMEUUID, tr.tid))
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid].keys()),len(tr_df_gets))
            self.assertEqual(len(ms._tr_dfs[tr.tid].keys()),len(tr_df_gets))
            for reg in tr_df_gets:
                self.assertFalse(reg['metric'] in ms._dfs)
                self.assertFalse(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._tr_synced_ranges[tr.tid][reg['metric']]),1)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertIsNone(await ms._tr_commit(tr))
            for reg in tr_df_gets:
                self.assertTrue(reg['metric'] in ms._dfs)
                self.assertTrue(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._dfs[reg['metric']]),1)
                self.assertEqual(len(ms._synced_ranges[reg['metric']]),1)
                self.assertEqual(ms._dfs[reg['metric']].iloc[0].value, reg['value'])
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertEqual(prproc.send_samples.call_count,1)
            self.assertEqual(len(prproc.send_samples.call_args[0][0]), len([Sample(r['metric'],r['t'],r['value']) for r in tr_df_inserts]))
            self.assertEqual(prproc.send_info.call_count,1)
            for r in tr_df_inserts:
                self.assertTrue(r['metric'] in prproc.send_info.call_args[0][0])
            prproc.send_samples = bck_send_samples
            prproc.send_info = bck_send_info
        except:
            prproc.send_samples = bck_send_samples
            prproc.send_info = bck_send_info
            raise

    @test.sync(loop)
    async def test_tr_commit_tr_exists_some_data_and_send_info_too_only_missing(self):
        ''' tr_commit should write data to store and send samples to Komlog, including ds info for missing or modified '''
        try:
            bck_send_samples = prproc.send_samples
            bck_send_info = prproc.send_info
            prproc.send_samples = test.AsyncMock(return_value = {'success':True})
            prproc.send_info = test.AsyncMock(return_value = {'success':True})
            ms = MetricStore()
            metrics = [Datasource(uri=str(i),supplies=['var.'+str(i)]) for i in range(1,10)]
            for metric in metrics:
                ms._hooked.add(metric)
            tr_df_inserts = [{'metric':metric, 't':TimeUUID(),'value':metric.uri+'i'} for metric in metrics]
            tr_df_gets = [{'metric':metric, 't':TimeUUID(),'value':metric.uri+'g'} for metric in metrics]
            t = TimeUUID()
            tr = Transaction(t)
            async def f():
                nonlocal ms, tr, tr_df_inserts, tr_df_gets
                for reg in tr_df_inserts:
                    ms.insert(reg['metric'], reg['t'], reg['value'])
                for reg in tr_df_gets:
                    self.assertIsNone(ms._store(reg['metric'], reg['t'], reg['value'], tm=time.monotonic(), op='g',tid=tr.tid))
                    self.assertIsNone(ms._add_synced_range(reg['metric'], time.monotonic(), MIN_TIMEUUID, MAX_TIMEUUID, tr.tid))
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid].keys()),len(tr_df_gets))
            self.assertEqual(len(ms._tr_dfs[tr.tid].keys()),len(tr_df_gets))
            for reg in tr_df_gets:
                self.assertFalse(reg['metric'] in ms._dfs)
                self.assertFalse(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._tr_synced_ranges[tr.tid][reg['metric']]),1)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertIsNone(await ms._tr_commit(tr))
            for reg in tr_df_gets:
                self.assertTrue(reg['metric'] in ms._dfs)
                self.assertTrue(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._dfs[reg['metric']]),1)
                self.assertEqual(len(ms._synced_ranges[reg['metric']]),1)
                self.assertEqual(ms._dfs[reg['metric']].iloc[0].value, reg['value'])
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertEqual(prproc.send_samples.call_count,1)
            self.assertEqual(len(prproc.send_samples.call_args[0][0]), len([Sample(r['metric'],r['t'],r['value']) for r in tr_df_inserts]))
            self.assertEqual(prproc.send_info.call_count,1)
            for r in tr_df_inserts:
                self.assertTrue(r['metric'] in prproc.send_info.call_args[0][0])
            prproc.send_samples.reset_mock()
            prproc.send_info.reset_mock()
            #another transaction that modifies some supplies
            metrics = [Datasource(uri=str(i),supplies=['var.'+str(i),'something']) for i in range(1,10)]
            # we want to update only the last 5 metrics
            for i,m in enumerate(metrics):
                if i <5:
                    m.supplies = None
            tr_df_inserts = [{'metric':metric, 't':TimeUUID(),'value':metric.uri+'i'} for metric in metrics]
            tr_df_gets = [{'metric':metric, 't':TimeUUID(),'value':metric.uri+'g'} for metric in metrics]
            t = TimeUUID()
            tr = Transaction(t)
            await TransactionTask(coro=f(), tr=tr)
            self.assertTrue(tr.tid in ms._tr_dfs)
            self.assertTrue(tr.tid in ms._tr_synced_ranges)
            self.assertEqual(len(ms._tr_synced_ranges[tr.tid].keys()),len(tr_df_gets))
            self.assertEqual(len(ms._tr_dfs[tr.tid].keys()),len(tr_df_gets))
            for reg in tr_df_gets:
                self.assertTrue(reg['metric'] in ms._dfs)
                self.assertTrue(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._tr_synced_ranges[tr.tid][reg['metric']]),1)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._tr_synced_ranges[tr.tid][reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertIsNone(await ms._tr_commit(tr))
            for reg in tr_df_gets:
                self.assertTrue(reg['metric'] in ms._dfs)
                self.assertTrue(reg['metric'] in ms._synced_ranges)
                self.assertTrue(reg['metric'] in ms._tr_dfs[tr.tid])
                self.assertTrue(reg['metric'] in ms._tr_synced_ranges[tr.tid])
                self.assertEqual(len(ms._tr_dfs[tr.tid][reg['metric']]),2)
                self.assertEqual(len(ms._dfs[reg['metric']]),2)
                self.assertEqual(len(ms._synced_ranges[reg['metric']]),1)
                self.assertEqual(ms._dfs[reg['metric']].iloc[0].value, reg['value'])
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['its'],MIN_TIMEUUID)
                self.assertEqual(ms._synced_ranges[reg['metric']][0]['ets'],MAX_TIMEUUID)
            self.assertEqual(prproc.send_samples.call_count,1)
            self.assertEqual(len(prproc.send_samples.call_args[0][0]), len([Sample(r['metric'],r['t'],r['value']) for r in tr_df_inserts]))
            self.assertEqual(prproc.send_info.call_count,1)
            for i,m in enumerate(metrics):
                if i<5:
                    self.assertTrue(m not in prproc.send_info.call_args[0][0])
                else:
                    self.assertTrue(m in prproc.send_info.call_args[0][0])
            prproc.send_samples = bck_send_samples
            prproc.send_info = bck_send_info
        except:
            prproc.send_samples = bck_send_samples
            prproc.send_info = bck_send_info
            raise


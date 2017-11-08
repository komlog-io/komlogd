import asyncio
import unittest
import uuid
import pandas as pd
import time
from komlogd.api import transfer_methods
from komlogd.api.common import timeuuid
from komlogd.api.model import test
from komlogd.api.model.metrics import Datasource, Datapoint
from komlogd.api.model.schedules import OnUpdateSchedule, CronSchedule
from komlogd.api.model.transfer_methods import TransferMethodsIndex

loop = asyncio.get_event_loop()

def noop():
    pass

class ApiModelTransferMethodsTest(unittest.TestCase):

    def tearDown(self):
        [task.cancel() for task in asyncio.Task.all_tasks()]

    def test_creation_TransferMethodsIndex_object(self):
        ''' test creating a TransferMethodsIndex object '''
        tmi = TransferMethodsIndex()
        self.assertEqual(tmi._enabled_methods, {})
        self.assertEqual(tmi._disabled_methods, {})

    def test_add_tm_success(self):
        ''' add_tm should add the transfermethod to the tmIndex, it will try to enable it '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)

    def test_add_tm_failure_already_added(self):
        ''' add_tm should fail if we try to add an already existing tm '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertFalse(tmi.add_tm(tm))

    @test.sync(loop)
    async def test_enable_tm_failure_non_existing_mid(self):
        ''' enable_tm should fail if mid does not exist '''
        tmi = TransferMethodsIndex()
        mid = uuid.uuid4()
        result = await tmi.enable_tm(mid)
        self.assertFalse(result)

    @test.sync(loop)
    async def test_enable_tm_failure_already_enabled(self):
        ''' enable_tm should fail if tm is already enabled '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertFalse(await tmi.enable_tm(tm.mid))

    @test.sync(loop)
    async def test_enable_tm_success(self):
        ''' enable_tm should enable the tm and set the enabling date '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm.mid]['first'])

    @test.sync(loop)
    async def test_enable_tm_failure_cannot_hook_metric(self):
        ''' enable_tm should fail if we cannot hook to a metric.It should generate a retry task '''
        tm = transfer_methods.transfermethod(f=noop, schedule=OnUpdateSchedule(Datasource('uri')))
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertFalse(await tmi.enable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertIsNone(tmi._disabled_methods[tm.mid]['first'])
        current_task = asyncio.Task.current_task()
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),2) #this task and retry task
        [task.cancel() for task in asyncio.Task.all_tasks() if task != current_task]

    @test.sync(loop)
    async def test_disable_tm_success(self):
        ''' disable_tm should disable the tm '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm.mid]['first'])
        self.assertTrue(tmi.disable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertTrue(tm.mid in tmi._disabled_methods)

    @test.sync(loop)
    async def test_disable_tm_failure_already_disabled(self):
        ''' disable_tm should fail if tm is already disabled '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm.mid]['first'])
        self.assertTrue(tmi.disable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertFalse(tmi.disable_tm(tm.mid))

    def test_disable_tm_failure_non_existing_mid(self):
        ''' disable_tm should fail if mid does not exist '''
        mid = uuid.uuid4()
        tmi = TransferMethodsIndex()
        self.assertFalse(tmi.disable_tm(mid))

    @test.sync(loop)
    async def test_delete_tm_success_previously_enabled(self):
        ''' delete_tm should delete a previously enabled tm '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm.mid]['first'])
        self.assertTrue(tmi.delete_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertFalse(tm.mid in tmi._disabled_methods)
        self.assertIsNone(tmi.get_tm_info(tm.mid))

    @test.sync(loop)
    async def test_delete_tm_success_previously_disabled(self):
        ''' delete_tm should delete a previously disabled tm '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm.mid]['first'])
        self.assertTrue(tmi.disable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(tmi.delete_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertFalse(tm.mid in tmi._disabled_methods)
        self.assertIsNone(tmi.get_tm_info(tm.mid))

    @test.sync(loop)
    async def test_delete_tm_success_previously_deleted(self):
        ''' delete_tm should return True if tm did not exist already '''
        tm = transfer_methods.transfermethod(f=noop)
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_tm(tm.mid))
        self.assertTrue(tm.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm.mid]['first'])
        self.assertTrue(tmi.disable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertTrue(tmi.delete_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertFalse(tm.mid in tmi._disabled_methods)
        self.assertIsNone(tmi.get_tm_info(tm.mid))
        self.assertTrue(tmi.delete_tm(tm.mid))

    @test.sync(loop)
    async def test_enable_all_success(self):
        ''' enable_all should enable all disabled transfer methods '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])

    @test.sync(loop)
    async def test_disable_all_success(self):
        ''' disable_all should disable all enabled transfer methods '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        self.assertTrue(tmi.disable_all())
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertFalse(tm1.mid in tmi._enabled_methods)
        self.assertFalse(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._disabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._disabled_methods[tm2.mid]['first'])

    @test.sync(loop)
    async def test_get_tm_info_success(self):
        ''' get_tm_info should return information about the tm '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        tm1_info = tmi.get_tm_info(tm1.mid)
        self.assertEqual(tm1_info['enabled'], True)
        self.assertEqual(tm1_info['tm'], tm1)
        tm2_info = tmi.get_tm_info(tm2.mid)
        self.assertEqual(tm2_info['enabled'], True)
        self.assertEqual(tm2_info['tm'], tm2)
        self.assertTrue(tmi.disable_all())
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertFalse(tm1.mid in tmi._enabled_methods)
        self.assertFalse(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._disabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._disabled_methods[tm2.mid]['first'])
        tm1_info = tmi.get_tm_info(tm1.mid)
        self.assertEqual(tm1_info['enabled'],False)
        self.assertEqual(tm1_info['tm'], tm1)
        tm2_info = tmi.get_tm_info(tm2.mid)
        self.assertEqual(tm2_info['enabled'],False)
        self.assertEqual(tm2_info['tm'], tm2)

    @test.sync(loop)
    async def test_metrics_updated_no_tm_activated_with_them(self):
        ''' metrics_updated should not generate any task if no tm is activated with those mets '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        metrics = [Datasource('uri1'),Datasource('uri2')]
        t = timeuuid.TimeUUID()
        current_task = asyncio.Task.current_task()
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),1) #this task
        tmi.metrics_updated(t=t, metrics=metrics, irt=timeuuid.TimeUUID())
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),1) #No new task added
        [task.cancel() for task in asyncio.Task.all_tasks() if task != current_task]

    @test.sync(loop)
    async def test_metrics_updated_one_tm_activated_with_them(self):
        ''' metrics_updated should generate one new task '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        tm1.schedule = OnUpdateSchedule(activation_metrics=Datasource('uri'))
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        metrics = [Datasource('uri'),Datasource('uri2')]
        t = timeuuid.TimeUUID()
        current_task = asyncio.Task.current_task()
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),1) #this task
        tmi.metrics_updated(t=t, metrics=metrics, irt=timeuuid.TimeUUID())
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),2) #One tm activated
        [task.cancel() for task in asyncio.Task.all_tasks() if task != current_task]

    @test.sync(loop)
    async def test_metrics_updated_two_tm_activated_with_them(self):
        ''' metrics_updated should generate two new tasks '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        tm1.schedule = OnUpdateSchedule(activation_metrics=Datasource('uri'))
        tm2.schedule = OnUpdateSchedule(activation_metrics=Datasource('uri'))
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        metrics = [Datasource('uri'),Datasource('uri2')]
        t = timeuuid.TimeUUID()
        current_task = asyncio.Task.current_task()
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),1) #this task
        tmi.metrics_updated(t=t, metrics=metrics, irt=timeuuid.TimeUUID())
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),3) #two tm activated
        [task.cancel() for task in asyncio.Task.all_tasks() if task != current_task]

    @test.sync(loop)
    async def test_get_tms_activated_with_none_found(self):
        ''' _get_tms_activated_with should return [] if no tm is activated with these metrics '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        metrics = [Datasource('uri'),Datasource('uri2')]
        self.assertEqual(tmi._get_tms_activated_with(metrics), [])

    @test.sync(loop)
    async def test_get_tms_activated_with_some_found(self):
        ''' _get_tms_activated_with should return the tms activated with these metrics '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        tm1.schedule = OnUpdateSchedule(activation_metrics=Datasource('uri'))
        tm2.schedule = OnUpdateSchedule(activation_metrics=Datasource('uri'))
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        metrics = [Datasource('uri'),Datasource('uri2')]
        activated = tmi._get_tms_activated_with(metrics)
        self.assertEqual(len(activated), 2)
        self.assertTrue(tm1 in activated)
        self.assertTrue(tm2 in activated)

    @test.sync(loop)
    async def test_get_tms_activated_with_some_found_no_repeat(self):
        ''' _get_tms_activated_with should return the tms activated with these metrics each only once '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        tm1.schedule = OnUpdateSchedule(activation_metrics=[Datasource('uri'),Datasource('uri2')])
        tm2.schedule = OnUpdateSchedule(activation_metrics=[Datasource('uri'),Datasource('uri2')])
        self.assertTrue(Datasource('uri') in tm1.schedule.activation_metrics)
        self.assertTrue(Datasource('uri') in tm2.schedule.activation_metrics)
        self.assertTrue(Datasource('uri2') in tm1.schedule.activation_metrics)
        self.assertTrue(Datasource('uri2') in tm2.schedule.activation_metrics)
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        metrics = [Datasource('uri'),Datasource('uri2')]
        activated = tmi._get_tms_activated_with(metrics)
        self.assertEqual(len(activated), 2)
        self.assertTrue(tm1 in activated)
        self.assertTrue(tm2 in activated)

    @test.sync(loop)
    async def test_get_tms_that_meet_none_found(self):
        ''' _get_tms_that_meet should return [] if no tm meets that schedule '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        t = time.localtime()
        self.assertEqual(tmi._get_tms_that_meet(t=t), [])

    @test.sync(loop)
    async def test_get_tms_that_meet_some_found(self):
        ''' _get_tms_that_meet should return the tms that meet the schedule '''
        tm1 = transfer_methods.transfermethod(f=noop)
        tm2 = transfer_methods.transfermethod(f=noop)
        tm1._decorate_method(tm1._f)
        tm2._decorate_method(tm2._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm1))
        self.assertTrue(tmi.add_tm(tm2))
        self.assertTrue(tm1.mid in tmi._disabled_methods)
        self.assertTrue(tm2.mid in tmi._disabled_methods)
        self.assertTrue(await tmi.enable_all())
        self.assertTrue(tm1.mid in tmi._enabled_methods)
        self.assertTrue(tm2.mid in tmi._enabled_methods)
        t = time.localtime()
        tm1.schedule = CronSchedule()
        tm1.schedule._schedule = {'minute':[t.tm_min],'hour':[t.tm_hour],'month':[t.tm_mon],'dow':[t.tm_wday],'dom':[t.tm_mday]}
        tm2.schedule = CronSchedule()
        tm2.schedule._schedule = {'minute':[t.tm_min],'hour':[t.tm_hour],'month':[t.tm_mon],'dow':[t.tm_wday],'dom':[t.tm_mday]}
        self.assertIsNotNone(tmi._enabled_methods[tm1.mid]['first'])
        self.assertIsNotNone(tmi._enabled_methods[tm2.mid]['first'])
        meet_tms = tmi._get_tms_that_meet(t=t)
        self.assertEqual(len(meet_tms), 2)
        self.assertTrue(tm1 in meet_tms)
        self.assertTrue(tm2 in meet_tms)

    @test.sync(loop)
    async def test_retry_failed_success_no_disabled_tms(self):
        ''' retry_failed should return True if no disabled tms exist '''
        tmi = TransferMethodsIndex()
        self.assertTrue(await tmi._retry_failed(sleep=1))

    @test.sync(loop)
    async def test_retry_failed_failure_cannot_hook_metric(self):
        ''' retry_failed should fail if we cannot enable a tm. It should generate a retry task '''
        tm = transfer_methods.transfermethod(f=noop, schedule=OnUpdateSchedule(Datasource('uri')))
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertFalse(await tmi.enable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertIsNone(tmi._disabled_methods[tm.mid]['first'])
        current_task = asyncio.Task.current_task()
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),3) #this task, retry task and first enable_tm task
        self.assertFalse(await tmi._retry_failed(sleep=1))
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),4) # a new retry task has been generated
        [task.cancel() for task in asyncio.Task.all_tasks() if task != current_task]

    @test.sync(loop)
    async def test_retry_failed_failure_cannot_hook_metric(self):
        ''' retry_failed should fail if we cannot enable a tm. It should generate a retry task '''
        tm = transfer_methods.transfermethod(f=noop, schedule=OnUpdateSchedule(Datasource('uri')))
        tm._decorate_method(tm._f)
        tmi = TransferMethodsIndex()
        self.assertTrue(tmi.add_tm(tm))
        self.assertTrue(tm.mid in tmi._disabled_methods)
        self.assertFalse(await tmi.enable_tm(tm.mid))
        self.assertFalse(tm.mid in tmi._enabled_methods)
        self.assertIsNone(tmi._disabled_methods[tm.mid]['first'])
        current_task = asyncio.Task.current_task()
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),2) #this task and retry task
        tm.schedule = OnUpdateSchedule()
        self.assertTrue(await tmi._retry_failed(sleep=1))
        tasks = asyncio.Task.all_tasks()
        self.assertEqual(len(tasks),2) # no new task
        [task.cancel() for task in asyncio.Task.all_tasks() if task != current_task]


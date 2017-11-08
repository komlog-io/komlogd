import asyncio
import unittest
import uuid
import pandas as pd
import time
from komlogd.api import session
from komlogd.api.common import crypto, timeuuid
from komlogd.api.model import test
from komlogd.api.model.transactions import Transaction, TransactionTask
from komlogd.api.model.session import sessionIndex

loop = asyncio.get_event_loop()

class ApiModelTransactionsTest(unittest.TestCase):

    def test_TransactionTask_has_tr_attribute(self):
        ''' check that a transactional task has a tr parameter '''
        async def function():
            pass
        @test.sync(loop)
        async def task_test():
            tr = Transaction(pd.Timestamp('now',tz='utc'))
            task = TransactionTask(coro=function(), tr=tr)
            self.assertEqual(task.get_tr(), tr)
        task_test()

    def test_TransactionTask_has_tr_attribute_None(self):
        ''' check that normal tasks has tr parameter if they run in a supported loop '''
        async def function():
            pass
        @test.sync(loop)
        async def task_test():
            task = loop.create_task(function())
            self.assertEqual(task.get_tr(), None)
        task_test()

    def test_TransactionTask_has_no_tr_attribute(self):
        ''' the loop must support TransactionTask or will fail '''
        loop2 = asyncio.new_event_loop()
        #loop.set_task_factory(lambda loop, coro: TransactionTask(coro, loop=loop))
        async def function():
            pass
        @test.sync(loop2)
        async def task_test():
            task = loop2.create_task(function())
            with self.assertRaises(AttributeError) as cm:
                self.assertEqual(task.get_tr(), None)
        task_test()
        loop2.close()

    def test_create_Transaction_object(self):
        ''' Check creation of a new Transaction object '''
        t = timeuuid.TimeUUID()
        tr = Transaction(t=t)
        self.assertTrue(tr.tm < time.monotonic())
        self.assertTrue(isinstance(tr.tid,uuid.UUID))
        self.assertEqual(tr.t, t)
        self.assertEqual(tr._dirty, set())

    @test.sync(loop)
    async def test_commit_transaction(self):
        ''' Commit transaction should execute _tr_commit coroutines for each dirty item '''
        class MyInterface:
            def __init__(self):
                self._counter = 0
            async def _tr_commit(self, tid):
                self._counter += 1
        t = timeuuid.TimeUUID()
        tr = Transaction(t=t)
        interfaces = [MyInterface() for _ in range(1,10)]
        [self.assertEqual(item._counter,0) for item in interfaces]
        [tr.add_dirty_item(item) for item in interfaces]
        await tr.commit()
        [self.assertEqual(item._counter,1) for item in interfaces]
        self.assertEqual(tr._dirty, set())

    def test_discard_transaction(self):
        ''' Discarding a transaction should execute _tr_discard functions for each dirty item '''
        class MyInterface:
            def __init__(self):
                self._counter = 0
            async def _tr_commit(self, tid):
                self._counter += 1
            def _tr_discard(self, tid):
                self._counter -= 1
        t = timeuuid.TimeUUID()
        tr = Transaction(t=t)
        interfaces = [MyInterface() for _ in range(1,10)]
        [self.assertEqual(item._counter,0) for item in interfaces]
        [tr.add_dirty_item(item) for item in interfaces]
        tr.discard()
        [self.assertEqual(item._counter,-1) for item in interfaces]
        self.assertEqual(tr._dirty, set())

    @test.sync(loop)
    async def test_discard_transaction_deletes_items_cannot_commit_after_that(self):
        ''' Discarding a transaction will remove all dirty items and disable any later commit '''
        class MyInterface:
            def __init__(self):
                self._counter = 0
            async def _tr_commit(self, tid):
                self._counter += 1
            def _tr_discard(self, tid):
                self._counter -= 1
        t = timeuuid.TimeUUID()
        tr = Transaction(t=t)
        interfaces = [MyInterface() for _ in range(1,10)]
        [self.assertEqual(item._counter,0) for item in interfaces]
        [tr.add_dirty_item(item) for item in interfaces]
        tr.discard()
        [self.assertEqual(item._counter,-1) for item in interfaces]
        self.assertEqual(tr._dirty, set())
        await tr.commit()
        [self.assertEqual(item._counter,-1) for item in interfaces]

    @test.sync(loop)
    async def test_discard_transaction_automatically_if_out_of_context(self):
        ''' at context exit, discard() will be called on the Transaction object automatically '''
        class MyInterface:
            def __init__(self):
                self._counter = 0
            def _tr_discard(self, tid):
                self._counter -= 1
        interfaces = [MyInterface() for _ in range(1,10)]
        t = timeuuid.TimeUUID()
        async with Transaction(t) as tr:
            [self.assertEqual(item._counter,0) for item in interfaces]
            [tr.add_dirty_item(item) for item in interfaces]
            self.assertNotEqual(tr._dirty, set())
        [self.assertEqual(item._counter,-1) for item in interfaces]
        self.assertEqual(tr._dirty, set())

    def test_add_dirty_item(self):
        ''' adding a dirty item should add the element to the _dirty set '''
        class MyInterface:
            def __init__(self):
                self._counter = 0
            def _tr_discard(self, tid):
                self._counter -= 1
        interfaces = [MyInterface() for _ in range(1,10)]
        t = timeuuid.TimeUUID()
        tr = Transaction(t)
        [tr.add_dirty_item(item) for item in interfaces]
        self.assertEqual(len(tr._dirty),9)
        [tr.add_dirty_item(item) for item in interfaces]
        self.assertEqual(len(tr._dirty),9)


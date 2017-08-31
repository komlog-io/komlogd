import asyncio
import uuid
import time
import sys
from komlogd.api.common import logging, exceptions


class TransactionTask(asyncio.Task):

    def __init__(self, coro, *, loop=None, tr=None):
        super().__init__(coro, loop=loop)
        if tr is None:
            ct = self.current_task(loop=self._loop)
            if ct is not None:
                tr = ct.get_tr()
        self._tr = tr

    def get_tr(self):
        return self._tr

class Transaction:
    def __init__(self, t, irt=None):
        self.tm = time.monotonic()
        self.tid = uuid.uuid4()
        self.t = t
        self.irt = irt
        self._dirty = set()

    async def __aenter__(self):
        logging.logger.debug('Entering transaction {}'.format(self.tid.hex))
        return self

    async def __aexit__(self, exc_type, exc, tb):
        logging.logger.debug('Exiting transaction {}, exc_type: {}, exc: {}, tb: {}'.format(self.tid.hex,str(exc_type), str(exc), str(tb)))
        self.discard()

    async def commit(self):
        tasks = []
        for item in self._dirty:
            tasks.append(item._tr_commit(self))
        await asyncio.gather(*tasks)
        self._dirty = set()

    def discard(self):
        for item in self._dirty:
            item._tr_discard(self)
        self._dirty = set()

    def add_dirty_item(self, item):
        self._dirty.add(item)


loop = asyncio.get_event_loop()
loop.set_task_factory(lambda loop, coro: TransactionTask(coro, loop=loop))

if sys.platform == 'win32':
    loop = asyncio.ProactorEventLoop()
    loop.set_task_factory(lambda loop, coro: TransactionTask(coro, loop=loop))
    asyncio.set_event_loop(loop)
else:
    loop = asyncio.get_event_loop()
    loop.set_task_factory(lambda loop, coro: TransactionTask(coro, loop=loop))



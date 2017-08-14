import asyncio
import traceback
from komlogd.api.common import logging

class ExitMessage:
    pass

class AsyncQueue:
    def __init__(self, num_workers, on_msg, name, loop=None):
        loop = loop or asyncio.get_event_loop()
        self._loop = loop
        self._num_workers = num_workers
        self._on_msg = on_msg
        self._name = name
        self._queue = asyncio.Queue()
        self._workers = None
        self._timeout = 0

    def start(self):
        logging.logger.debug('Starting AsyncQueue {}'.format(self._name))
        assert self._workers is None
        self._workers = [asyncio.ensure_future(self._worker_loop(i+1), loop=self._loop) for i in range(self._num_workers)]

    async def _worker_loop(self, instance):
        logging.logger.debug('Starting worker {}/{} on {} queue'.format(str(instance), str(self._num_workers), self._name))
        while True:
            new_msg = False
            try:
                item = await self._queue.get()
                new_msg = True
                if item.__class__ is ExitMessage:
                    logging.logger.debug('Stopping worker {}/{} on {} queue'.format(str(instance), str(self._num_workers),self._name))
                    break
                args, kwargs = item
                await asyncio.wait_for(self._on_msg(*args, **kwargs),self._timeout, loop=self._loop)
            except (KeyboardInterrupt, MemoryError, SystemExit):
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
                raise
            except BaseException:
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
            finally:
                if new_msg:
                    self._queue.task_done()

    async def join(self):
        if not self._workers:
            return
        for _ in range(self._num_workers):
            await self._queue.put(ExitMessage())
        try:
            await asyncio.gather(*self._workers, loop=self._loop)
            self._workers = None
        except:
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
            raise
        finally:
            logging.logger.debug('Exiting AsyncQueue {}'.format(self._name))

    async def push(self, *args,**kwargs):
        await self._queue.put((args,kwargs))
        return None


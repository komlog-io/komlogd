'''

transfermethod decorator


'''

import asyncio
import inspect
import uuid
import pandas as pd
import traceback
import weakref
from functools import wraps
from komlogd.api.common import logging, exceptions
from komlogd.api.model.metrics import Metric
from komlogd.api.model.schedules import Schedule, OnUpdateSchedule
from komlogd.api.model.transactions import Transaction, TransactionTask
from komlogd.api.model.transfer_methods import tmIndex


class transfermethod:

    def __init__(self, f=None, f_params=None, schedule=None):
        self.mid = uuid.uuid4()
        self._f = f
        self.f_params = f_params
        self.schedule = schedule

    @property
    def f_params(self):
        return self._f_params

    @f_params.setter
    def f_params(self, value):
        if value is None:
            self._f_params = {}
        elif isinstance(value, dict):
            for k in value.keys():
                if k in ['t','updated','others']:
                    raise exceptions.BadParametersException('Invalid function parameter. "{}" is a reserved parameter'.format(str(k)))
            else:
                self._f_params = value
        else:
            raise exceptions.BadParametersException('"f_params" attribute must be a dict')

    @property
    def schedule(self):
        return self._schedule

    @schedule.setter
    def schedule(self, value):
        if value is None or isinstance(value, Schedule):
            self._schedule = value
        else:
            raise exceptions.BadParametersException('Invalid "schedule" attribute')

    def _get_execution_params(self, t, metrics):
        exec_params={}
        updated = []
        others = []
        for arg in self._func_params:
            if arg == 't':
                exec_params[arg]=t
            elif arg == 'updated':
                exec_params[arg]=metrics
            elif arg == 'others':
                exec_params[arg]=[m for m in self.schedule.activation_metrics if m not in metrics]
            elif arg in self._f_params:
                exec_params[arg]=self._f_params[arg]
        return exec_params

    def _decorate_method(self, f):
        @wraps(f)
        async def decorated(t, metrics):
            now=pd.Timestamp('now',tz='utc')
            exec_params=self._get_execution_params(t=t, metrics=metrics)
            if asyncio.iscoroutinefunction(f):
                await f(**exec_params)
            else:
                f(**exec_params)
            return True
        self.f = decorated
        self._func_params = inspect.signature(f).parameters
        if self.schedule == None:
            self.schedule = OnUpdateSchedule(activation_metrics = self.f_params)

    def __call__(self, f):
        self._decorate_method(f)
        logging.logger.debug('Registering decorated transfer method '+self.mid.hex)
        if tmIndex.add_tm(tm=self):
            asyncio.ensure_future(tmIndex.enable_tm(self.mid))
        return f

    async def bind(self):
        if self._f is None:
            raise exceptions.BadParametersException('No function associated to transfermethod object')
        self._decorate_method(self._f)
        logging.logger.debug('Binding transfer method '+self.mid.hex)
        if tmIndex.add_tm(tm=weakref.proxy(self)):
            return await tmIndex.enable_tm(mid=self.mid)
        return False

    def unbind(self):
        logging.logger.debug('Unbinding transfer method '+self.mid.hex)
        tmIndex.delete_tm(self.mid)

    async def run(self, t, metrics, irt=None):
        async with Transaction(t=t, irt=irt) as tr:
            try:
                await TransactionTask(coro=self.f(t=t, metrics=metrics), tr=tr)
            except Exception:
                logging.logger.error('Error while executing tm {}. disabling it.'.format(self.mid.hex))
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
            else:
                try:
                    await tr.commit()
                except exceptions.SessionException as e:
                    logging.logger.error('Transaction could not be commited completely {}.'.format(tr.tid.hex))
                    logging.logger.error('Error: {}'.format(e.msg))

    def __del__(self):
        logging.logger.debug('Automatically unbinding transfer method '+self.mid.hex)
        self.unbind()


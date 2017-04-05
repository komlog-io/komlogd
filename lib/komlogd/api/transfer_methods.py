'''

transfermethod decorator


'''

import asyncio
import uuid
import inspect
import pandas as pd
from functools import wraps
from komlogd.api import logging, uri, exceptions
from komlogd.api.model.transfer_methods import static_transfer_methods
from komlogd.api.protocol.model import validation
from komlogd.api.protocol.model.types import Metric
from komlogd.api.protocol.model.transfer_methods import DataRequirements


class transfermethod:

    def __init__(self, uris, min_exec_delta=None, data_reqs=None, exec_on_load=False):
        self.mid = uuid.uuid4()
        self.uris=uris
        self.last_exec=None
        self.data_reqs=data_reqs
        self.metrics=[Metric(uri=uri) for uri in uris]
        self.min_exec_delta = min_exec_delta
        self.exec_on_load = exec_on_load

    @property
    def uris(self):
        return self._uris

    @uris.setter
    def uris(self, value):
        if not isinstance(value,list):
            raise exceptions.BadParametersException('Invalid uris parameter type')
        try:
            all(validation.validate_uri(uri) for uri in value)
            self._uris=value
        except TypeError:
            raise exceptions.BadParametersException('Invalid uri')

    @property
    def min_exec_delta(self):
        return self._min_exec_delta

    @min_exec_delta.setter
    def min_exec_delta(self, value):
        try:
            self._min_exec_delta = pd.Timedelta(value) if value is not None else None
        except ValueError:
            raise exceptions.BadParametersException('Invalid min_exec_delta value')

    @property
    def data_reqs(self):
        return self._data_reqs

    @data_reqs.setter
    def data_reqs(self, value):
        if value is None:
            self._data_reqs = None
            return
        elif isinstance(value, DataRequirements):
            self._data_reqs = value
            return
        elif isinstance(value, dict):
            for key,reqs in value.items():
                if not isinstance(reqs, DataRequirements):
                    break
            else:
                self._data_reqs = value
                return
        raise TypeError('Invalid data_reqs parameter')

    @property
    def exec_on_load(self):
        return self._exec_on_load

    @exec_on_load.setter
    def exec_on_load(self, value):
        self._exec_on_load = bool(value)

    def __call__(self, f):
        logging.logger.debug('registering static transfer method, f: '+f.__name__+' uris: '+str(self.uris))
        @wraps(f)
        async def decorated(*args, **kwargs):
            now=pd.Timestamp('now',tz='utc')
            if self.min_exec_delta and self.last_exec and now-self.min_exec_delta<self.last_exec:
                logging.logger.debug('min_exec_delta condition not satisfied, not executing '+f.__name__)
                return
            funcargs={}
            arg_metrics_uris=[uri.get_global_uri(metric,owner=kwargs['session']._username) for metric in kwargs['metrics']]
            for arg in self.funcargs:
                if arg == 'ts':
                    funcargs[arg]=kwargs[arg]
                elif arg == 'updated':
                    funcargs[arg]=[metric for metric in self.metrics if uri.get_global_uri(metric,owner=kwargs['session']._username) in arg_metrics_uris]
                elif arg == 'others':
                    funcargs[arg]=[metric for metric in self.metrics if uri.get_global_uri(metric,owner=kwargs['session']._username) not in arg_metrics_uris]
                elif arg == 'data':
                    data={}
                    for metric in self.metrics:
                        data[metric]=kwargs['session']._metrics_store.get_serie(metric=metric)
                    funcargs[arg]=data
            self.last_exec=pd.Timestamp('now',tz='utc')
            if asyncio.iscoroutinefunction(f):
                result = await f(**funcargs)
            else:
                result = f(**funcargs)
            if isinstance(result, dict) and 'samples' in result:
                if isinstance(result['samples'], list):
                    await kwargs['session'].send_samples(result['samples'])
                else:
                    logging.logger.error('Transfer method response unsupported, samples field should be a list of Samples')
        self.funcargs=inspect.signature(f).parameters
        self.f = decorated
        static_transfer_methods.add_transfer_method(transfer_method=self, enabled=False)
        return f


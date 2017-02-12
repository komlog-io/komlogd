'''

Functions for requesting the metrics store


'''

import asyncio
import uuid
import inspect
import pandas as pd
from functools import wraps
from komlogd.api import logging
from komlogd.api.model import impulses, orm


class impulsemethod:

    def __init__(self, uris, min_exec_delta=None, data_reqs=None):
        if not isinstance(uris, list):
            raise TypeError('uris parameter must be a list')
        self.lid = uuid.uuid4()
        self.uris=uris
        self.last_exec=None
        self.min_exec_delta=pd.Timedelta(min_exec_delta) if min_exec_delta else None
        self.data_reqs=data_reqs
        self.metrics=[orm.Metric(uri=uri) for uri in uris]

    @property
    def data_reqs(self):
        return self._data_reqs

    @data_reqs.setter
    def data_reqs(self, value):
        if value is None:
            self._data_reqs = None
            return
        elif isinstance(value, orm.DataRequirements):
            self._data_reqs = value
            return
        elif isinstance(value, dict):
            for key,reqs in value.items():
                if not isinstance(reqs,orm.DataRequirements):
                    break
            else:
                self._data_reqs = value
                return
        raise TypeError('Invalid data_reqs parameter')

    def __call__(self, f):
        logging.logger.debug('registering static impulse, f: '+f.__name__+' uris: '+str(self.uris))
        @wraps(f)
        async def decorated(*args, **kwargs):
            now=pd.Timestamp('now',tz='utc')
            if self.min_exec_delta and self.last_exec and now-self.min_exec_delta<self.last_exec:
                logging.logger.debug('min_exec_delta condition not satisfied, not executing '+f.__name__)
                return
            funcargs={}
            arg_metrics_uris=[orm.get_global_uri(metric,owner=kwargs['session'].username) for metric in kwargs['metrics']]
            for arg in self.funcargs:
                if arg == 'ts':
                    funcargs[arg]=kwargs[arg]
                elif arg == 'updated':
                    funcargs[arg]=[metric for metric in self.metrics if orm.get_global_uri(metric,owner=kwargs['session'].username) in arg_metrics_uris]
                elif arg == 'others':
                    funcargs[arg]=[metric for metric in self.metrics if orm.get_global_uri(metric,owner=kwargs['session'].username) not in arg_metrics_uris]
                elif arg == 'data':
                    data={}
                    for metric in self.metrics:
                        data[metric]=kwargs['session'].metrics_store.get_serie(metric=metric)
                    funcargs[arg]=data
            self.last_exec=pd.Timestamp('now',tz='utc')
            if asyncio.iscoroutinefunction(f):
                result = await f(**funcargs)
            else:
                result = f(**funcargs)
            if isinstance(result, dict) and 'samples' in result:
                if isinstance(result['samples'], list):
                    kwargs['session'].send_samples(result['samples'])
                else:
                    logging.logger.error('Impulse function response unsupported, samples field should be a list of Samples')
        self.funcargs=inspect.signature(f).parameters
        self.f = decorated
        impulses.static_impulses.set_impulse(impulse_method=self)
        return f


'''

transfermethod decorator


'''

import asyncio
import copy
import inspect
import uuid
import pandas as pd
from functools import wraps
from komlogd.api import logging, uri, exceptions
from komlogd.api.model.transfer_methods import static_transfer_methods
from komlogd.api.protocol.model import validation
from komlogd.api.protocol.model.types import Metric, Sample
from komlogd.api.protocol.model.transfer_methods import DataRequirements


class transfermethod:

    def __init__(self, p_in=None, p_out=None, data_reqs=None, min_exec_delta=None, exec_on_load=False, allow_loops=False):
        self.mid = uuid.uuid4()
        self.last_exec = None
        self.p_in = p_in
        self.p_out = p_out
        self.data_reqs = data_reqs
        self.min_exec_delta = min_exec_delta
        self.exec_on_load = exec_on_load
        self.allow_loops = allow_loops

    @property
    def p_in(self):
        return self._p_in

    @p_in.setter
    def p_in(self, value):
        if value is None:
            self._p_in = {}
        elif isinstance(value, dict):
            for k in value.keys():
                if k in ['ts','updated','others']:
                    raise exceptions.BadParametersException('Invalid input parameter. "{}" is a reserved parameter'.format(str(k)))
            else:
                self._p_in = value
        else:
            raise exceptions.BadParametersException('"p_in" attribute must be a dict')

    @property
    def p_out(self):
        return self._p_out

    @p_out.setter
    def p_out(self, value):
        if value is None:
            self._p_out = {}
        elif isinstance(value, dict):
            for k in value.keys():
                if k in ['ts','updated','others']:
                    raise exceptions.BadParametersException('Invalid output parameter. "{}" is a reserved parameter'.format(str(k)))
            else:
                self._p_out = value
        else:
            raise exceptions.BadParametersException('"p_out" attribute must be a dict')

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
        raise exceptions.BadParametersException('Invalid data_reqs parameter')

    @property
    def exec_on_load(self):
        return self._exec_on_load

    @exec_on_load.setter
    def exec_on_load(self, value):
        self._exec_on_load = bool(value)

    @property
    def allow_loops(self):
        return self._allow_loops

    @allow_loops.setter
    def allow_loops(self, value):
        self._allow_loops = bool(value)

    @property
    def m_in(self):
        return self._m_in

    def _inspect_input_params(self):
        self._p_in_routes = {}
        for k,o in self._p_in.items():
            if k in self._func_params:
                self._p_in_routes[k] = self._get_param_routes(obj=o, base=[], seen=set())

    def _inspect_output_params(self):
        self._p_out_routes = {}
        for k,o in self._p_out.items():
            if k in self._func_params and not k in self._p_in:
                self._p_out_routes[k] = self._get_param_routes(obj=o, base=[], seen=set())

    def _get_param_routes(self, obj, base, seen):
        param_routes = []
        my_id = id(obj)
        if my_id in seen:
            return param_routes
        else:
            seen.add(my_id)
        if isinstance(obj, Metric):
            if len(base) == 0:
                tmp_route = [('s',0)]
                param_routes.append(tmp_route)
            else:
                param_routes = base[:]
        elif isinstance(obj, list) or isinstance(obj,tuple):
            for i,item in enumerate(obj):
                tmp_route = base[:]
                tmp_route.append(('i',i))
                param_route = self._get_param_routes(obj=obj[i], base=tmp_route, seen=seen)
                if len(param_route)>0:
                    if isinstance(param_route[0],list):
                        for route in param_route:
                            param_routes.append(route)
                    else:
                        param_routes.append(param_route)
        elif isinstance(obj, dict):
            for k,v in obj.items():
                tmp_route = base[:]
                tmp_route.append(('k',k))
                param_route = self._get_param_routes(obj=obj[k], base=tmp_route, seen=seen)
                if len(param_route)>0:
                    if isinstance(param_route[0],list):
                        for route in param_route:
                            param_routes.append(route)
                    else:
                        param_routes.append(param_route)
        else:
            for att,value in inspect.getmembers(obj):
                if isinstance(value,list) or isinstance(value,tuple) or isinstance(value,dict) or isinstance(value,Metric):
                    tmp_route = base[:]
                    tmp_route.append(('a', att))
                    param_route = self._get_param_routes(obj=value, base=tmp_route, seen=seen)
                    if len(param_route)>0:
                        if isinstance(param_route[0],list):
                            for route in param_route:
                                param_routes.append(route)
                        else:
                            param_routes.append(param_route)
        return param_routes

    def _register_metrics(self):
        metrics = set()
        for k,r in self._p_in_routes.items():
            obj = self._p_in[k]
            for route in r:
                m = self._get_metric_by_route(obj=obj, route=route)
                if m:
                    metrics.add(m)
        self._m_in = metrics
        metrics = set()
        for k,r in self._p_out_routes.items():
            obj = self._p_out[k]
            for route in r:
                m = self._get_metric_by_route(obj=obj, route=route)
                if m:
                    metrics.add(m)
        self._m_out = metrics
        for m in self._m_out:
            if m in self._m_in and not self.allow_loops:
                logging.logger.warning('Posible loop in metric {} in transfer method {}. Loops are not allowed, output values for this metric will be discarded'.format(str(m.uri),str(f.__name__)))

    def _get_metric_by_route(self, obj, route):
        m = obj
        try:
            for p in route:
                if p[0] == 'a':
                    m = getattr(m,p[1],None)
                elif p[0] == 'k':
                    m = m.get(p[1],None)
                elif p[0] == 'i':
                    m = m[p[1]]
        except (AttributeError, ValueError, IndexError, TypeError) as e:
            logging.logger.debug('Exception _get_metric_by_route: {}'.format(str(e)))
            return None
        else:
            return m

    def _get_execution_params(self, session, ts, metrics):
        exec_params={}
        arg_metrics_uris=[uri.get_global_uri(metric,owner=session._username) for metric in metrics]
        updated = []
        others = []
        data={}
        for m in self._m_in:
            its = None
            count = None
            reqs = self.data_reqs[m.uri] if isinstance(self.data_reqs,dict) and m.uri in self.data_reqs else self.data_reqs
            if reqs and reqs.past_delta:
                its = ts - reqs.past_delta
            if reqs and not its and reqs.past_count:
                count = reqs.past_count
            data[m]=session._metrics_store.get_serie(metric=m, ets=ts, its=its, count=count)
        for arg in self._func_params:
            if arg == 'ts':
                exec_params[arg]=ts
            elif arg == 'updated':
                exec_params[arg]=[m for m in self._m_in if uri.get_global_uri(m,owner=session._username) in arg_metrics_uris]
                for m in exec_params[arg]:
                    m.data = data[m]
            elif arg == 'others':
                exec_params[arg]=[m for m in self._m_in if uri.get_global_uri(m,owner=session._username) not in arg_metrics_uris]
                for m in exec_params[arg]:
                    m.data = data[m]
            elif arg in self._p_in:
                #function input params, load metrics and add data attribute with the necessary data
                obj=copy.deepcopy(self._p_in[arg])
                if arg in self._p_in_routes:
                    for route in self._p_in_routes[arg]:
                        m = self._get_metric_by_route(obj=obj, route=route)
                        if m:
                            m.data = data[m]
                exec_params[arg]=obj
            elif arg in self._p_out:
                #funcion output params, load metrics and add data attribute with an empty Series() object
                obj=copy.deepcopy(self._p_out[arg])
                if arg in self._p_out_routes:
                    for route in self._p_out_routes[arg]:
                        m = self._get_metric_by_route(obj=obj, route=route)
                        if m:
                            m.data = pd.Series()
                exec_params[arg]=obj
        return exec_params

    async def _process_exec_result(self, session, params):
        samples = []
        for k,obj in params.items():
            if k in self._p_out:
                routes = None
                if k in self._p_in_routes:
                    routes = self._p_in_routes[k]
                elif k in self._p_out_routes:
                    routes = self._p_out_routes[k]
                if routes:
                    for route in routes:
                        m = self._get_metric_by_route(obj=obj, route=route)
                        if m and (self.allow_loops or not m in self._m_in):
                            for row in m.data.items():
                                if not session._metrics_store.isin(metric=m, ts=row[0], content=row[1]):
                                    samples.append(Sample(metric=m, ts=row[0], data=row[1]))
        await session.send_samples(samples)

    def __call__(self, f):
        @wraps(f)
        async def decorated(session, ts, metrics):
            now=pd.Timestamp('now',tz='utc')
            if self.min_exec_delta and self.last_exec and now-self.min_exec_delta<self.last_exec:
                logging.logger.debug('min_exec_delta condition not satisfied, not executing '+f.__name__)
                return
            exec_params=self._get_execution_params(session=session, ts=ts, metrics=metrics)
            self.last_exec=pd.Timestamp('now',tz='utc')
            if asyncio.iscoroutinefunction(f):
                result = await f(**exec_params)
            else:
                result = f(**exec_params)
            await self._process_exec_result(session, exec_params)
        self.f = decorated
        self._func_params = inspect.signature(f).parameters
        self._inspect_input_params()
        self._inspect_output_params()
        self._register_metrics()
        static_transfer_methods.add_transfer_method(transfer_method=self, enabled=False)
        return f


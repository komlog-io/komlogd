'''

Datastores

'''

import time
import decimal
import pandas as pd
from komlogd.api import logging, uri
from komlogd.api.protocol.model import validation
from komlogd.api.protocol.model.transfer_methods import DataRequirements
from komlogd.api.protocol.model.types import Metric, Datasource, Datapoint


class MetricsStore:
    def __init__(self, owner, maintenance_exec_delta=None, data_delta=None):
        self.owner = owner
        self.maintenance_exec_delta=maintenance_exec_delta
        self.default_reqs=data_delta
        self._last_maintenance_exec=pd.Timestamp('now',tz='utc')
        self._metric_reqs={}
        self._series={}

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        validation.validate_username(value)
        self._owner = value.lower()

    @property
    def maintenance_exec_delta(self):
        return self._maintenance_exec_delta

    @maintenance_exec_delta.setter
    def maintenance_exec_delta(self, value):
        if value is None:
            self._maintenance_exec_delta=pd.Timedelta('5 m')
        elif isinstance(value, pd.Timedelta):
            self._maintenance_exec_delta=value
        else:
            raise TypeError('Invalid maintenance_exec_delta parameter')

    @property
    def default_reqs(self):
        return self._default_reqs

    @default_reqs.setter
    def default_reqs(self, value):
        if value is None:
            self._default_reqs=DataRequirements(past_delta=pd.Timedelta('10min'),past_count=2)
        elif isinstance(value, DataRequirements):
            self._default_reqs=value
        else:
            raise TypeError('Invalid data_delta parameter')

    def store(self, metric, ts, content):
        if isinstance(metric, Datasource):
            validation.validate_ds_content(content)
        elif isinstance(metric, Datapoint):
            validation.validate_dp_content(content)
        else:
            return
        validation.validate_ts(ts)
        ts=pd.Timestamp(ts.astimezone('utc'))
        guri = uri.get_global_uri(metric, owner=self.owner)
        try:
            if isinstance(content, decimal.Decimal):
                value = int(content) if content%1 == 0 else float(content)
            else:
                value = content
            self._series[guri][ts]=value
            if self._series[guri].index[-1]<self._series[guri].index[-2]:
                self._series[guri]=self._series[guri].sort_index()
        except KeyError:
            self._series[guri]=pd.Series(data=[value], index=[ts])
        except IndexError:
            pass

    def isin(self, metric, ts, content):
        guri = uri.get_global_uri(metric, owner=self.owner)
        if not guri in self._series:
            return False
        elif not ts in self._series[guri]:
            return False
        elif not self._series[guri][ts] == content:
            return False
        return True

    def run_maintenance(self):
        now = pd.Timestamp('now',tz='utc')
        if now - self.maintenance_exec_delta > self._last_maintenance_exec:
            self.purge()
            self._last_maintenance_exec=pd.Timestamp('now',tz='utc')

    def purge(self):
        now = pd.Timestamp('now',tz='utc')
        for guri in self._series.keys():
            reqs = self._metric_reqs[guri] if guri in self._metric_reqs else self._default_reqs
            past_delta = reqs.past_delta if reqs.past_delta else self._default_reqs.past_delta
            past_count = reqs.past_count if reqs.past_count else self._default_reqs.past_count
            t1_delta = now-past_delta if past_delta else None
            t1_count = self._series[guri][:now][-past_count:].index[0] if past_count else None
            if t1_delta or t1_count:
                count=self._series[guri].count()
                if t1_delta and t1_count:
                    t1 = t1_delta if t1_delta < t1_count else t1_count
                else:
                    t1 = t1_delta if t1_delta else t1_count
                self._series[guri]=self._series[guri].ix[t1:now]
                new_count=self._series[guri].count()
                logging.logger.debug('Deleted '+str(count-new_count)+' rows from time series '+guri+' before: '+str(count)+' now: '+str(new_count))
        end = pd.Timestamp('now',tz='utc')
        elapsed = end-now
        logging.logger.debug('Purge procedure finished. Elapsed time (s): '+'.'.join((str(elapsed.seconds),str(elapsed.microseconds).zfill(6))))
        return True

    def add_metric_data_reqs(self, metric, reqs):
        if not isinstance(metric, Metric):
            raise TypeError('Invalid metric type')
        if not isinstance(reqs, DataRequirements):
            raise TypeError('Invalid requirements type')
        guri = uri.get_global_uri(metric, owner=self.owner)
        if not guri in self._metric_reqs:
            self._metric_reqs[guri]=reqs
        else:
            new_reqs = DataRequirements()
            if reqs.past_delta and self._metric_reqs[guri].past_delta:
                new_reqs.past_delta = reqs.past_delta if reqs.past_delta > self._metric_reqs[guri].past_delta else self._metric_reqs[guri].past_delta
            elif reqs.past_delta:
                new_reqs.past_delta = reqs.past_delta
            else:
                new_reqs.past_delta = self._metric_reqs[guri].past_delta
            if reqs.past_count != None and self._metric_reqs[guri].past_count != None:
                new_reqs.past_count = reqs.past_count if reqs.past_count > self._metric_reqs[guri].past_count else self._metric_reqs[guri].past_count
            elif reqs.past_count != None:
                new_reqs.past_count = reqs.past_count
            else:
                new_reqs.past_count = self._metric_reqs[guri].past_count
            self._metric_reqs[guri] = new_reqs
        return True

    def get_metric_data_reqs(self, metric):
        guri = uri.get_global_uri(metric, owner=self.owner)
        try:
            return self._metric_reqs[guri]
        except KeyError:
            return None

    def get_serie(self, metric, ets=None, its=None, count=None):
        guri = uri.get_global_uri(metric, owner=self.owner)
        try:
            serie = self._series[guri]
            if not ets:
                d = serie.copy(deep=True)
            elif its:
                d = serie[its:ets].copy(deep=True)
            elif count:
                d = serie[:ets].tail(count).copy(deep=True)
            else:
                d = serie[:ets].tail(1).copy(deep=True)
            d.name = metric
            return d
        except KeyError:
            serie = pd.Series()
            serie.name = metric
            return serie


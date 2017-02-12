'''

Datastores

'''

import time
import decimal
import pandas as pd
from komlogd.api import logging
from komlogd.api.model import validation, orm


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
            self._default_reqs=orm.DataRequirements(past_delta=pd.Timedelta('10min'),past_count=2)
        elif isinstance(value, orm.DataRequirements):
            self._default_reqs=value
        else:
            raise TypeError('Invalid data_delta parameter')

    def store(self, metric, ts, content):
        if isinstance(metric, orm.Datasource):
            validation.validate_ds_content(content)
        elif isinstance(metric, orm.Datapoint):
            validation.validate_dp_content(content)
        else:
            return
        validation.validate_ts(ts)
        ts=pd.Timestamp(ts.astimezone('utc'))
        uri = orm.get_global_uri(metric, owner=self.owner)
        try:
            if isinstance(content, decimal.Decimal):
                value = int(content) if content%1 == 0 else float(content)
            else:
                value = content
            self._series[uri][ts]=value
            if self._series[uri].index[-1]<self._series[uri].index[-2]:
                self._series[uri]=self._series[uri].sort_index()
        except KeyError:
            self._series[uri]=pd.Series(data=[value], index=[ts])
        except IndexError:
            pass

    def run_maintenance(self):
        now = pd.Timestamp('now',tz='utc')
        if now - self.maintenance_exec_delta > self._last_maintenance_exec:
            self.purge()
            self._last_maintenance_exec=pd.Timestamp('now',tz='utc')

    def purge(self):
        now = pd.Timestamp('now',tz='utc')
        for uri in self._series.keys():
            reqs = self._metric_reqs[uri] if uri in self._metric_reqs else self._default_reqs
            past_delta = reqs.past_delta if reqs.past_delta else self._default_reqs.past_delta
            past_count = reqs.past_count if reqs.past_count else self._default_reqs.past_count
            t1_delta = now-past_delta if past_delta else None
            t1_count = self._series[uri][:now][-past_count:].index[0] if past_count else None
            if t1_delta or t1_count:
                count=self._series[uri].count()
                if t1_delta and t1_count:
                    t1 = t1_delta if t1_delta < t1_count else t1_count
                else:
                    t1 = t1_delta if t1_delta else t1_count
                self._series[uri]=self._series[uri].ix[t1:now]
                new_count=self._series[uri].count()
                logging.logger.debug('Deleted '+str(count-new_count)+' rows from time series '+uri+' before: '+str(count)+' now: '+str(new_count))
        end = pd.Timestamp('now',tz='utc')
        elapsed = end-now
        logging.logger.debug('Purge procedure finished. Elapsed time (s): '+'.'.join((str(elapsed.seconds),str(elapsed.microseconds).zfill(6))))
        return True

    def set_metric_data_reqs(self, metric, requirements):
        if not isinstance(metric, orm.Metric):
            raise TypeError('Invalid metric type')
        if not isinstance(requirements, orm.DataRequirements):
            raise TypeError('Invalid requirements type')
        uri = orm.get_global_uri(metric, owner=self.owner)
        if not uri in self._metric_reqs:
            self._metric_reqs[uri]=requirements
        else:
            new_reqs = orm.DataRequirements()
            if requirements.past_delta and self._metric_reqs[uri].past_delta:
                new_reqs.past_delta = requirements.past_delta if requirements.past_delta > self._metric_reqs[uri].past_delta else self._metric_reqs[uri].past_delta
            elif requirements.past_delta:
                new_reqs.past_delta = requirements.past_delta
            else:
                new_reqs.past_delta = self._metric_reqs[uri].past_delta
            if requirements.past_count != None and self._metric_reqs[uri].past_count != None:
                new_reqs.past_count = requirements.past_count if requirements.past_count > self._metric_reqs[uri].past_count else self._metric_reqs[uri].past_count
            elif requirements.past_count != None:
                new_reqs.past_count = requirements.past_count
            else:
                new_reqs.past_count = self._metric_reqs[uri].past_count
            self._metric_reqs[uri] = new_reqs
        return True

    def get_metric_data_reqs(self, metric):
        uri = orm.get_global_uri(metric, owner=self.owner)
        try:
            return self._metric_reqs[uri]
        except KeyError:
            return None

    def get_serie(self, metric):
        uri = orm.get_global_uri(metric, owner=self.owner)
        try:
            return self._series[uri]
        except KeyError:
            return pd.Series()


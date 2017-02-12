import pandas as pd
from komlogd.api.model import validation
from komlogd.api.model.types import Metrics

class Metric:
    _m_type_=None

    def __init__(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    def __eq__(self, other):
        return self.uri == other.uri

    def __hash__(self):
        return hash(self._uri)

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        raise TypeError('uri cannot be modified')

    @property
    def m_type(self):
        return self._m_type_

    @m_type.setter
    def m_type(self, value):
        raise TypeError('m_type cannot be modified')


class Datasource(Metric):
    _m_type_=Metrics.DATASOURCE

    def __init__(self, uri):
        super().__init__(uri=uri)

class Datapoint(Metric):
    _m_type_=Metrics.DATAPOINT

    def __init__(self, uri):
        super().__init__(uri=uri)

class DataRequirements:
    def __init__(self, past_delta=None, past_count=None):
        self.past_delta = past_delta
        self.past_count = past_count

    @property
    def past_delta(self):
        return self._past_delta

    @past_delta.setter
    def past_delta(self, value):
        if isinstance(value, pd.Timedelta) or value is None:
            self._past_delta = value
        else:
            raise TypeError('Invalid past_delta parameter')

    @property
    def past_count(self):
        return self._past_count

    @past_count.setter
    def past_count(self, value):
        if (isinstance(value, int) and value >= 0) or value is None:
            self._past_count = value
        else:
            raise TypeError('Invalid past_count parameter')

class Sample:
    def __init__(self, metric, data, ts):
        self.metric = metric
        self.ts = ts
        self.data = data

    @property
    def metric(self):
        return self._metric

    @metric.setter
    def metric(self, value):
        if isinstance(value, Metric):
            self._metric = value
        else:
            raise TypeError('Invalid metric parameter')

    @property
    def ts(self):
        return self._ts

    @ts.setter
    def ts(self, ts):
        validation.validate_ts(ts)
        self._ts=pd.Timestamp(ts)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if self.metric and self.metric.m_type == Metrics.DATASOURCE:
            validation.validate_ds_content(data)
        elif self.metric and self.metric.m_type == Metrics.DATAPOINT:
            validation.validate_dp_content(data)
        else:
            raise TypeError('Invalid Metric')
        self._data=data

def get_global_uri(metric, owner):
    if not isinstance(metric, Metric):
        raise TypeError('Invalid metric')
    if validation.is_local_uri(metric.uri):
        return ':'.join((owner.lower(),metric.uri))
    elif validation.is_global_uri(metric.uri):
        owner,local_uri=metric.uri.split(':')
        uri = ':'.join((owner.lower(),local_uri))
        return uri
    else:
        raise TypeError('Invalid metric uri')


import pandas as pd
from copy import deepcopy
from enum import Enum, unique
from komlogd.api.protocol.model import validation

@unique
class Actions(Enum):
    HOOK_TO_URI             = 'hook_to_uri'
    SEND_MULTI_DATA         = 'send_multi_data'
    SEND_DP_DATA            = 'send_dp_data'
    SEND_DS_DATA            = 'send_ds_data'
    UNHOOK_FROM_URI         = 'unhook_from_uri'
    REQUEST_DATA            = 'request_data'
    SEND_DATA_INTERVAL      = 'send_data_interval'
    GENERIC_RESPONSE        = 'generic_response'

@unique
class Metrics(Enum):
    DATASOURCE              = 'd'
    DATAPOINT               = 'p'

class Metric:
    _m_type_ = None

    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        return self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)

    def __copy__(self, memo):
        return type(self)(self.uri)

    def __deepcopy__(self, memo):
        return type(self)(deepcopy(self.uri, memo))

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        if not getattr(self, 'uri', None):
            validation.validate_uri(value)
            self._uri = value
            return
        raise TypeError('uri cannot be modified')

    @property
    def m_type(self):
        return self._m_type_

    @m_type.setter
    def m_type(self, value):
        raise TypeError('m_type cannot be modified')


class Datasource(Metric):
    _m_type_ = Metrics.DATASOURCE

    def __init__(self, uri):
        super().__init__(uri=uri)

class Datapoint(Metric):
    _m_type_ = Metrics.DATAPOINT

    def __init__(self, uri):
        super().__init__(uri=uri)

class Anomaly(Datapoint):

    def __init__(self, metric):
        super().__init__(uri='.'.join((metric.uri,'_anomaly')))

    def __deepcopy__(self, memo):
        uri = self.uri.split('._anomaly')[0]
        return type(self)(Metric(deepcopy(uri, memo)))

class Filter(Datapoint):

    def __init__(self, metric, key, value):
        super().__init__(uri='.'.join((metric.uri,'_filters',key,value)))

    def __deepcopy__(self, memo):
        uri,rest = self.uri.split('._filters.')[0:2]
        key,value = rest.split('.')[0:2]
        return type(self)(metric=Metric(deepcopy(uri, memo)),key=key, value=value)

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



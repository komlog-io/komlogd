import decimal
import uuid
import pandas as pd
from copy import deepcopy
from enum import Enum, unique
from komlogd.api.common import exceptions
from komlogd.api.model.session import sessionIndex
from komlogd.api.protocol import validation

@unique
class Metrics(Enum):
    DATASOURCE              = 'd'
    DATAPOINT               = 'p'

class Metric:
    _m_type_ = None

    def __new__(cls, *args, **kwargs):
        if cls is Metric:
            raise TypeError('<Metric> cannot be instantiated directly')
        return object.__new__(cls)

    def __init__(self, uri, session):
        self.uri = uri
        self._session = session
        self._guri = None

    def __eq__(self, other):
        try:
            equal = other._m_type_ == self._m_type_ and other.guri == self.guri and other.session.sid == self.session.sid
        except exceptions.SessionNotFoundException:
            equal = other._m_type_ == self._m_type_ and self._get_std_uri(self.uri) == self._get_std_uri(other.uri)
        except AttributeError:
            equal = False
        return equal

    def __hash__(self):
        try:
            h = hash((self._m_type_.value,self.session.sid.hex,self.guri))
        except exceptions.SessionNotFoundException:
            h = hash((self._m_type_.value,self._get_std_uri(self.uri)))
        return h

    def _get_std_uri(self, uri):
        e_uri = uri.split(':')
        if len(e_uri)>1:
            value = ':'.join((e_uri[0].lower(),e_uri[1]))
        else:
            value = uri
        return value

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
    def guri(self):
        if self._guri == None:
            if len(self.uri.split(':'))>1:
                self._guri = self._get_std_uri(self.uri)
            else:
                owner = self.session.username
                uri = ':'.join((owner,self.uri))
                self._guri = self._get_std_uri(uri)
        return self._guri

    @guri.setter
    def guri(self, value):
        raise ValueError('global uri cannot be set manually')

    @property
    def session(self):
        if self._session is None:
            session = sessionIndex.get_session()
            if session is None:
                raise exceptions.SessionNotFoundException('No session found')
            else:
                self._session = session
        return self._session

    async def get(self, *args, **kwargs):
        return await self.session.store.get(metric=self, *args, **kwargs)

    def insert(self, *args, **kwargs):
        return  self.session.store.insert(metric=self, *args, **kwargs)

class Datasource(Metric):
    _m_type_ = Metrics.DATASOURCE

    def __init__(self, uri, session=None, supplies=None):
        super().__init__(uri=uri, session=session)
        self.supplies = supplies

    @property
    def supplies(self):
        return self._supplies

    @supplies.setter
    def supplies(self, uris):
        if uris is None:
            self._supplies = None
        elif isinstance(uris, list):
            for uri in uris:
                validation.validate_local_uri(uri)
            self._supplies = sorted(list(set(uris)))
        else:
            raise TypeError('Invalid supplies parameter')

class Datapoint(Metric):
    _m_type_ = Metrics.DATAPOINT

    def __init__(self, uri, session=None):
        super().__init__(uri=uri, session=session)

class Anomaly(Datapoint):

    def __init__(self, metric, session=None):
        super().__init__(uri='.'.join((metric.uri,'_anomaly')), session=session)

class Tag(Datapoint):

    def __init__(self, metric, key, value, session=None):
        validation.validate_uri_level(key)
        validation.validate_uri_level(value)
        super().__init__(uri='.'.join((metric.uri,'_tags',key,value)), session=session)

class Sample:
    def __init__(self, metric, t, value):
        self.metric = metric
        self.t = t
        self.value = value

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
    def t(self):
        return self._t

    @t.setter
    def t(self, t):
        validation.validate_timeuuid(t)
        self._t=t

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if self.metric._m_type_ == Metrics.DATASOURCE:
            validation.validate_ds_value(value)
            self._value = value
        elif self.metric._m_type_ == Metrics.DATAPOINT:
            validation.validate_dp_value(value)
            self._value = decimal.Decimal(value)
        else:
            raise TypeError('Invalid Metric')



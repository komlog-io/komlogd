import decimal
import pandas as pd
from komlogd.api.model import validation, orm
from komlogd.api.model.types import Metrics, Actions

class Catalog(type):
    def __init__(cls, name, bases, dct):
        if hasattr(cls, '_action_'):
            cls._catalog_[cls._action_.value]=cls
        super().__init__(name, bases, dct)

class KomlogMessage(metaclass=Catalog):
    _version_ = 1
    _catalog_ = {}

    def __new__(cls, *args, **kwargs):
        if cls is KomlogMessage:
            raise TypeError('<KomlogMessage> cannot be instantiated directly')
        return object.__new__(cls)

    @property
    def action(self):
        return self._action_

    @action.setter
    def action(self, value):
        raise TypeError('Action cannot be modified')

    @property
    def v(self):
        return self._version_

    @v.setter
    def v(self, value):
        raise TypeError('Version cannot be modified')

    @classmethod
    def load_from_dict(cls, msg):
        if cls is KomlogMessage:
            if isinstance(msg, dict) and 'action' in msg:
                try:
                    return cls._catalog_[msg['action']].load_from_dict(msg)
                except KeyError:
                    raise TypeError('Unknown message type')
            raise TypeError('Message not supported')
        else:
            raise NotImplementedError

class SendDsData(KomlogMessage):
    _action_ = Actions.SEND_DS_DATA

    def __init__(self, uri, ts, content):
        self.uri=uri
        self.ts=ts
        self.content=content

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    @property
    def ts(self):
        return self._ts

    @ts.setter
    def ts(self, ts):
        validation.validate_ts(ts)
        self._ts=pd.Timestamp(ts)

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        validation.validate_ds_content(content)
        self._content=content

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'payload':{
                'uri':self.uri,
                'ts':self.ts.isoformat(),
                'content':self.content
            }
        }

class SendDpData(KomlogMessage):
    _action_ = Actions.SEND_DP_DATA

    def __init__(self, uri, ts, content):
        self.uri=uri
        self.ts=ts
        self.content=content

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    @property
    def ts(self):
        return self._ts

    @ts.setter
    def ts(self, ts):
        validation.validate_ts(ts)
        self._ts=pd.Timestamp(ts)

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        validation.validate_dp_content(content)
        self._content= content if isinstance(content,decimal.Decimal) else decimal.Decimal(str(content))

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'payload':{
                'uri':self.uri,
                'ts':self.ts.isoformat(),
                'content':str(self.content)
            }
        }

class SendMultiData(KomlogMessage):
    _action_ = Actions.SEND_MULTI_DATA

    def __init__(self, ts, uris):
        self.ts=ts
        self.uris=uris

    @property
    def uris(self):
        return self._uris

    @uris.setter
    def uris(self, uris):
        if (isinstance(uris,list)
            and all(isinstance(item,dict) for item in uris)
            and all('uri' in item for item in uris)
            and all(validation.validate_uri(item['uri']) for item in uris)
            and all('type' in item for item in uris)
            and all(validation.validate_metric_type(item['type']) for item in uris)
            and all('content' in item for item in uris)
            and all(validation.validate_ds_content(item['content']) for item in uris if item['type'] in (Metrics.DATASOURCE.value, Metrics.DATASOURCE))
            and all(validation.validate_dp_content(item['content']) for item in uris if item['type'] in (Metrics.DATAPOINT.value, Metrics.DATAPOINT))):
            ds_uris=[{'uri':item['uri'],'type':Metrics(item['type']),'content':item['content']} for item in uris if item['type'] in ( Metrics.DATASOURCE.value, Metrics.DATASOURCE)]
            dp_uris=[{'uri':item['uri'],'type':Metrics(item['type']),'content':decimal.Decimal(str(item['content']))} for item in uris if item['type'] in (Metrics.DATAPOINT.value, Metrics.DATAPOINT)]
            self._uris=ds_uris+dp_uris
        else:
            raise TypeError('Uris parameter not valid')

    @property
    def ts(self):
        return self._ts

    @ts.setter
    def ts(self, ts):
        validation.validate_ts(ts)
        self._ts=pd.Timestamp(ts)

    @classmethod
    def load_from_dict(cls, msg):
        if (isinstance(msg, dict)
            and 'v' in msg
            and 'action' in msg
            and 'payload' in msg
            and isinstance(msg['v'],int) and msg['v']==cls._version_
            and isinstance(msg['action'],str) and msg['action']==cls._action_.value
            and isinstance(msg['payload'],dict)
            and 'ts' in msg['payload']
            and 'uris' in msg['payload']):
            ts=msg['payload']['ts']
            uris=msg['payload']['uris']
            return cls(ts=ts,uris=uris)
        else:
            raise TypeError('Could not load message, invalid type')

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        ds_uris=[{'uri':item['uri'],'type':item['type'].value,'content':item['content']} for item in self._uris if item['type'] == Metrics.DATASOURCE]
        dp_uris=[{'uri':item['uri'],'type':item['type'].value,'content':str(item['content'])} for item in self._uris if item['type'] == Metrics.DATAPOINT]
        return {
            'v':self.v,
            'action':self.action.value,
            'payload':{
                'ts':self.ts.isoformat(),
                'uris':ds_uris+dp_uris
            }
        }

class HookToUri(KomlogMessage):
    _action_ = Actions.HOOK_TO_URI

    def __init__(self, uri):
        self.uri=uri

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'payload':{
                'uri':self.uri
            }
        }

class UnHookFromUri(KomlogMessage):
    _action_ = Actions.UNHOOK_FROM_URI

    def __init__(self, uri):
        self.uri = uri

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'payload':{
                'uri':self.uri
            }
        }

class RequestData(KomlogMessage):
    _action_ = Actions.REQUEST_DATA

    def __init__(self, uri, start=None, end=None, count=None):
        self.uri = uri
        self.start = start
        self.end = end
        self.count = count

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri = uri

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, start):
        if start is not None:
            validation.validate_ts(start)
            self._start=pd.Timestamp(start)
        else:
            self._start = None

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, end):
        if end is not None:
            validation.validate_ts(end)
            self._end=pd.Timestamp(end)
        else:
            self._end = None

    @property
    def count(self):
        return self._count

    @count.setter
    def count(self, count):
        if isinstance(count,int) or count is None:
            self._count=count
        else:
            raise TypeError('Invalid count parameter')

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'payload':{
                'uri':self._uri,
                'start':self._start.isoformat() if self._start else None,
                'end':self._end.isoformat() if self._end else None,
                'count':self._count,
            }
        }

class SendDataInterval(KomlogMessage):
    _action_ = Actions.SEND_DATA_INTERVAL

    def __init__(self, metric, start, end, data):
        self.metric = metric
        self.start = start
        self.end = end 
        self.data = data

    @property
    def metric(self):
        return self._metric

    @metric.setter
    def metric(self, metric):
        if isinstance(metric, orm.Datasource) or isinstance(metric, orm.Datapoint):
            self._metric = metric
        else:
            raise TypeError('Invalid metric type')

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, start):
        validation.validate_ts(start)
        self._start = pd.Timestamp(start)

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, end):
        validation.validate_ts(end)
        self._end = pd.Timestamp(end)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if (isinstance(data, list)
            and all(
                isinstance(item,list)
                and len(item)==2
                and validation.validate_ts(item[0])
                and (validation.validate_dp_content(item[1]) if isinstance(self._metric, orm.Datapoint) else validation.validate_ds_content(item[1]))
                for item in data)
            ):
            if isinstance(self._metric, orm.Datasource):
                self._data=[(pd.Timestamp(row[0]),row[1]) for row in data]
            elif isinstance(self._metric, orm.Datapoint):
                self._data=[(pd.Timestamp(row[0]),decimal.Decimal(str(row[1]))) for row in data]
        else:
            raise TypeError('Invalid data')

    @classmethod
    def load_from_dict(cls, msg):
        if (isinstance(msg,dict)
            and 'v' in msg
            and 'action' in msg
            and 'payload' in msg
            and isinstance(msg['v'],int) and msg['v']==cls._version_
            and isinstance(msg['action'],str) and msg['action']==cls._action_.value
            and isinstance(msg['payload'],dict)
            and 'uri' in msg['payload']
            and isinstance(msg['payload']['uri'],dict)
            and 'uri' in msg['payload']['uri']
            and 'type' in msg['payload']['uri']
            and 'start' in msg['payload']
            and 'end' in msg['payload']
            and 'data' in msg['payload']):
            if msg['payload']['uri']['type'] == Metrics.DATASOURCE.value:
                metric = orm.Datasource(uri=msg['payload']['uri']['uri'])
            elif msg['payload']['uri']['type'] == Metrics.DATAPOINT.value:
                metric = orm.Datapoint(uri=msg['payload']['uri']['uri'])
            else:
                raise TypeError ('Invalid metric type')
            start=msg['payload']['start']
            end=msg['payload']['end']
            data=msg['payload']['data']
            return cls(metric=metric, start=start, end=end, data=data)
        else:
            raise TypeError('Could not load message, invalid type')


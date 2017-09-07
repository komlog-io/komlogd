import uuid
import decimal
from enum import Enum, unique
from komlogd.api.common import timeuuid
from komlogd.api.protocol import validation
from komlogd.api.model.metrics import Metrics

@unique
class Actions(Enum):
    GENERIC_RESPONSE        = 'generic_response'
    REQUEST_DATA            = 'request_data'
    HOOK_TO_URI             = 'hook_to_uri'
    SEND_DATA_INTERVAL      = 'send_data_interval'
    SEND_MULTI_DATA         = 'send_multi_data'
    SEND_DP_DATA            = 'send_dp_data'
    SEND_DS_DATA            = 'send_ds_data'
    SEND_DS_INFO            = 'send_ds_info'
    UNHOOK_FROM_URI         = 'unhook_from_uri'


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

    def __init__(self, seq, irt):
        self.seq = seq if seq != None else timeuuid.TimeUUID()
        self.irt = irt

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

    @property
    def seq(self):
        return self._seq

    @seq.setter
    def seq(self, value):
        if hasattr(self, '_seq'):
            raise TypeError('Sequence cannot be modified')
        elif validation.is_message_sequence(value):
            self._seq = value
        else:
            raise TypeError('Invalid sequence')

    @property
    def irt(self):
        return self._irt

    @irt.setter
    def irt(self, value):
        if hasattr(self, '_irt'):
            raise TypeError('irt cannot be modified')
        elif value is None or validation.is_message_sequence(value):
            self._irt = value
        else:
            raise TypeError('Invalid irt')

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

class GenericResponse(KomlogMessage):
    _action_ = Actions.GENERIC_RESPONSE

    def __init__(self, status, error, reason, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
        self.status = status
        self.error = error
        self.reason = reason

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if isinstance(value, int) and value>0:
            self._status=value
        else:
            raise TypeError('Invalid status')

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, value):
        if isinstance(value, int) and value>=0:
            self._error=value
        else:
            raise TypeError('Invalid error')

    @property
    def reason(self):
        return self._reason

    @reason.setter
    def reason(self, value):
        if isinstance(value, str) or value is None:
            self._reason=value
        else:
            raise TypeError('Invalid reason')

    @classmethod
    def load_from_dict(cls, msg):
        if (isinstance(msg,dict)
            and 'v' in msg
            and 'action' in msg
            and 'seq' in msg
            and 'irt' in msg
            and 'payload' in msg
            and isinstance(msg['v'],int)
            and isinstance(msg['action'],str) and msg['action']==cls._action_.value
            and isinstance(msg['payload'],dict)
            and 'status' in msg['payload']
            and 'error' in msg['payload']
            and 'reason' in msg['payload']):
            status=msg['payload']['status']
            error=msg['payload']['error']
            reason=msg['payload']['reason']
            seq = timeuuid.TimeUUID(s=msg['seq'])
            irt = timeuuid.TimeUUID(s=msg['irt']) if msg['irt']!=None else None
            return cls(status=status, error=error, reason=reason, seq=seq, irt=irt)
        else:
            raise TypeError('Could not load message, invalid type')

class SendDsData(KomlogMessage):
    _action_ = Actions.SEND_DS_DATA

    def __init__(self, uri, t, content, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
        self.uri=uri
        self.t=t
        self.content=content

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    @property
    def t(self):
        return self._t

    @t.setter
    def t(self, t):
        validation.validate_timeuuid(t)
        self._t=t

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        validation.validate_ds_value(content)
        self._content=content

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt else None,
            'payload':{
                'uri':self.uri,
                't':self.t.hex,
                'content':self.content
            }
        }

class SendDsInfo(KomlogMessage):
    _action_ = Actions.SEND_DS_INFO

    def __init__(self, uri, supplies=None, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
        self.uri = uri
        self.supplies = supplies

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        validation.validate_uri(value)
        self._uri = value

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

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt else None,
            'payload':{
                'uri':self.uri,
                'supplies':self.supplies
            }
        }

class SendDpData(KomlogMessage):
    _action_ = Actions.SEND_DP_DATA

    def __init__(self, uri, t, content, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
        self.uri=uri
        self.t=t
        self.content=content

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, uri):
        validation.validate_uri(uri)
        self._uri=uri

    @property
    def t(self):
        return self._t

    @t.setter
    def t(self, t):
        validation.validate_timeuuid(t)
        self._t=t

    @property
    def content(self):
        return self._content

    @content.setter
    def content(self, content):
        validation.validate_dp_value(content)
        self._content = content if isinstance(content,decimal.Decimal) else decimal.Decimal(str(content))

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        return {
            'v':self.v,
            'action':self.action.value,
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt != None else None,
            'payload':{
                'uri':self.uri,
                't':self.t.hex,
                'content':str(self.content)
            }
        }

class SendMultiData(KomlogMessage):
    _action_ = Actions.SEND_MULTI_DATA

    def __init__(self, t, uris, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
        self.t=t
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
            and all(item['type'] in [m.value for m in Metrics] + [m for m in Metrics] for item in uris)
            and all('content' in item for item in uris)
            and all(validation.validate_ds_value(item['content']) for item in uris if item['type'] in (Metrics.DATASOURCE.value, Metrics.DATASOURCE))
            and all(validation.validate_dp_value(item['content']) for item in uris if item['type'] in (Metrics.DATAPOINT.value, Metrics.DATAPOINT))):
            ds_uris=[{'uri':item['uri'],'type':Metrics(item['type']),'content':item['content']} for item in uris if item['type'] in ( Metrics.DATASOURCE.value, Metrics.DATASOURCE)]
            dp_uris=[{'uri':item['uri'],'type':Metrics(item['type']),'content':decimal.Decimal(str(item['content']))} for item in uris if item['type'] in (Metrics.DATAPOINT.value, Metrics.DATAPOINT)]
            self._uris=ds_uris+dp_uris
        else:
            raise TypeError('Uris parameter not valid')

    @property
    def t(self):
        return self._t

    @t.setter
    def t(self, t):
        validation.validate_timeuuid(t)
        self._t=t

    @classmethod
    def load_from_dict(cls, msg):
        if (isinstance(msg, dict)
            and 'v' in msg
            and 'action' in msg
            and 'seq' in msg
            and 'irt' in msg
            and 'payload' in msg
            and isinstance(msg['v'],int) and msg['v']==cls._version_
            and isinstance(msg['action'],str) and msg['action']==cls._action_.value
            and isinstance(msg['payload'],dict)
            and 't' in msg['payload']
            and 'uris' in msg['payload']):
            t = timeuuid.TimeUUID(s=msg['payload']['t'])
            uris = msg['payload']['uris']
            seq = timeuuid.TimeUUID(s=msg['seq'])
            irt = timeuuid.TimeUUID(s=msg['irt']) if msg['irt'] != None else None
            return cls(t=t, uris=uris, seq=seq, irt=irt)
        else:
            raise TypeError('Could not load message, invalid type')

    def to_dict(self):
        ''' returns a JSON serializable dict '''
        ds_uris=[{'uri':item['uri'],'type':item['type'].value,'content':item['content']} for item in self._uris if item['type'] == Metrics.DATASOURCE]
        dp_uris=[{'uri':item['uri'],'type':item['type'].value,'content':str(item['content'])} for item in self._uris if item['type'] == Metrics.DATAPOINT]
        return {
            'v':self.v,
            'action':self.action.value,
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt != None else None,
            'payload':{
                't':self.t.hex,
                'uris':ds_uris+dp_uris
            }
        }

class HookToUri(KomlogMessage):
    _action_ = Actions.HOOK_TO_URI

    def __init__(self, uri, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
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
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt != None else None,
            'payload':{
                'uri':self.uri
            }
        }

class UnHookFromUri(KomlogMessage):
    _action_ = Actions.UNHOOK_FROM_URI

    def __init__(self, uri, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
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
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt != None else None,
            'payload':{
                'uri':self.uri
            }
        }

class RequestData(KomlogMessage):
    _action_ = Actions.REQUEST_DATA

    def __init__(self, uri, start=None, end=None, count=None, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
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
            validation.validate_timeuuid(start)
            self._start=start
        else:
            self._start = None

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, end):
        if end is not None:
            validation.validate_timeuuid(end)
            self._end=end
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
            'seq':self.seq.hex,
            'irt':self.irt.hex if self.irt != None else None,
            'payload':{
                'uri':self._uri,
                'start':self._start.hex if self._start else None,
                'end':self._end.hex if self._end else None,
                'count':self._count,
            }
        }

class SendDataInterval(KomlogMessage):
    _action_ = Actions.SEND_DATA_INTERVAL

    def __init__(self, uri, m_type, start, end, data, seq=None, irt=None):
        super().__init__(seq=seq, irt=irt)
        self.uri = uri
        self.m_type = m_type
        self.start = start
        self.end = end
        self.data = data

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        validation.validate_uri(value)
        self._uri = value

    @property
    def m_type(self):
        return self._m_type

    @m_type.setter
    def m_type(self, value):
        if value in Metrics:
            self._m_type = value
        else:
            raise TypeError('Invalid m_type')

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, start):
        validation.validate_timeuuid(start)
        self._start = start

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, end):
        validation.validate_timeuuid(end)
        self._end = end

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, data):
        if (isinstance(data, list)
            and all(
                isinstance(item,list)
                and len(item)==2
                and (validation.validate_dp_value(item[1]) if self.m_type == Metrics.DATAPOINT else validation.validate_ds_value(item[1]))
                for item in data)
            ):
            if self.m_type == Metrics.DATAPOINT:
                self._data=[(timeuuid.TimeUUID(s=row[0]),decimal.Decimal(str(row[1]))) for row in data]
            else:
                self._data=[(timeuuid.TimeUUID(s=row[0]),row[1]) for row in data]
        else:
            raise TypeError('Invalid data')

    @classmethod
    def load_from_dict(cls, msg):
        if (isinstance(msg,dict)
            and 'v' in msg
            and 'action' in msg
            and 'seq' in msg
            and 'irt' in msg
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
                m_type = Metrics.DATASOURCE
            elif msg['payload']['uri']['type'] == Metrics.DATAPOINT.value:
                m_type = Metrics.DATAPOINT
            else:
                raise TypeError ('Invalid metric type')
            start = timeuuid.TimeUUID(s=msg['payload']['start'])
            end = timeuuid.TimeUUID(s=msg['payload']['end'])
            data=msg['payload']['data']
            seq = timeuuid.TimeUUID(s=msg['seq'])
            irt = timeuuid.TimeUUID(s=msg['irt']) if msg['irt'] != None else None
            return cls(uri=msg['payload']['uri']['uri'], m_type=m_type, start=start, end=end, data=data, seq=seq, irt=irt)
        else:
            raise TypeError('Could not load message, invalid type')


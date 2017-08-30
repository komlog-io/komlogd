import uuid
import decimal
import re
import pandas as pd
from datetime import datetime
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from komlogd.api.common import timeuuid

URILEVEL=re.compile('^[a-zA-Z0-9\-_]+(?!\s)$')
LOCALURI=re.compile('^([a-zA-Z0-9\-_]+\.)*[a-zA-Z0-9\-_]+(?!\s)$')
GLOBALURI=re.compile('^([a-zA-Z0-9\-_]+\.)*[a-zA-Z0-9\-_]+:([a-zA-Z0-9\-_]+\.)*[a-zA-Z0-9\-_]+(?!\s)$')
USERNAME=re.compile('^([a-zA-Z0-9\-_]+\.)*[a-zA-Z0-9\-_]+(?!\s)$')
ISODATE=re.compile('^((?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$')

def validate_username(value):
    if isinstance(value,str) and USERNAME.search(value):
        return True
    raise TypeError('username is not valid: '+str(value))

def validate_uri(value):
    if not isinstance(value,str):
        raise TypeError('value is not a string: '+str(value))
    if LOCALURI.search(value) or GLOBALURI.search(value):
        return True
    else:
        raise TypeError('value is not a valid uri: '+value)

def validate_local_uri(value):
    if not isinstance(value,str):
        raise TypeError('value is not a string: '+str(value))
    if LOCALURI.search(value):
        return True
    else:
        raise TypeError('value is not a valid local uri: '+value)

def validate_uri_level(value):
    if not isinstance(value,str):
        raise TypeError('value is not a string: '+str(value))
    if URILEVEL.search(value):
        return True
    else:
        raise TypeError('value is not a valid uri level: '+value)

def validate_timeuuid(value):
    if isinstance(value,timeuuid.TimeUUID) and value.version == 1:
        return True
    raise TypeError('value is not a valid TimeUUID: '+str(value))

def validate_ts(value):
    if ((isinstance(value, str) and ISODATE.search(value))
        or isinstance(value, pd.Timestamp)
        or isinstance(value, datetime)):
        try:
            t=pd.Timestamp(value)
            if t.tz is None:
                raise TypeError('timezone is required')
            if t.nanosecond != 0:
                raise TypeError('ts max precision is milliseconds')
            return True
        except ValueError:
            raise TypeError('ts value is out of limits')
    else:
        raise TypeError('ts type is not valid')

def validate_privkey(value):
    if isinstance(value, RSAPrivateKey) and value.key_size >= 4096:
        return True
    raise TypeError('Invalid private key')

def validate_ds_value(value):
    if not isinstance(value, str):
        raise TypeError('value not a string')
    if len(value.encode('utf-8'))>2**17:
        raise TypeError('value size limit is 128K bytes')
    return True

def validate_dp_value(value):
    if isinstance(value,int) or isinstance(value,float):
        return True
    else:
        try:
            num=decimal.Decimal(value)
            float(num)
            return True
        except (decimal.InvalidOperation, ValueError, TypeError, OverflowError):
            raise TypeError('value not a number')

def is_message_sequence(value):
    if isinstance(value,timeuuid.TimeUUID) and value.version == 1:
        return True
    else:
        return False

def is_local_uri(uri):
    if isinstance(uri,str) and LOCALURI.search(uri):
        return True
    return False

def is_global_uri(uri):
    if isinstance(uri,str) and GLOBALURI.search(uri):
        return True
    return False

def is_username(value):
    if isinstance(value,str) and USERNAME.search(value):
        return True
    return False

def is_uuid1_hex(value):
    try:
        u = uuid.UUID(value)
        if u.version == 1:
            return True
    except (ValueError, AttributeError):
        return False
    else:
        return False


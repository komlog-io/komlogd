import decimal
import re
import pandas as pd
from datetime import datetime
from komlogd.api.model.types import Metrics

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
        raise TypeError('uri is not a string')
    if LOCALURI.search(value) or GLOBALURI.search(value):
        return True
    else:
        raise TypeError('uri is not valid: '+value)

def validate_ts(value):
    if ((isinstance(value, str) and ISODATE.search(value))
        or isinstance(value, pd.Timestamp)
        or isinstance(value, datetime)):
        try:
            t=pd.Timestamp(value)
            if t.tz is None:
                raise TypeError('timezone is required')
            if t.nanosecond != 0:
                raise TypeError('ts max precision')
            return True
        except ValueError:
            raise TypeError('ts value is out of bounds')
    else:
        raise TypeError('ts type is not valid')

def validate_metric_type(value):
    if value in Metrics:
        return True
    elif any(value is metric.value for metric in Metrics):
        return True
    else:
        raise TypeError('Invalid metric type')

def validate_ds_content(value):
    if not isinstance(value, str):
        raise TypeError('content is not a string')
    if len(value.encode('utf-8'))>2**17:
        raise TypeError('content size limit is 128K bytes')
    return True

def validate_dp_content(value):
    try:
        num=decimal.Decimal(value)
        return True
    except (decimal.InvalidOperation, ValueError, TypeError):
        raise TypeError('datapoint value not valid')

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


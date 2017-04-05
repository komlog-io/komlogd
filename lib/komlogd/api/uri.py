import pandas as pd
from komlogd.api.protocol.model import validation
from komlogd.api.protocol.model.types import Metric

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


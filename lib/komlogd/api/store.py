'''

Functions for requesting the metrics store


'''

import uuid
from komlogd.api import logging
from komlogd.api.model import store


def get_content_at(metric, ts):
    try:
        return store.MetricStore.series[metric.uri][ts]
    except KeyError:
        return None

def get_interval(metric, start, end):
    try:
        return store.MetricStore.series[metric.uri][start:end]
    except KeyError:
        return None


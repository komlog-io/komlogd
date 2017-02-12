import asyncio
import json
from komlogd.api import logging
from komlogd.api.model.types import Metrics, Actions
from komlogd.api.model.orm import Datasource, Datapoint, Metric


def process_message_send_multi_data(msg, session, **kwargs):
    metrics=[]
    for item in msg.uris:
        if item['type'] == Metrics.DATASOURCE:
            metric=Datasource(uri=item['uri'])
        elif item['type'] == Metrics.DATAPOINT:
            metric=Datapoint(uri=item['uri'])
        session.metrics_store.store(metric, msg.ts, item['content'])
        metrics.append(metric)
    impulse_methods=session.impulses.get_impulses(metrics=metrics)
    for item in impulse_methods:
        logging.logger.debug('Requesting execution of method: '+item.f.__name__)
        asyncio.ensure_future(item.f(ts=msg.ts, metrics=metrics, session=session))

def process_message_send_data_interval(msg, session, **kwargs):
    logging.logger.debug('Received data for uri: '+msg.metric.uri)
    logging.logger.debug('Interval ['+msg.start.isoformat()+' - '+msg.end.isoformat()+']')
    for row in msg.data[::-1]:
        session.metrics_store.store(metric=msg.metric, ts=row[0], content=row[1])


processing_map={
    Actions.SEND_MULTI_DATA:process_message_send_multi_data,
    Actions.SEND_DATA_INTERVAL:process_message_send_data_interval,
}

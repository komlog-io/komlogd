import asyncio
import json
from komlogd.api import logging
from komlogd.api.protocol.processing import procedure as prproc
from komlogd.api.protocol.model.types import Metrics, Actions, Datasource, Datapoint, Metric


def process_message_send_multi_data(msg, session, **kwargs):
    metrics=[]
    for item in msg.uris:
        if item['type'] == Metrics.DATASOURCE:
            metric=Datasource(uri=item['uri'])
        elif item['type'] == Metrics.DATAPOINT:
            metric=Datapoint(uri=item['uri'])
        if not session._metrics_store.isin(metric=metric, ts=msg.ts, content=item['content']):
            session._metrics_store.store(metric, msg.ts, item['content'])
            metrics.append(metric)
    transfer_methods = session._transfer_methods.get_on_update_transfer_methods(metrics=metrics)
    for item in transfer_methods:
        logging.logger.debug('Requesting execution of method: '+item.f.__name__)
        asyncio.ensure_future(prproc.exec_transfer_method(mid=item.mid,ts=msg.ts,metrics=metrics,session=session))

def process_message_send_data_interval(msg, session, **kwargs):
    logging.logger.debug('Received data for uri: '+msg.metric.uri)
    logging.logger.debug('Interval ['+msg.start.isoformat()+' - '+msg.end.isoformat()+']')
    for row in msg.data[::-1]:
        session._metrics_store.store(metric=msg.metric, ts=row[0], content=row[1])

def process_message_generic_response(msg, session, **kwargs):
    logging.logger.debug('Received generic_response message')
    logging.logger.debug(str(msg.__dict__))

processing_map={
    Actions.SEND_MULTI_DATA:process_message_send_multi_data,
    Actions.SEND_DATA_INTERVAL:process_message_send_data_interval,
    Actions.GENERIC_RESPONSE:process_message_generic_response,
}

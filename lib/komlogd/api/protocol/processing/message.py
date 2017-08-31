import asyncio
import json
from komlogd.api.common import logging
from komlogd.api.protocol.messages import Actions
from komlogd.api.model.metrics import Metrics, Datasource, Datapoint, Metric
from komlogd.api.model.transfer_methods import tmIndex


def process_message_send_multi_data(msg, session, **kwargs):
    metrics=[]
    for item in msg.uris:
        if item['type'] == Metrics.DATASOURCE:
            metric=Datasource(uri=item['uri'], session=session)
        elif item['type'] == Metrics.DATAPOINT:
            metric=Datapoint(uri=item['uri'], session=session)
        if not session.store.is_in(metric=metric, t=msg.t, value=item['content']):
            session.store.insert(metric, msg.t, item['content'])
            metrics.append(metric)
    tmIndex.metrics_updated(t=msg.t, metrics=metrics, irt=msg.seq)

def process_message_send_data_interval(msg, session, **kwargs):
    if msg.m_type == Metrics.DATAPOINT:
        metric = Datapoint(uri=msg.uri, session=session)
    else:
        metric = Datasource(uri=msg.uri, session=session)
    for row in msg.data[::-1]:
        session.store.insert(metric, row[0], row[1])

def process_message_generic_response(msg, session, **kwargs):
    logging.logger.debug('Received generic_response message')
    logging.logger.debug(str(msg.__dict__))

processing_map={
    Actions.SEND_MULTI_DATA:process_message_send_multi_data,
    Actions.SEND_DATA_INTERVAL:process_message_send_data_interval,
    Actions.GENERIC_RESPONSE:process_message_generic_response,
}

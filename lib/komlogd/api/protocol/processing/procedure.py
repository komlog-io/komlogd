import asyncio
import json
import pandas as pd
from komlogd.api import logging
from komlogd.api.protocol.model import messages
from komlogd.api.protocol.model.codes import Status
from komlogd.api.protocol.model.types import Metrics, Actions, Datasource, Datapoint, Metric, Sample

async def initialize_transfer_methods(session):
    methods = session._transfer_methods.get_transfer_methods(enabled=False)
    for method in methods:
        await initialize_transfer_method(session, method)

async def initialize_transfer_method(session, method):
    initialized={metric.uri:False for metric in method.m_in}
    for metric in method.m_in:
        reqs = method.data_reqs[metric.uri] if isinstance(method.data_reqs, dict) and metric.uri in method.data_reqs else method.data_reqs
        if reqs:
            session._metrics_store.add_metric_data_reqs(metric=metric, reqs=reqs)
            end = pd.Timestamp('now',tz='utc') if reqs.past_delta else None
            start= end - reqs.past_delta if reqs.past_delta else None
            count = reqs.past_count if reqs.past_count else None
        msg = messages.HookToUri(uri=metric.uri)
        rsp = await session._await_response(msg)
        session._mark_message_done(msg.seq)
        if rsp.action == Actions.GENERIC_RESPONSE and rsp.status == Status.MESSAGE_EXECUTION_OK:
            if reqs is None:
                initialized[metric.uri]=True
            else:
                msg2 = messages.RequestData(uri=metric.uri, start=start, end=end, count=count)
                rsp2 = await session._await_response(msg2)
                done = False
                done_start = False if start else True
                done_end = False if end else True
                while not done:
                    if rsp2.action == Actions.GENERIC_RESPONSE:
                        if rsp2.status != Status.MESSAGE_ACCEPTED_FOR_PROCESSING:
                            session._mark_message_done(msg2.seq)
                            done = True
                            logging.logger.debug('Error requesting data for {}. {}'.format(str(metric.uri),str(rsp2.__dict__)))
                        else:
                            future = session._mark_message_undone(msg2.seq)
                            rsp2 = await future
                    elif rsp2.action == Actions.SEND_DATA_INTERVAL:
                        if start and rsp2.start == start:
                            done_start = True
                        if end and rsp2.end == end:
                            done_end = True
                        for row in rsp2.data[::-1]:
                            session._metrics_store.store(metric=rsp2.metric, ts=row[0], content=row[1])
                        if done_start and done_end:
                            session._mark_message_done(msg2.seq)
                            done = True
                            initialized[metric.uri]=True
                        else:
                            future = session._mark_message_undone(msg2.seq)
                            rsp2 = await future
        elif rsp.action == Actions.GENERIC_RESPONSE and rsp.status == Status.RESOURCE_NOT_FOUND:
            initialized[metric.uri]=True
        else:
            logging.logger.debug('Error hooking to uri {}. {}'.format(str(metric.uri),str(rsp.__dict__)))
    #by now, always initialize no matter if all metrics initialized ok. will add more parameters for fine grained initialization options
    if method.exec_on_load:
        now = pd.Timestamp('now', tz='utc')
        asyncio.ensure_future(method.f(ts=now, metrics=[], session=session))
    session._transfer_methods.enable_transfer_method(method.mid)
    logging.logger.debug('Transfer method initialized: {}'.format(str(method.f.__name__)))

async def send_samples(session, samples):
    if not isinstance(samples, list):
        return False
    grouped = {}
    for sample in samples:
        if isinstance(sample, Sample):
            try:
                grouped[sample.ts].append(sample)
            except KeyError:
                grouped[sample.ts]=[sample]
    logging.logger.debug('Grouped {}'.format(str((grouped))))
    tss = grouped.keys()
    msgs = []
    for ts in tss:
        items = grouped[ts]
        if len(items)>1:
            uris = []
            for item in items:
                uris.append({
                    'uri':item.metric.uri,
                    'type':item.metric.m_type.value,
                    'content':item.data,
                })
            msgs.append(messages.SendMultiData(ts=ts, uris=uris))
        elif isinstance(items[0].metric, Datasource):
            msgs.append(messages.SendDsData(uri=items[0].metric.uri, ts=ts, content=items[0].data))
        elif isinstance(items[0].metric, Datapoint):
            msgs.append(messages.SendDpData(uri=items[0].metric.uri, ts=ts, content=items[0].data))
        else:
            raise TypeError('invalid metric type')
    msgs.sort(key=lambda x: x.ts)
    result= {'success':True, 'error':''}
    for msg in msgs:
        rsp = await session._await_response(msg)
        session._mark_message_done(msg.seq)
        if rsp.status not in (Status.MESSAGE_ACCEPTED_FOR_PROCESSING, Status.MESSAGE_EXECUTION_OK):
            result['success']=False
            result['msg']=' '.join(('code:',rsp.error,rsp.reason))
    return result


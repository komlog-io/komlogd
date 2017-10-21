import asyncio
import json
import time
import pandas as pd
from komlogd.api.common import logging, exceptions
from komlogd.api.protocol import messages
from komlogd.api.protocol.codes import Status
from komlogd.api.model.schedules import OnUpdateSchedule, CronSchedule
from komlogd.api.model.metrics import Metrics, Datasource, Datapoint, Metric, Sample

async def send_samples(samples, irt=None):
    by_session_samples = {}
    for sample in samples:
        try:
            by_session_samples[sample.metric.session][sample.t].append(sample)
        except KeyError:
            if not sample.metric.session in by_session_samples:
                by_session_samples[sample.metric.session] = {sample.t:[sample]}
            else:
                by_session_samples[sample.metric.session][sample.t] = [sample]
    response = {'errors':[], 'success':True}
    for session, ts in by_session_samples.items():
        msgs = []
        for t,smpls in ts.items():
            if len(smpls)>1:
                uris = []
                for smp in smpls:
                    uris.append({
                        'uri':smp.metric.uri,
                        'type':smp.metric._m_type_.value,
                        'content':smp.value,
                    })
                msgs.append(messages.SendMultiData(t=t, uris=uris, irt=irt))
            elif isinstance(smpls[0].metric, Datasource):
                msgs.append(messages.SendDsData(uri=smpls[0].metric.uri, t=t, content=smpls[0].value, irt=irt))
            elif isinstance(smpls[0].metric, Datapoint):
                msgs.append(messages.SendDpData(uri=smpls[0].metric.uri, t=t, content=smpls[0].value, irt=irt))
        msgs.sort(key=lambda x: x.t)
        for msg in msgs:
            rsp = await session.send_message(msg)
            session._mark_message_done(msg.seq)
            if not isinstance(rsp, messages.GenericResponse):
                result = {'msg':msg, 'success':False, 'error':'Unexpected message type'}
                response['errors'].append(result)
                response['success'] = False
            elif rsp.status not in (Status.MESSAGE_ACCEPTED_FOR_PROCESSING, Status.MESSAGE_EXECUTION_OK):
                result = {'msg':msg, 'success':False, 'error':' '.join(('code:',str(rsp.error),rsp.reason))}
                response['errors'].append(result)
                response['success'] = False
    return response

async def request_data(metric, start, end, count):
    msg = messages.RequestData(uri=metric.uri, start=start, end=end, count=count)
    rsp = await metric.session.send_message(msg)
    done = False
    done_start = False if start else True
    done_end = False if end else True
    response = {'success':True, 'data':[],'error':None}
    while not done:
        if not isinstance(rsp, messages.KomlogMessage):
            metric.session._mark_message_done(msg.seq)
            done = True
            logging.logger.debug('Error requesting data for {}. {}'.format(str(metric.uri),'Unknown response'))
            response['success'] = False
            response['error'] = 'Unknown response'
        elif rsp.action == messages.Actions.GENERIC_RESPONSE:
            if rsp.status != Status.MESSAGE_ACCEPTED_FOR_PROCESSING:
                metric.session._mark_message_done(msg.seq)
                done = True
                logging.logger.debug('Error requesting data for {}. {}'.format(str(metric.uri),str(rsp.__dict__)))
                response['success'] = False
                response['error'] = str(rsp.__dict__)
            else:
                future = metric.session._mark_message_undone(msg.seq)
                rsp = await future
        elif rsp.action == messages.Actions.SEND_DATA_INTERVAL:
            if start and rsp.start == start:
                done_start = True
            if end and rsp.end == end:
                done_end = True
            for row in rsp.data[::-1]:
                response['data'].append((row[0],row[1]))
            if done_start and done_end:
                metric.session._mark_message_done(msg.seq)
                done = True
            else:
                future = metric.session._mark_message_undone(msg.seq)
                rsp = await future
    return response

async def hook_to_metric(metric):
    msg = messages.HookToUri(uri=metric.uri)
    rsp = await metric.session.send_message(msg)
    metric.session._mark_message_done(msg.seq)
    if not isinstance(rsp, messages.KomlogMessage):
        return {'hooked':False, 'exists':False}
    elif rsp.action == messages.Actions.GENERIC_RESPONSE and rsp.status == Status.MESSAGE_EXECUTION_OK:
        return {'hooked':True, 'exists':True}
    elif rsp.action == messages.Actions.GENERIC_RESPONSE and rsp.status == Status.RESOURCE_NOT_FOUND:
        #Komlog will notify us automatically when metric is created
        return {'hooked':True, 'exists':False}
    else:
        return {'hooked':False, 'exists':False}

async def send_info(metrics, irt=None):
    by_session_msgs = {}
    for metric in metrics:
        if isinstance(metric, Datasource):
            try:
                msg = messages.SendDsInfo(uri=metric.uri, supplies=metric.supplies, irt=irt)
                by_session_msgs[metric.session].append(msg)
            except KeyError:
                by_session_msgs[metric.session] = [msg]
    response = {'errors':[], 'success':True}
    for session, msgs in by_session_msgs.items():
        for msg in msgs:
            rsp = await session.send_message(msg)
            session._mark_message_done(msg.seq)
            if not isinstance(rsp, messages.GenericResponse):
                result = {'msg':msg, 'success':False, 'error':'Unexpected message type'}
                response['errors'].append(result)
                response['success'] = False
            elif rsp.status not in (Status.MESSAGE_ACCEPTED_FOR_PROCESSING, Status.MESSAGE_EXECUTION_OK):
                result = {'msg':msg, 'success':False, 'error':' '.join(('code:',str(rsp.error),rsp.reason))}
                response['errors'].append(result)
                response['success'] = False
    return response


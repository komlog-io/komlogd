import asyncio
import uuid
import unittest
import decimal
import json
import pandas as pd
from komlogd.api.model import test
from komlogd.api.session import KomlogSession
from komlogd.api.common import crypto
from komlogd.api.common.timeuuid import TimeUUID
from komlogd.api.protocol import messages
from komlogd.api.protocol.processing import message as prmsg
from komlogd.api.protocol.processing import procedure as prproc
from komlogd.api.model.metrics import Metrics, Datasource, Datapoint, Sample
from komlogd.api.model.transfer_methods import tmIndex
from komlogd.api.model.session import sessionIndex
from unittest.mock import call, Mock, patch
from komlogd.api.model.transactions import TransactionTask

loop = asyncio.get_event_loop()

class ApiProtocolProcessingProcedureTest(unittest.TestCase):

    @test.sync(loop)
    async def test_send_samples_success(self):
        ''' send_samples should create messages and send through the sessions to Komlog '''
        username1 = 'username1'
        privkey1=crypto.generate_rsa_key()
        username2 = 'username2'
        privkey2=crypto.generate_rsa_key()
        session1 = KomlogSession(username=username1, privkey=privkey1)
        session2 = KomlogSession(username=username2, privkey=privkey2)
        session1.send_message = test.AsyncMock(return_value = None)
        session2.send_message = test.AsyncMock(return_value = None)
        t_common = TimeUUID()
        t1 = TimeUUID()
        t2 = TimeUUID()
        samples_s1 = [
            Sample(Datasource('datasource1',session=session1),t_common,'value'),
            Sample(Datasource('datasource2',session=session1),t_common,'value'),
            Sample(Datasource('datasource3',session=session1),t_common,'value'),
            Sample(Datasource('datasource4',session=session1),t_common,'value'),
            Sample(Datapoint('datapoint1',session=session1),t_common,1),
            Sample(Datapoint('datapoint2',session=session1),t_common,1),
            Sample(Datapoint('datapoint3',session=session1),t_common,1),
            Sample(Datapoint('datapoint4',session=session1),t_common,1),
            Sample(Datasource('datasource5',session=session1),t1,'value'),
            Sample(Datapoint('datapoint5',session=session1),t2,1),
        ]
        samples_s2 = [
            Sample(Datasource('datasource1',session=session2),t_common,'value'),
            Sample(Datasource('datasource2',session=session2),t_common,'value'),
            Sample(Datasource('datasource3',session=session2),t_common,'value'),
            Sample(Datasource('datasource4',session=session2),t_common,'value'),
            Sample(Datapoint('datapoint1',session=session2),t_common,1),
            Sample(Datapoint('datapoint2',session=session2),t_common,1),
            Sample(Datapoint('datapoint3',session=session2),t_common,1),
            Sample(Datapoint('datapoint4',session=session2),t_common,1),
            Sample(Datasource('datasource5',session=session2),t1,'value'),
            Sample(Datapoint('datapoint5',session=session2),t2,1),
        ]
        total_samples = []
        for smp in samples_s1:
            total_samples.append(smp)
        for smp in samples_s2:
            total_samples.append(smp)
        response = await prproc.send_samples(total_samples)
        self.assertEqual(session1.send_message.call_count, 3)
        self.assertEqual(session2.send_message.call_count, 3)
        self.assertEqual(response['success'],False)
        self.assertEqual(len(response['errors']),6)
        for i,m in enumerate(response['errors']):
            self.assertEqual(m['success'],False)
            self.assertEqual(m['error'],'Unexpected message type')
            msg = m['msg']
            if i == 0:
                self.assertTrue(isinstance(msg, messages.SendMultiData))
                self.assertEqual(msg.t, t_common)
                self.assertEqual(msg.uris, [{'uri':s.metric.uri, 'type':s.metric._m_type_, 'content':s.value} for s in samples_s1[:-2]])
            elif i==1:
                self.assertTrue(isinstance(msg, messages.SendDsData))
                self.assertEqual(msg.uri, samples_s1[-2].metric.uri)
                self.assertEqual(msg.t, samples_s1[-2].t)
                self.assertEqual(msg.content, samples_s1[-2].value)
            elif i==2:
                self.assertTrue(isinstance(msg, messages.SendDpData))
                self.assertEqual(msg.uri, samples_s1[-1].metric.uri)
                self.assertEqual(msg.t, samples_s1[-1].t)
                self.assertEqual(msg.content, samples_s1[-1].value)
            elif i == 3:
                self.assertTrue(isinstance(msg, messages.SendMultiData))
                self.assertEqual(msg.t, t_common)
                self.assertEqual(msg.uris, [{'uri':s.metric.uri, 'type':s.metric._m_type_, 'content':s.value} for s in samples_s2[:-2]])
            elif i==4:
                self.assertTrue(isinstance(msg, messages.SendDsData))
                self.assertEqual(msg.uri, samples_s2[-2].metric.uri)
                self.assertEqual(msg.t, samples_s2[-2].t)
                self.assertEqual(msg.content, samples_s2[-2].value)
            elif i==5:
                self.assertTrue(isinstance(msg, messages.SendDpData))
                self.assertEqual(msg.uri, samples_s2[-1].metric.uri)
                self.assertEqual(msg.t, samples_s2[-1].t)
                self.assertEqual(msg.content, samples_s2[-1].value)
        sessionIndex.unregister_session(session1.sid)
        sessionIndex.unregister_session(session2.sid)

    @test.sync(loop)
    async def test_request_data_failure_unknown_response(self):
        ''' request_data should fail if we receive and unknown response '''
        username1 = 'username1'
        privkey1=crypto.generate_rsa_key()
        session1 = KomlogSession(username=username1, privkey=privkey1)
        session1.send_message = test.AsyncMock(return_value = None)
        start = TimeUUID(100)
        end = TimeUUID(300)
        count = 10
        metric = Datasource('my_ds', session=session1)
        response = await prproc.request_data(metric, start, end, count)
        self.assertEqual(session1.send_message.call_count, 1)
        self.assertEqual(response['success'],False)
        self.assertEqual(response['error'],'Unknown response')
        sessionIndex.unregister_session(session1.sid)

    @test.sync(loop)
    async def test_hook_to_metric_failure_invalid_response(self):
        ''' hook_to_metric should fail if we receive and unknown response '''
        username1 = 'username1'
        privkey1=crypto.generate_rsa_key()
        session1 = KomlogSession(username=username1, privkey=privkey1)
        session1.send_message = test.AsyncMock(return_value = None)
        metric = Datasource('my_ds', session=session1)
        response = await prproc.hook_to_metric(metric)
        self.assertEqual(session1.send_message.call_count, 1)
        self.assertEqual(response['hooked'],False)
        self.assertEqual(response['exists'],False)
        sessionIndex.unregister_session(session1.sid)


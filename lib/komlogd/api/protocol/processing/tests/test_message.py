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
from komlogd.api.model.metrics import Metrics, Datasource, Datapoint, Sample
from komlogd.api.model.transfer_methods import tmIndex
from komlogd.api.model.session import sessionIndex
from unittest.mock import call, Mock, patch
from komlogd.api.model.transactions import TransactionTask

loop = asyncio.get_event_loop()

class ApiProtocolProcessingMessageTest(unittest.TestCase):

    @test.sync(loop)
    async def test_process_message_send_multi_data_success(self):
        ''' process_message_send_multi_data should store contents and notify tmIndex '''
        try:
            username = 'username'
            privkey=crypto.generate_rsa_key()
            t = TimeUUID()
            uris = [
                {'uri':'datasource1','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource2','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource3','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource4','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datapoint1','type':Metrics.DATAPOINT.value,'content':'1232'},
                {'uri':'datapoint2','type':Metrics.DATAPOINT.value,'content':'1233'},
                {'uri':'datapoint3','type':Metrics.DATAPOINT.value,'content':'1234'},
                {'uri':'datapoint4','type':Metrics.DATAPOINT.value,'content':'1235'}
            ]
            msg = messages.SendMultiData(t, uris)
            session = KomlogSession(username=username, privkey=privkey)
            bck = tmIndex.metrics_updated
            tmIndex.metrics_updated = Mock(return_value = None)
            self.assertIsNone(prmsg.process_message_send_multi_data(msg, session))
            metrics = [
                Datasource('datasource1',session=session),
                Datasource('datasource2',session=session),
                Datasource('datasource3',session=session),
                Datasource('datasource4',session=session),
                Datapoint('datapoint1',session=session),
                Datapoint('datapoint2',session=session),
                Datapoint('datapoint3',session=session),
                Datapoint('datapoint4',session=session)
            ]
            for uri in uris:
                if uri['type']==Metrics.DATASOURCE.value:
                    smp = Sample(Datasource(uri['uri'],session=session),t,uri['content'])
                else:
                    smp = Sample(Datapoint(uri['uri'],session=session),t,uri['content'])
                self.assertTrue(session.store.is_in(smp.metric, smp.t, smp.value))
            self.assertEqual(tmIndex.metrics_updated.call_args[1]['t'],t)
            self.assertEqual(tmIndex.metrics_updated.call_args[1]['metrics'],metrics)
            sessionIndex.unregister_session(session.sid)
            tmIndex.metrics_updated = bck
        except:
            tmIndex.metrics_updated = bck
            raise

    @test.sync(loop)
    async def test_process_message_send_multi_data_success_no_notify_already_stored_values(self):
        ''' process_message_send_multi_data should store contents and notify tmIndex '''
        try:
            username = 'username'
            privkey=crypto.generate_rsa_key()
            t = TimeUUID()
            uris = [
                {'uri':'datasource1','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource2','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource3','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource4','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datapoint1','type':Metrics.DATAPOINT.value,'content':'1232'},
                {'uri':'datapoint2','type':Metrics.DATAPOINT.value,'content':'1233'},
                {'uri':'datapoint3','type':Metrics.DATAPOINT.value,'content':'1234'},
                {'uri':'datapoint4','type':Metrics.DATAPOINT.value,'content':'1235'}
            ]
            msg = messages.SendMultiData(t, uris)
            session = KomlogSession(username=username, privkey=privkey)
            bck = tmIndex.metrics_updated
            tmIndex.metrics_updated = Mock(return_value = None)
            self.assertIsNone(prmsg.process_message_send_multi_data(msg, session))
            metrics = [
                Datasource('datasource1',session=session),
                Datasource('datasource2',session=session),
                Datasource('datasource3',session=session),
                Datasource('datasource4',session=session),
                Datapoint('datapoint1',session=session),
                Datapoint('datapoint2',session=session),
                Datapoint('datapoint3',session=session),
                Datapoint('datapoint4',session=session)
            ]
            for uri in uris:
                if uri['type']==Metrics.DATASOURCE.value:
                    smp = Sample(Datasource(uri['uri'],session=session),t,uri['content'])
                else:
                    smp = Sample(Datapoint(uri['uri'],session=session),t,uri['content'])
                self.assertTrue(session.store.is_in(smp.metric, smp.t, smp.value))
            self.assertEqual(tmIndex.metrics_updated.call_args[1]['t'],t)
            self.assertEqual(tmIndex.metrics_updated.call_args[1]['metrics'],metrics)
            updated_uris = [
                {'uri':'datasource1','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource2','type':Metrics.DATASOURCE.value,'content':'ds data2'},
                {'uri':'datasource3','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource4','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datasource5','type':Metrics.DATASOURCE.value,'content':'ds data'},
                {'uri':'datapoint1','type':Metrics.DATAPOINT.value,'content':'1233'},
                {'uri':'datapoint2','type':Metrics.DATAPOINT.value,'content':'1234'},
                {'uri':'datapoint3','type':Metrics.DATAPOINT.value,'content':'1234'},
                {'uri':'datapoint4','type':Metrics.DATAPOINT.value,'content':'1235'},
                {'uri':'datapoint5','type':Metrics.DATAPOINT.value,'content':'1235'}
            ]
            msg = messages.SendMultiData(t, updated_uris)
            updated_metrics = [
                Datasource('datasource2',session=session),
                Datasource('datasource5',session=session),
                Datapoint('datapoint1',session=session),
                Datapoint('datapoint2',session=session),
                Datapoint('datapoint5',session=session)
            ]
            self.assertIsNone(prmsg.process_message_send_multi_data(msg, session))
            for uri in updated_uris:
                if uri['type']==Metrics.DATASOURCE.value:
                    smp = Sample(Datasource(uri['uri']),t,uri['content'])
                else:
                    smp = Sample(Datapoint(uri['uri']),t,uri['content'])
                self.assertTrue(session.store.is_in(smp.metric, smp.t, smp.value))
            self.assertEqual(tmIndex.metrics_updated.call_args[1]['t'],t)
            self.assertEqual(tmIndex.metrics_updated.call_args[1]['metrics'],updated_metrics)
            sessionIndex.unregister_session(session.sid)
            tmIndex.metrics_updated = bck
        except:
            tmIndex.metrics_updated = bck
            raise

    @test.sync(loop)
    async def test_process_message_send_data_interval_success_dp_data(self):
        ''' process_message_send_data_interval should store contents in session store '''
        username = 'username'
        privkey=crypto.generate_rsa_key()
        uri = 'my_datapoint'
        m_type = Metrics.DATAPOINT
        start = TimeUUID(1)
        end = TimeUUID(3000)
        data_json = json.dumps([(TimeUUID(i).hex,i) for i in range(1,100)])
        data = json.loads(data_json)
        session = KomlogSession(username=username, privkey=privkey)
        msg = messages.SendDataInterval(uri, m_type, start, end, data)
        self.assertIsNone(prmsg.process_message_send_data_interval(msg, session))
        for d in data:
            smp = Sample(Datapoint(uri,session),TimeUUID(s=d[0]),d[1])
            self.assertTrue(session.store.is_in(smp.metric, smp.t, smp.value))
        sessionIndex.unregister_session(session.sid)

    @test.sync(loop)
    async def test_process_message_send_data_interval_success_ds_data(self):
        ''' process_message_send_data_interval should store contents in session store '''
        username = 'username'
        privkey=crypto.generate_rsa_key()
        uri = 'my_datasource'
        m_type = Metrics.DATASOURCE
        start = TimeUUID(1)
        end = TimeUUID(3000)
        data_json = json.dumps([(TimeUUID(i).hex,'sample '+str(i)) for i in range(1,100)])
        data = json.loads(data_json)
        session = KomlogSession(username=username, privkey=privkey)
        msg = messages.SendDataInterval(uri, m_type, start, end, data)
        self.assertIsNone(prmsg.process_message_send_data_interval(msg, session))
        for d in data:
            smp = Sample(Datasource(uri,session),TimeUUID(s=d[0]),d[1])
            self.assertTrue(session.store.is_in(smp.metric, smp.t, smp.value))
        sessionIndex.unregister_session(session.sid)


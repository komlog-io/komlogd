import unittest
import decimal
import pandas as pd
from komlogd.api.model import messages, orm
from komlogd.api.model.types import Metrics, Actions

class ApiModelMessagesTest(unittest.TestCase):

    def test_version(self):
        ''' Protocol version '''
        self.assertEqual(messages.KomlogMessage._version_,1)

    def test_KomlogMessage_failure_cannot_instantiate_directly(self):
        ''' Direct instantiation of KomlogMessage objects is not allowed '''
        with self.assertRaises(TypeError) as cm:
            msg=messages.KomlogMessage()

    def test_KomlogMessage_version_modification_not_allowed(self):
        ''' We cannot modify the version in a KomlogMessage derived class '''
        uri='uri'
        ts=pd.Timestamp('now',tz='utc')
        content='content'
        msg=messages.SendDsData(uri=uri, ts=ts, content=content)
        with self.assertRaises(TypeError) as cm:
            msg.v=2

    def test_KomlogMessage_action_modification_not_allowed(self):
        ''' We cannot modify the action in a KomlogMessage derived class '''
        uri='uri'
        ts=pd.Timestamp('now',tz='utc')
        content='content'
        msg=messages.SendDsData(uri=uri, ts=ts, content=content)
        with self.assertRaises(TypeError) as cm:
            msg.action=Actions.SEND_MULTI_DATA

    def test_KomlogMessage_load_from_dict_failure_message_not_supported_no_dict(self):
        ''' KomlogMessage.load_from_dict should fail if msg passed is not a dict '''
        msg='message'
        with self.assertRaises(TypeError) as cm:
            messages.KomlogMessage.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Message not supported')

    def test_KomlogMessage_load_from_dict_failure_message_not_supported_no_action(self):
        ''' KomlogMessage.load_from_dict should fail if msg passed has no action field '''
        msg={'payload':'payload'}
        with self.assertRaises(TypeError) as cm:
            messages.KomlogMessage.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Message not supported')

    def test_KomlogMessage_load_from_dict_failure_unknown_action(self):
        ''' KomlogMessage.load_from_dict should fail if msg action is not in catalog '''
        msg={'action':'my_custom_action','payload':'payload'}
        with self.assertRaises(TypeError) as cm:
            messages.KomlogMessage.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Unknown message type')

    def test_KomlogMessage_load_from_dict_failure_not_implemented_in_this_version(self):
        ''' KomlogMessage.load_from_dict should fail if msg type has not implemented load_from_dict'''
        for action in (Actions.SEND_DS_DATA, Actions.SEND_DP_DATA, Actions.HOOK_TO_URI, Actions.UNHOOK_FROM_URI):
            msg={'action':action.value,'payload':'payload'}
            with self.assertRaises(NotImplementedError) as cm:
                messages.KomlogMessage.load_from_dict(msg)

    def test_SendDsData_failure_invalid_uri(self):
        ''' SendDsData creation should fail if uri is not valid '''
        uri='..not_valid'
        ts=pd.Timestamp('now',tz='utc')
        content='content'
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDsData(uri=uri, ts=ts, content=content)

    def test_SendDsData_failure_invalid_ts(self):
        ''' SendDsData creation should fail if ts is not valid '''
        uri='valid.uri'
        ts=-1.232
        content='content'
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDsData(uri=uri, ts=ts, content=content)

    def test_SendDsData_failure_invalid_content(self):
        ''' SendDsData creation should fail if content is not valid '''
        uri='valid.uri'
        ts=pd.Timestamp('now',tz='utc')
        content=223
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDsData(uri=uri, ts=ts, content=content)

    def test_SendDsData_success(self):
        ''' SendDsData creation should succeed if parameters are valid '''
        uri='valid.uri'
        ts=pd.Timestamp('now',tz='utc')
        content='content\n'
        msg=messages.SendDsData(uri=uri, ts=ts, content=content)
        self.assertTrue(isinstance(msg, messages.SendDsData))
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.SEND_DS_DATA)
        self.assertEqual(msg.uri,uri)
        self.assertEqual(msg.ts,ts)
        self.assertEqual(msg.content,content)

    def test_SendDsData_to_dict_success(self):
        ''' SendDsData should return a valid dict representation of the object '''
        uri='valid.uri'
        ts=pd.Timestamp('now',tz='utc')
        content='content\n'
        msg=messages.SendDsData(uri=uri, ts=ts, content=content)
        self.assertTrue(isinstance(msg, messages.SendDsData))
        self.assertEqual(
            msg.to_dict(),
            {'v':messages.KomlogMessage._version_, 'action':Actions.SEND_DS_DATA.value,'payload':
                {'uri':uri,'ts':ts.isoformat(),'content':content}
            }
        )

    def test_SendDpData_failure_invalid_uri(self):
        ''' SendDpData creation should fail if uri is not valid '''
        uri='..not_valid'
        ts=pd.Timestamp('now',tz='utc')
        content='content'
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDpData(uri=uri, ts=ts, content=content)

    def test_SendDpData_failure_invalid_ts(self):
        ''' SendDpData creation should fail if ts is not valid '''
        uri='valid.uri'
        ts=-1.232
        content='content'
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDpData(uri=uri, ts=ts, content=content)

    def test_SendDpData_failure_invalid_content(self):
        ''' SendDpData creation should fail if content is not valid '''
        uri='valid.uri'
        ts=pd.Timestamp('now',tz='utc')
        content='223,223'
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDpData(uri=uri, ts=ts, content=content)

    def test_SendDpData_success(self):
        ''' SendDpData creation should succeed if parameters are valid '''
        uri='valid.uri'
        ts=pd.Timestamp('now',tz='utc')
        content='123.32'
        msg=messages.SendDpData(uri=uri, ts=ts, content=content)
        self.assertTrue(isinstance(msg, messages.SendDpData))
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.SEND_DP_DATA)
        self.assertEqual(msg.uri, uri)
        self.assertEqual(msg.ts,ts)
        self.assertEqual(msg.content,decimal.Decimal(content))

    def test_SendDpData_to_dict_success(self):
        ''' SendDpData should return a valid dict representation of the object '''
        uri='valid.uri'
        ts=pd.Timestamp('now',tz='utc')
        content=decimal.Decimal('300.2')
        msg=messages.SendDpData(uri=uri, ts=ts, content=content)
        self.assertTrue(isinstance(msg, messages.SendDpData))
        self.assertEqual(
            msg.to_dict(),
            {'v':messages.KomlogMessage._version_, 'action':Actions.SEND_DP_DATA.value,'payload':
                {'uri':uri,'ts':ts.isoformat(),'content':str(content)}
            }
        )

    def test_SendMultiData_failure_none_uris(self):
        ''' SendMultiData creation should fail if uris is None '''
        uris=None
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_invalid_type(self):
        ''' SendMultiData creation should fail if uris is not a list '''
        uris='uri'
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_invalid_item_type(self):
        ''' SendMultiData creation should fail if uris item is not a dict '''
        uris=['uri']
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_uri_key_not_found(self):
        ''' SendMultiData creation should fail if uris item has not uri '''
        uris=[{'type':Metrics.DATASOURCE,'content':'content '}]
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_content_key_not_found(self):
        ''' SendMultiData creation should fail if uris item has not uri '''
        uris=[{'uri':'uri','type':Metrics.DATAPOINT}]
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_type_key_not_found(self):
        ''' SendMultiData creation should fail if uris item has not uri '''
        uris=[{'uri':'uri','content':'content'}]
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_invalid_item_uri(self):
        ''' SendMultiData creation should fail if uris item has invalid uri '''
        uris=[{'uri':'invalid uri','type':Metrics.DATASOURCE,'content':'content'}]
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_invalid_item_content_for_metric_datasource(self):
        ''' SendMultiData creation should fail if uris item has invalid uri '''
        uris=[{'uri':'valid_uri','type':Metrics.DATASOURCE, 'content':{'content'}}]
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_uris_invalid_item_content_for_metric_datapoint(self):
        ''' SendMultiData creation should fail if uris item has invalid uri '''
        uris=[{'uri':'valid_uri','type':Metrics.DATAPOINT, 'content':'content'}]
        ts=pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_invalid_ts(self):
        ''' SendMultiData creation should fail if ts is not valid '''
        uris=[{'uri':'uri','content':'333','type':Metrics.DATAPOINT}]
        ts=-1.232
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_failure_none_ts(self):
        ''' SendMultiData creation should fail if ts is None '''
        uris=[{'uri':'uri','content':'333','type':Metrics.DATAPOINT}]
        ts=None
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData(uris=uris, ts=ts)

    def test_SendMultiData_success(self):
        ''' SendMultiData creation should succeed if parameters are valid '''
        uris=[
            {'uri':'uri.dp','content':decimal.Decimal('333'),'type':Metrics.DATAPOINT},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        message_uris=[
            {'uri':'uri.dp','content':decimal.Decimal('333'),'type':Metrics.DATAPOINT},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE},
        ]
        ts=pd.Timestamp('now',tz='utc')
        msg=messages.SendMultiData(uris=uris, ts=ts)
        self.assertTrue(isinstance(msg, messages.SendMultiData))
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.SEND_MULTI_DATA)
        self.assertEqual(sorted(msg.uris, key=lambda x: x['uri']), sorted(message_uris, key=lambda x: x['uri']))
        self.assertEqual(msg.ts,ts)

    def test_SendMultiData_success_extra_uris_fields_not_propagated(self):
        ''' SendMultiData creation should succeed and non standard uris keys should not be propagated '''
        uris=[
            {'uri':'uri.dp','content':'333','type':Metrics.DATAPOINT,'non_standard':'content'},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE,'non_standard':'content'},
        ]
        standard_uris=[
            {'uri':'uri.dp','content':decimal.Decimal('333'),'type':Metrics.DATAPOINT},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE},
        ]
        ts=pd.Timestamp('now',tz='utc')
        msg=messages.SendMultiData(uris=uris, ts=ts)
        self.assertTrue(isinstance(msg, messages.SendMultiData))
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.SEND_MULTI_DATA)
        self.assertEqual(sorted(msg.uris, key=lambda x:x['uri']), sorted(standard_uris, key=lambda x: x['uri']))
        self.assertEqual(msg.ts,ts)

    def test_SendMultiData_to_dict_success(self):
        ''' SendMultiData should return a valid dict representation of the object '''
        self.maxDiff=None
        uris=[
            {'uri':'uri.dp','content':decimal.Decimal('0.333'),'type':Metrics.DATAPOINT},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE},
        ]
        dict_uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        ts=pd.Timestamp('now',tz='utc')
        msg=messages.SendMultiData(uris=uris, ts=ts)
        self.assertTrue(isinstance(msg, messages.SendMultiData))
        dict_msg=msg.to_dict()
        dict_msg['payload']['uris']=sorted(dict_msg['payload']['uris'], key=lambda x:x['uri'])
        self.assertEqual(
            dict_msg,
            {'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,'payload':
                {'uris':dict_uris,'ts':ts.isoformat()}
            }
        )

    def test_SendMultiData_load_from_dict_failure_no_dict_passed(self):
        ''' SendMultiData.load_from_dict should fail if parameter is not a dict '''
        data='data'
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_no_version(self):
        ''' SendMultiData.load_from_dict should fail if data has no version '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'action':Actions.SEND_MULTI_DATA.value,'payload':
            {'uris':uris,'ts':ts.isoformat()}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_no_action(self):
        ''' SendMultiData.load_from_dict should fail if data has no action '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'payload':
            {'uris':uris,'ts':ts.isoformat()}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_no_payload(self):
        ''' SendMultiData.load_from_dict should fail if data has no payload '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_invalid_version(self):
        ''' SendMultiData.load_from_dict should fail if data version is not valid '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':'a', 'action':Actions.SEND_MULTI_DATA.value,'payload':
            {'uris':uris,'ts':ts.isoformat()}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_invalid_action(self):
        ''' SendMultiData.load_from_dict should fail if data has invalid action '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_DS_DATA.value,'payload':
            {'uris':uris,'ts':ts.isoformat()}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_payload_no_dict(self):
        ''' SendMultiData.load_from_dict should fail if payload is not a dict '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,'payload':
            [{'uris':uris,'ts':ts.isoformat()},]
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_payload_has_no_ts(self):
        ''' SendMultiData.load_from_dict should fail if payload has no ts '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,'payload':
            {'uris':uris}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_payload_has_no_uris(self):
        ''' SendMultiData.load_from_dict should fail if payload has no uris '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,'payload':
            {'ts':ts.isoformat()}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendMultiData_load_from_dict_failure_invalid_ts(self):
        ''' SendMultiData.load_from_dict should fail if data has no version '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','content':'0.333','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','content':'content','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,'payload':
            {'uris':uris,'ts':'1'}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'ts type is not valid')

    def test_SendMultiData_load_from_dict_failure_invalid_uris(self):
        ''' SendMultiData.load_from_dict should fail if data has no version '''
        ts=pd.Timestamp('now',tz='utc')
        uris=[
            {'uri':'uri.dp','type':Metrics.DATAPOINT.value},
            {'uri':'uri.ds','type':Metrics.DATASOURCE.value},
        ]
        data={'v':messages.KomlogMessage._version_, 'action':Actions.SEND_MULTI_DATA.value,'payload':
            {'uris':uris,'ts':ts.isoformat()}
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendMultiData.load_from_dict(data)
        self.assertEqual(str(cm.exception), 'Uris parameter not valid')

    def test_HookToUri_failure_none_uri(self):
        ''' HookToUri creation should fail if uri is None '''
        uri=None
        with self.assertRaises(TypeError) as cm:
            msg=messages.HookToUri(uri=uri)

    def test_HookToUri_failure_invalid_uri(self):
        ''' HookToUri creation should fail if uri is not valid '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            msg=messages.HookToUri(uri=uri)

    def test_HookToUri_success(self):
        ''' HookToUri creation should succeed if parameters are valid '''
        uri='uri'
        msg=messages.HookToUri(uri=uri)
        self.assertTrue(isinstance(msg, messages.HookToUri))
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.HOOK_TO_URI)
        self.assertEqual(msg.uri, uri)

    def test_HookToUri_to_dict_success(self):
        ''' HookToUri should return a valid dict representation of the object '''
        uri='uri'
        msg=messages.HookToUri(uri=uri)
        self.assertTrue(isinstance(msg, messages.HookToUri))
        self.assertEqual(
            msg.to_dict(),
            {'v':messages.KomlogMessage._version_, 'action':Actions.HOOK_TO_URI.value,'payload':
                {'uri':uri}
            }
        )

    def test_UnHookFromUri_failure_none_uri(self):
        ''' UnHookFromUri creation should fail if uri is None '''
        uri=None
        with self.assertRaises(TypeError) as cm:
            msg=messages.UnHookFromUri(uri=uri)

    def test_UnHookFromUri_failure_invalid_uri(self):
        ''' UnHookFromUri creation should fail if uri is not valid '''
        uri='invalid uri'
        with self.assertRaises(TypeError) as cm:
            msg=messages.UnHookFromUri(uri=uri)

    def test_UnHookFromUri_success(self):
        ''' UnHookFromUri creation should succeed if parameters are valid '''
        uri='uri'
        msg=messages.UnHookFromUri(uri=uri)
        self.assertTrue(isinstance(msg, messages.UnHookFromUri))
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.UNHOOK_FROM_URI)
        self.assertEqual(msg.uri, uri)

    def test_UnHookFromUri_to_dict_success(self):
        ''' UnHookFromUri should return a valid dict representation of the object '''
        uri='uri'
        msg=messages.UnHookFromUri(uri=uri)
        self.assertTrue(isinstance(msg, messages.UnHookFromUri))
        self.assertEqual(
            msg.to_dict(),
            {'v':messages.KomlogMessage._version_, 'action':Actions.UNHOOK_FROM_URI.value,'payload':
                {'uri':uri}
            }
        )

    def test_RequestData_failure_invalid_uri(self):
        ''' creating a new RequestData instance should fail if uri is not valid '''
        uri='invalid uri'
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            messages.RequestData(uri=uri, start=start, end=end)
        self.assertEqual(str(cm.exception), 'uri is not valid: '+uri)

    def test_RequestData_failure_invalid_start(self):
        ''' creating a new RequestData instance should fail if start is not valid '''
        uri='valid.uri'
        start = 123123
        end = pd.Timestamp('now',tz='utc')
        with self.assertRaises(TypeError) as cm:
            messages.RequestData(uri=uri, start=start, end=end)
        self.assertEqual(str(cm.exception), 'ts type is not valid')

    def test_RequestData_failure_invalid_end(self):
        ''' creating a new RequestData instance should fail if end is not valid '''
        uri='valid.uri'
        start = pd.Timestamp('now',tz='utc')
        end = 123123
        with self.assertRaises(TypeError) as cm:
            messages.RequestData(uri=uri, start=start, end=end)
        self.assertEqual(str(cm.exception), 'ts type is not valid')

    def test_RequestData_success(self):
        ''' creating a new RequestData instance should succeed '''
        uri='valid.uri'
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        msg=messages.RequestData(uri=uri, start=start, end=end)
        self.assertEqual(msg.uri, uri)
        self.assertEqual(msg.start, start)
        self.assertEqual(msg.end, end)

    def test_RequestData_to_dict_success(self):
        ''' RequestData.to_dict() should return a valid representation of the instance '''
        uri='valid.uri'
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        msg=messages.RequestData(uri=uri, start=start, end=end)
        self.assertEqual(msg.uri, uri)
        self.assertEqual(msg.start, start)
        self.assertEqual(msg.end, end)
        d=msg.to_dict()
        expected = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.REQUEST_DATA.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'count':None,
            }
        }
        self.assertEqual(d,expected)

    def test_SendDataInterval_failure_invalid_metric(self):
        ''' creating a new RequestData instance should fail if metric is not valid.
            Metric should be of type Datasource or Datapoint. '''
        metric=orm.Metric(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data = []
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'Invalid metric type')

    def test_SendDataInterval_failure_invalid_start(self):
        ''' creating a new RequestData instance should fail if start is not valid. '''
        metric=orm.Datasource(uri='valid.uri')
        start = 213123
        end = pd.Timestamp('now',tz='utc')
        data = []
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'ts type is not valid')

    def test_SendDataInterval_failure_invalid_end(self):
        ''' creating a new RequestData instance should fail if end is not valid. '''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = 213123
        data = []
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'ts type is not valid')

    def test_SendDataInterval_failure_invalid_data_not_a_list(self):
        ''' creating a new RequestData instance should fail if data is not a list. '''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data = tuple()
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'Invalid data')

    def test_SendDataInterval_failure_invalid_data_item_not_a_list(self):
        ''' creating a new RequestData instance should fail if a data item is not a list.'''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[{'set'}]
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'Invalid data')

    def test_SendDataInterval_failure_invalid_data_item_does_not_have_two_items(self):
        ''' creating a new RequestData instance should fail if a data item does not have two items'''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'23232'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 253232323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 253232323','extra!'],
        ]
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'Invalid data')

    def test_SendDataInterval_failure_invalid_data_item_ts_is_invalid(self):
        ''' creating a new RequestData instance should fail if a data item ts is invalid '''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[
            [234234,'23232'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 253232323'],
        ]
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'ts type is not valid')

    def test_SendDataInterval_failure_invalid_data_item_content_is_invalid_dp_content(self):
        ''' creating a new RequestData instance should fail if a data item content is invalid '''
        metric=orm.Datapoint(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 253232323'],
        ]
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'datapoint value not valid')

    def test_SendDataInterval_failure_invalid_data_item_content_is_invalid_ds_content(self):
        ''' creating a new RequestData instance should fail if a data item content is invalid '''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),['ds content 323']],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 253232323'],
        ]
        with self.assertRaises(TypeError) as cm:
            messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(str(cm.exception), 'content is not a string')

    def test_SendDataInterval_success_datasource(self):
        ''' creating a new RequestData instance should succeed '''
        metric=orm.Datasource(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'ds content 253232323'],
        ]
        msg=messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(msg.metric, metric)
        self.assertEqual(msg.start, start)
        self.assertEqual(msg.end, end)
        self.assertEqual(sorted(msg.data, key=lambda x: x[0]),sorted([(pd.Timestamp(item[0]),item[1]) for item in data], key=lambda x:x[0]))

    def test_SendDataInterval_success_datapoint(self):
        ''' creating a new RequestData instance should succeed '''
        metric=orm.Datapoint(uri='valid.uri')
        start = pd.Timestamp('now',tz='utc')
        end = pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'253232323'],
        ]
        msg=messages.SendDataInterval(metric=metric, start=start, end=end, data=data)
        self.assertEqual(msg.metric, metric)
        self.assertEqual(msg.start, start)
        self.assertEqual(msg.end, end)
        self.assertEqual(sorted(msg.data, key=lambda x: x[0]),sorted([(pd.Timestamp(item[0]),decimal.Decimal(item[1])) for item in data], key=lambda x:x[0]))

    def test_SendDataInterval_load_from_dict_failure_invalid_msg_type(self):
        ''' creating a new RequestData from a serialization should fail if serialization is not of type dict '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = [{
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }]
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_no_version(self):
        ''' creating a new RequestData from a serialization should fail if msg has no version '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'av':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_version(self):
        ''' creating a new RequestData from a serialization should fail if msg has invalid version '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':[messages.KomlogMessage._version_],
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_no_action(self):
        ''' creating a new RequestData from a serialization should fail if msg has no action'''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'the_action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_action(self):
        ''' creating a new RequestData from a serialization should fail if msg has invalid action'''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.REQUEST_DATA.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_no_payload(self):
        ''' creating a new RequestData from a serialization should fail if msg has no payload'''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'the_payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_type(self):
        ''' creating a new RequestData from a serialization should fail if msg has invalid payload type'''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':[{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }]
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_no_uri(self):
        ''' creating a new RequestData from a serialization should fail if msg payload has no uri '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'ari':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_uri_type(self):
        ''' creating a new RequestData from a serialization should fail if msg payload uri has invalid type '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':[uri],
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_uri_no_uri(self):
        ''' creating a new RequestData from a serialization should fail if msg payload uri has no uri '''
        uri={'ari':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_uri_no_type(self):
        ''' creating a new RequestData from a serialization should fail if msg payload uri has no type '''
        uri={'uri':'valid.uri','taip':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_no_start(self):
        ''' creating a new RequestData from a serialization should fail if payload has no start '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'estart':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_no_end(self):
        ''' creating a new RequestData from a serialization should fail if payload has no end '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'theend':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_no_data(self):
        ''' creating a new RequestData from a serialization should fail if payload has no data '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'thedata':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Could not load message, invalid type')

    def test_SendDataInterval_load_from_dict_failure_invalid_payload_uri_type(self):
        ''' creating a new RequestData from a serialization should fail if payload has no data '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        msg = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        with self.assertRaises(TypeError) as cm:
            msg=messages.SendDataInterval.load_from_dict(msg)
        self.assertEqual(str(cm.exception), 'Invalid metric type')

    def test_SendDataInterval_load_from_dict_success_datasource(self):
        ''' creating a new RequestData from a serialization should succeed '''
        uri={'uri':'valid.uri','type':Metrics.DATASOURCE.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'content 323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'content 253232323'],
        ]
        source = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        msg=messages.SendDataInterval.load_from_dict(source)
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.SEND_DATA_INTERVAL)
        self.assertEqual(msg.metric, orm.Datasource(uri=uri['uri']))
        self.assertEqual(msg.start, pd.Timestamp(start))
        self.assertEqual(msg.end, pd.Timestamp(end))
        self.assertEqual(sorted(msg.data, key=lambda x:x[0]), sorted([(pd.Timestamp(item[0]),item[1]) for item in data]))

    def test_SendDataInterval_load_from_dict_success_datapoint(self):
        ''' creating a new SendDataInterval from a serialization should succeed '''
        uri={'uri':'valid.uri','type':Metrics.DATAPOINT.value}
        start=pd.Timestamp('now',tz='utc')
        end=pd.Timestamp('now',tz='utc')
        data =[
            [pd.Timestamp('now',tz='utc').isoformat(),'323'],
            [pd.Timestamp('now',tz='utc').isoformat(),'3223'],
            [pd.Timestamp('now',tz='utc').isoformat(),'25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'25623'],
            [pd.Timestamp('now',tz='utc').isoformat(),'253232323'],
        ]
        source = {
            'v':messages.KomlogMessage._version_,
            'action':Actions.SEND_DATA_INTERVAL.value,
            'payload':{
                'uri':uri,
                'start':start.isoformat(),
                'end':end.isoformat(),
                'data':data
            }
        }
        msg=messages.SendDataInterval.load_from_dict(source)
        self.assertEqual(msg.v, messages.KomlogMessage._version_)
        self.assertEqual(msg.action, Actions.SEND_DATA_INTERVAL)
        self.assertEqual(msg.metric, orm.Datapoint(uri=uri['uri']))
        self.assertEqual(msg.start, pd.Timestamp(start))
        self.assertEqual(msg.end, pd.Timestamp(end))
        self.assertEqual(sorted(msg.data, key=lambda x:x[0]), sorted([(pd.Timestamp(item[0]),decimal.Decimal(item[1])) for item in data]))


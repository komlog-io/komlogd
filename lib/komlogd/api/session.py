import asyncio
import aiohttp
import json
import traceback
import pandas as pd
from komlogd.api import logging
from komlogd.api import crypto
from komlogd.api.processing import message as procmsg
from komlogd.api.model import messages, exceptions, orm, store, transfer_methods, queues

KOMLOG_LOGIN_URL = 'https://www.komlog.io/login'
KOMLOG_WS_URL = 'https://agents.komlog.io/'


class KomlogSession:
    def __init__(self, username, privkey, loop=None):
        loop = loop or asyncio.get_event_loop()
        self._loop = loop
        self._username = username
        self._privkey = privkey
        self._pubkey = self._privkey.public_key()
        self._metrics_store=store.MetricsStore(owner=username)
        self._session = None
        self._ws = None
        self._session_future = None
        self._loop_future = None
        self._stop_f = False
        self._serialized_pubkey = crypto.serialize_public_key(self._pubkey)
        self._printable_pubkey = crypto.get_printable_pubkey(self._pubkey)
        self._deferred=[]
        self._hooked_metrics=set()
        self._q_msg_workers = queues.AsyncQueue(num_workers=5, on_msg=self._process_input_message, name='Message Workers', loop=self._loop)

    async def close(self):
        logging.logger.info('closing Komlog connection')
        self._stop_f = True
        await self._q_msg_workers.join()
        if self._ws and self._ws.closed is False:
            await self._ws.close()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None
        if self._loop_future:
            await self._loop_future
            self._session_future.set_result(True)

    async def login(self):
        if self._session_future is None:
            logging.logger.info('Authenticating agent')
            await self._auth()
            logging.logger.info('Initializing websocket connection')
            await self._ws_connect()
            self._q_msg_workers.start()
            logging.logger.info('Entering loop')
            self._loop_future = asyncio.ensure_future(self._session_loop(), loop=self._loop)
            self._session_future = asyncio.futures.Future(loop=self._loop)
        return self._session_future

    async def _auth(self):
        self._session = aiohttp.ClientSession()
        data = {
            'u':self._username,
            'k':self._serialized_pubkey,
            'pv':messages.KomlogMessage._version_
        }
        try:
            async with self._session.post(KOMLOG_LOGIN_URL,data=data) as resp:
                resp_content = await resp.json()
                if resp.status == 403:
                    logging.logger.error('Access Denied')
                    logging.logger.error('is username correct? '+self._username)
                    logging.logger.error('is agent public key added on web and in active state?')
                    logging.logger.error('public key content is:\n'+self._printable_pubkey)
                    raise exceptions.LoginException('Access denied')
                elif not (resp.status == 200 and 'challenge' in resp_content):
                    logging.logger.error('Unexpected server response: '+str(resp))
                    raise exceptions.LoginException('Unexpected error')
            c = crypto.process_challenge(self._privkey, resp_content['challenge'])
            s = crypto.sign_message(self._privkey, c)
            data['c']=c
            data['s']=s
            async with self._session.post(KOMLOG_LOGIN_URL,data=data) as resp:
                resp_content = await resp.json()
                if resp.status == 403:
                    logging.logger.error('Access Denied. is agent active?')
                    raise exceptions.LoginException('Authentication process failed')
        except:
            if self._session:
                await self._session.close()
            raise

    async def _ws_connect(self):
        try:
            self._ws = await self._session.ws_connect(KOMLOG_WS_URL)
        except:
            if self._ws:
                await self._ws.close()
            raise

    async def _session_loop(self):
        while not self._stop_f:
            try:
                if not self._session:
                    logging.logger.debug('Restarting Komlog session')
                    await self._auth()
                    await self._ws_connect()
                elif not self._ws:
                    logging.logger.debug('Restarting websocket connection')
                    await self._ws_connect()
                self._load_transfer_methods()
                self._ws_reconnected()
                async for msg in self._ws:
                    logging.logger.debug('Message received from server: '+str(msg))
                    if msg.tp == aiohttp.WSMsgType.CLOSED:
                        break
                    elif msg.tp == aiohttp.WSMsgType.ERROR:
                        break
                    else:
                        await self._q_msg_workers.push(msg)
                        self._metrics_store.run_maintenance()
                logging.logger.debug('Unexpected session close')
            except Exception:
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
            finally:
                if self._ws and self._ws.closed:
                    if self._ws.close_code == 4403:
                        logging.logger.debug('Server denied access. Retrying connection.')
                        self._session = None
                    self._ws = None
                if not self._stop_f:
                    await asyncio.sleep(15)

    def _load_transfer_methods(self):
        logging.logger.debug('Loading transfer methods')
        self._transfer_methods=transfer_methods.TransferMethodsIndex(owner=self._username)
        for item in transfer_methods.static_transfer_methods.get_transfer_methods():
            self._transfer_methods.set_transfer_method(item)
            for metric in item.metrics:
                self._hooked_metrics.add(metric)
            if item.data_reqs is not None:
                for metric in item.metrics:
                    self._metrics_store.set_metric_data_reqs(metric=metric, requirements=item.data_reqs)

    def _ws_reconnected(self):
        for metric in self._hooked_metrics:
            msg=messages.HookToUri(uri=metric.uri)
            self._send_message(msg)
            data_reqs=self._metrics_store.get_metric_data_reqs(metric)
            if data_reqs:
                if data_reqs.past_delta:
                    end = pd.Timestamp('now',tz='utc')
                    start= end - data_reqs.past_delta
                else:
                    end = None
                    start = None
                msg=messages.RequestData(uri=metric.uri, start=start, end=end, count=data_reqs.past_count)
                self._send_message(msg)
        for msg in self._deferred[:]:
            logging.logger.debug('sending deferred message')
            self._deferred.remove(msg)
            self._send_message(msg)

    async def _process_input_message(self, msg):
        try:
            data=json.loads(msg.data)
            if 'action' in data:
                message=messages.KomlogMessage.load_from_dict(data)
                procmsg.processing_map[message.action](msg=message, session=self)
        except Exception:
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)

    def _send_message(self, message):
        if not isinstance(message, messages.KomlogMessage):
            raise exceptions.InvalidMessageException()
        try:
            logging.logger.debug('sending message '+str(message.to_dict()))
            self._ws.send_str(json.dumps(message.to_dict()))
        except Exception:
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
            self._deferred.append(message)

    def hook(self, metrics, on_update=None):
        h_metrics=[]
        if isinstance(metrics, list):
            for metric in metrics:
                msg=messages.HookToUri(uri=metric.uri)
                self._send_message(msg)
                h_metrics.append(metric)
        else:
            msg=messages.HookToUri(uri=metrics.uri)
            self._send_message(msg)
            h_metrics.append(metrics)
        for metric in h_metrics:
            self._hooked_metrics.add(metric)
        if on_update and len(h_metrics)>0:
            self._transfer_methods.set_transfer_method(metrics=h_metrics, func=on_update)

    def unhook(self, metrics):
        uh_metrics=[]
        if isinstance(metrics, list):
            for metric in metrics:
                msg=messages.UnHookFromUri(uri=metric.uri)
                self._send_message(msg)
                uh_metrics.append(metric)
        else:
            msg=messages.UnHookFromUri(uri=metrics.uri)
            self._send_message(msg)
            uh_metrics.append(metrics)
        for metric in uh_metrics:
            try:
                self._hooked_metrics.remove(metric)
            except KeyError:
                pass

    def send_samples(self, samples):
        if not isinstance(samples, list):
            return False
        grouped = {}
        for sample in samples:
            if isinstance(sample, orm.Sample):
                try:
                    grouped[sample.ts].append(sample)
                except KeyError:
                    grouped[sample.ts]=[sample]
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
                    self._metrics_store.store(metric=item.metric, ts=ts, content=item.data)
                msgs.append(messages.SendMultiData(ts=ts, uris=uris))
            elif isinstance(items[0].metric, orm.Datasource):
                msgs.append(messages.SendDsData(uri=items[0].metric.uri, ts=ts, content=items[0].data))
                self._metrics_store.store(metric=items[0].metric, ts=ts, content=items[0].data)
            elif isinstance(items[0].metric, orm.Datapoint):
                msgs.append(messages.SendDpData(uri=items[0].metric.uri, ts=ts, content=items[0].data))
                self._metrics_store.store(metric=items[0].metric, ts=ts, content=items[0].data)
            else:
                raise TypeError('invalid metric type')
        msgs.sort(key=lambda x: x.ts)
        for msg in msgs:
            self._send_message(msg)
        return True



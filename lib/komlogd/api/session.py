import asyncio
import aiohttp
import json
import traceback
import pandas as pd
from komlogd.api import logging
from komlogd.api import crypto
from komlogd.api.processing import message as procmsg
from komlogd.api.model import messages, exceptions, orm, store, impulses

KOMLOG_LOGIN_URL = 'https://www.komlog.io/login'
KOMLOG_WS_URL = 'https://agents.komlog.io/'

loop = asyncio.get_event_loop()

class KomlogSession:
    def __init__(self, username, privkey):
        self.username = username
        self.privkey = privkey
        self.pubkey = self.privkey.public_key()
        self.metrics_store=store.MetricsStore(owner=username)
        self.session = None
        self.ws = None
        self.t_loop = None
        self.stop_f = False
        self._serialized_pubkey = crypto.serialize_public_key(self.pubkey)
        self._printable_pubkey = crypto.get_printable_pubkey(self.pubkey)
        self._deferred=[]
        self._hooked_metrics=set()

    async def close(self):
        logging.logger.info('closing Komlog connection')
        self.stop_f = True
        if self.ws and self.ws.closed is False:
            await self.ws.close()
            self.ws = None
        if self.session:
            await self.session.close()
            self.session = None
        if self.t_loop:
            await self.t_loop
            self.t_loop = None

    async def login(self):
        if self.t_loop:
            return False
        else:
            logging.logger.info('Authenticating agent')
            await self._auth()
            logging.logger.info('Initializing websocket connection')
            await self._ws_connect()
            logging.logger.info('Entering loop')
            self.t_loop = loop.create_task(self._loop())
            return True

    async def _auth(self):
        self.session = aiohttp.ClientSession()
        data = {
            'u':self.username,
            'k':self._serialized_pubkey,
            'pv':messages.KomlogMessage._version_
        }
        try:
            async with self.session.post(KOMLOG_LOGIN_URL,data=data) as resp:
                resp_content = await resp.json()
                if resp.status == 403:
                    logging.logger.error('Access Denied')
                    logging.logger.error('is username correct? '+self.username)
                    logging.logger.error('is agent public key added on web and in active state?')
                    logging.logger.error('public key content is:\n'+self._printable_pubkey)
                    raise exceptions.LoginException('Access denied')
                elif not (resp.status == 200 and 'challenge' in resp_content):
                    logging.logger.error('Unexpected server response: '+str(resp))
                    raise exceptions.LoginException('Unexpected error')
            c = crypto.process_challenge(self.privkey, resp_content['challenge'])
            s = crypto.sign_message(self.privkey, c)
            data['c']=c
            data['s']=s
            async with self.session.post(KOMLOG_LOGIN_URL,data=data) as resp:
                resp_content = await resp.json()
                if resp.status == 403:
                    logging.logger.error('Access Denied. is agent active?')
                    raise exceptions.LoginException('Authentication process failed')
        except:
            if self.session:
                await self.session.close()
            raise

    async def _ws_connect(self):
        try:
            self.ws = await self.session.ws_connect(KOMLOG_WS_URL)
        except:
            if self.ws:
                await self.ws.close()
            raise

    async def _loop(self):
        while not self.stop_f:
            try:
                if not self.session:
                    logging.logger.debug('Restarting Komlog session')
                    await self._auth()
                    await self._ws_connect()
                elif not self.ws:
                    logging.logger.debug('Restarting websocket connection')
                    await self._ws_connect()
                self._load_impulses()
                self._ws_reconnected()
                async for msg in self.ws:
                    logging.logger.debug('Message received from server: '+str(msg))
                    if msg.tp == aiohttp.WSMsgType.CLOSED:
                        break
                    elif msg.tp == aiohttp.WSMsgType.ERROR:
                        break
                    else:
                        self._process_input_message(msg)
                        self.metrics_store.run_maintenance()
                logging.logger.debug('Unexpected session close')
            except Exception:
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
            finally:
                if self.ws and self.ws.closed:
                    if self.ws.close_code == 4403:
                        logging.logger.debug('Server denied access. Retrying connection.')
                        self.session = None
                    self.ws = None
                if not self.stop_f:
                    await asyncio.sleep(15)

    def _load_impulses(self):
        logging.logger.debug('Loading impulse methods')
        self.impulses=impulses.Impulses(owner=self.username)
        for item in impulses.static_impulses.get_impulses():
            self.impulses.set_impulse(item)
            for metric in item.metrics:
                self._hooked_metrics.add(metric)
            if item.data_reqs is not None:
                for metric in item.metrics:
                    self.metrics_store.set_metric_data_reqs(metric=metric, requirements=item.data_reqs)

    def _ws_reconnected(self):
        for metric in self._hooked_metrics:
            msg=messages.HookToUri(uri=metric.uri)
            self._send_message(msg)
            data_reqs=self.metrics_store.get_metric_data_reqs(metric)
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

    def _process_input_message(self, msg):
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
            self.ws.send_str(json.dumps(message.to_dict()))
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
            self.impulses.set_impulse(metrics=h_metrics, func=on_update)

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
                    self.metrics_store.store(metric=item.metric, ts=ts, content=item.data)
                msgs.append(messages.SendMultiData(ts=ts, uris=uris))
            elif isinstance(items[0].metric, orm.Datasource):
                msgs.append(messages.SendDsData(uri=items[0].metric.uri, ts=ts, content=items[0].data))
                self.metrics_store.store(metric=items[0].metric, ts=ts, content=items[0].data)
            elif isinstance(items[0].metric, orm.Datapoint):
                msgs.append(messages.SendDpData(uri=items[0].metric.uri, ts=ts, content=items[0].data))
                self.metrics_store.store(metric=items[0].metric, ts=ts, content=items[0].data)
            else:
                raise TypeError('invalid metric type')
        msgs.sort(key=lambda x: x.ts)
        for msg in msgs:
            self._send_message(msg)
        return True



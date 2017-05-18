import asyncio
import aiohttp
import json
import traceback
import time
import pandas as pd
from komlogd.api import logging, exceptions, crypto
from komlogd.api.protocol.model import messages, validation
from komlogd.api.protocol.processing import message as prmsg
from komlogd.api.protocol.processing import procedure as prproc
from komlogd.api.model import store, transfer_methods, queues

KOMLOG_LOGIN_URL = 'https://www.komlog.io/login'
KOMLOG_WS_URL = 'https://agents.komlog.io/'


class KomlogSession:
    def __init__(self, username, privkey, loop=None, load_tm=True):
        loop = loop or asyncio.get_event_loop()
        self._loop = loop
        self.username = username
        self.privkey = privkey
        self._load_tm = load_tm
        self._metrics_store = store.MetricsStore(owner=self.username)
        self._transfer_methods = transfer_methods.TransferMethodsIndex(owner=self.username)
        self._session = None
        self._ws = None
        self._session_future = None
        self._loop_future = None
        self._deferred = []
        self._waiting_response = {}
        self._q_msg_workers = queues.AsyncQueue(num_workers=5, on_msg=self._process_received_message, name='Message Workers', loop=self._loop)

    @property
    def username(self):
        return self._username

    @username.setter
    def username(self, value):
        try:
            validation.validate_username(value)
            getattr(self, '_username')
            raise exceptions.BadParametersException('username modification not allowed')
        except AttributeError:
            self._username = value
        except TypeError:
            raise exceptions.BadParametersException('Invalid username {}'.format(str(value)))

    @property
    def privkey(self):
        return self._privkey

    @privkey.setter
    def privkey(self, value):
        try:
            validation.validate_privkey(value)
            getattr(self, '_privkey')
            raise exceptions.BadParametersException('private key modification not allowed')
        except AttributeError:
            self._privkey = value
            self._pubkey = value.public_key()
            self._serialized_pubkey = crypto.serialize_public_key(self._pubkey)
            self._printable_pubkey = crypto.get_printable_pubkey(self._pubkey)
        except TypeError:
            raise exceptions.BadParametersException('Invalid private key')

    async def close(self):
        logging.logger.info('closing Komlog connection')
        self._stop_f = True
        await self._q_msg_workers.join()
        if self._ws and self._ws.closed is False:
            await self._ws.close()
        if self._session:
            await self._session.close()
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
            if self._load_tm:
                self._load_anonymous_transfer_methods()
            logging.logger.info('Entering loop')
            self._loop_future = asyncio.ensure_future(self._session_loop(), loop=self._loop)
            self._session_future = asyncio.futures.Future(loop=self._loop)

    async def join(self):
        if self._session_future is not None:
            while not getattr(self, '_stop_f',False):
                await self._session_future

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
        while not getattr(self, '_stop_f',False):
            try:
                if not self._session:
                    logging.logger.debug('Restarting Komlog session')
                    await self._auth()
                    await self._ws_connect()
                elif not self._ws:
                    logging.logger.debug('Restarting websocket connection')
                    await self._ws_connect()
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
            except Exception:
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
            finally:
                logging.logger.debug('Unexpected session close')
                if self._ws and self._ws.closed:
                    if self._ws.close_code == 4403:
                        logging.logger.debug('Server denied access. Retrying connection.')
                        self._session = None
                    self._ws = None
                if not getattr(self, '_stop_f',False):
                    self._ws_disconnected()
                    await asyncio.sleep(15)

    def _load_anonymous_transfer_methods(self):
        logging.logger.debug('Loading transfer methods')
        for item in transfer_methods.anon_transfer_methods.get_transfer_methods(enabled=False):
            if not self._transfer_methods.add_transfer_method(item, enabled=False):
                logging.logger.error('Error loading transfer method '+item.f.__name__)

    def _ws_disconnected(self):
        self._transfer_methods.disable_all()

    def _ws_reconnected(self):
        for msg in self._deferred[:]:
            logging.logger.debug('sending deferred message')
            self._deferred.remove(msg)
            self._send_message(msg)
        asyncio.ensure_future(prproc.initialize_transfer_methods(self))

    async def _periodic_transfer_method_call(self, mid):
        t = time.time()
        await asyncio.sleep(60-t%60)
        localtime = time.localtime(t+(60-t%60))
        tm_info = self._transfer_methods.get_transfer_method_info(mid)
        if (tm_info and
            tm_info['enabled']):
            if tm_info['tm'].schedule.meets(t=localtime):
                ts=pd.Timestamp(ts_input=t+(60-t%60), unit='s', tz='utc')
                await prproc.exec_transfer_method(session=self, mid=tm_info['tm'].mid, ts=ts, metrics=[])
            asyncio.ensure_future(self._periodic_transfer_method_call(mid))

    async def _process_received_message(self, msg):
        try:
            data=json.loads(msg.data)
            if 'action' in data:
                message=messages.KomlogMessage.load_from_dict(data)
                if message.irt and message.irt in self._waiting_response:
                    if self._waiting_response[message.irt].done():
                        # some messages generate multiple responses. Protocol procedures are responsible for
                        # marking msg done or undone, adding more pending futures in the last case.
                        logging.logger.debug('Enqueueing message again')
                        await self._q_msg_workers.push(msg)
                    else:
                        logging.logger.debug('processing message response procedure')
                        self._waiting_response[message.irt].set_result(message)
                else:
                    logging.logger.debug('processing non requested message')
                    prmsg.processing_map[message.action](msg=message, session=self)
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

    async def _await_response(self, message):
        self._send_message(message)
        future = self._mark_message_undone(message.seq)
        return await future

    def _mark_message_done(self, seq):
        self._waiting_response.pop(seq,None)

    def _mark_message_undone(self, seq):
        future = asyncio.futures.Future(loop=self._loop)
        self._waiting_response[seq]=future
        return future

    async def send_samples(self, samples):
        logging.logger.debug('sending samples')
        result = await prproc.send_samples(self, samples)
        logging.logger.debug('result: {}'.format(str(result)))
        return result


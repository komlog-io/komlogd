import asyncio
import aiohttp
import json
import traceback
import time
import uuid
import pandas as pd
from komlogd.api.common import logging, exceptions, crypto
from komlogd.api.protocol import messages, validation
from komlogd.api.protocol.processing import message as prmsg
from komlogd.api.model import store, queues
from komlogd.api.model.session import sessionIndex



class KomlogSession:

    def __init__(self, username, privkey):
        self.sid = uuid.uuid4()
        self.username = username
        self.privkey = privkey
        self.store = store.MetricStore()
        self._loop = asyncio.get_event_loop()
        self._session = None
        self._ws = None
        self._session_future = None
        self._loop_future = None
        self._deferred = []
        self._waiting_response = {}
        self._q_msg_workers = queues.AsyncQueue(num_workers=5, on_msg=self._process_received_message, name='Message Workers', loop=self._loop)
        sessionIndex.register_session(self)

    def __del__(self):
        sessionIndex.unregister_session(self.sid)

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
        sessionIndex.unregister_session(self.sid)

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
            login_url = 'https://www.komlog.io/login'
            async with self._session.post(login_url, data=data) as resp:
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
            async with self._session.post(login_url, data=data) as resp:
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
            ws_url = 'https://agents.komlog.io/'
            self._ws = await self._session.ws_connect(ws_url)
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
                await self._ws_reconnected()
                async for msg in self._ws:
                    logging.logger.debug('Message received from server: '+str(msg))
                    if msg.tp == aiohttp.WSMsgType.CLOSED:
                        break
                    elif msg.tp == aiohttp.WSMsgType.ERROR:
                        break
                    else:
                        await self._q_msg_workers.push(msg)
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

    def _ws_disconnected(self):
        self.store.clear_synced()

    async def _ws_reconnected(self):
        await self.store.sync()
        for msg in self._deferred[:]:
            logging.logger.debug('sending deferred message')
            self._deferred.remove(msg)
            await self.send_message(msg)

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

    def _mark_message_done(self, seq):
        self._waiting_response.pop(seq,None)

    def _mark_message_undone(self, seq):
        future = asyncio.futures.Future(loop=self._loop)
        self._waiting_response[seq]=future
        return future

    async def send_message(self, message, defer=True, timeout=None, defer_timeout=None):
        if not isinstance(message, messages.KomlogMessage):
            raise exceptions.InvalidMessageException()
        try:
            logging.logger.debug('sending message '+str(message.to_dict()))
            self._ws.send_str(json.dumps(message.to_dict()))
        except Exception:
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
            if defer:
                self._deferred.append(message)
                fut = self._mark_message_undone(message.seq)
                try:
                    result = await asyncio.wait_for(fut, defer_timeout)
                    return result
                except asyncio.TimeoutError:
                    return None
            else:
                return None
        else:
            fut = self._mark_message_undone(message.seq)
            try:
                result = await asyncio.wait_for(fut, timeout)
                return result
            except asyncio.TimeoutError:
                return None


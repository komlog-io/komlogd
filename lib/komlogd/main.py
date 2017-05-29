'''

Main application

'''

import asyncio
import os
import sys
import signal
import functools
import traceback
from stat import S_ISFIFO, S_ISREG
from komlogd.base import config, logging
from komlogd.transfer_methods import main as tfmain
from komlogd.web import main as webmain

app = None

async def _signal_handler(signame):
    logging.logger.info(signame+' detected.')
    if app:
        await app.stop()

class Application:

    def __init__(self, config_file, uri):
        self.config_file = config_file
        self.uri = uri
        self._input_detected = True if S_ISFIFO(os.fstat(0).st_mode) or S_ISREG(os.fstat(0).st_mode) else False
        self._stdin_mode = True if uri else False
        try:
            config.load_application_config(filename=self.config_file)
            logging.initialize_logger()
            if self._input_detected != self._stdin_mode:
                if self._stdin_mode:
                    raise RuntimeError('uri parameter found, but no input detected')
                else:
                    raise RuntimeError('stdin data detected, but no uri parameter found')
            if self._stdin_mode:
                webmain.initialize_komlog_session(load_tm=False)
            else:
                tfmain.load_transfer_methods_files()
                webmain.initialize_komlog_session()
        except Exception as e:
            sys.stderr.write('Error initializing komlogd.\n')
            if logging.logger is not None and len(logging.logger.handlers)>0:
                sys.stderr.write('See log file: '+logging.logger.handlers[0].baseFilename+'\n')
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)
            else:
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    print (line)
            exit()

    async def start(self):
        logging.logger.debug('Starting komlogd.')
        if self._stdin_mode:
            await webmain.start_komlogd_stdin_mode(uri=self.uri)
        else:
            await webmain.start_komlog_session()

    async def stop(self):
        logging.logger.debug('Stopping komlogd.')
        await webmain.stop_komlog_session()

if sys.platform == 'win32':
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)
else:
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), lambda: asyncio.ensure_future(_signal_handler(signame)))

def start_application(config_file=None, uri=None):
    global app
    app = Application(config_file=config_file, uri=uri)
    try:
        loop.run_until_complete(app.start())
    except Exception as e:
        sys.stderr.write('Error detected.\n')
        if logging.logger:
            sys.stderr.write('See log file: '+logging.logger.handlers[0].baseFilename+'\n')
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
    finally:
        logging.logger.debug('Closing loop.')
        loop.close()


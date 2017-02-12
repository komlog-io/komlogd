'''

Main application

'''

import asyncio
import sys
import signal
import functools
import traceback
from komlogd.base import config, logging
from komlogd.scheduler import main as schmain
from komlogd.impulses import main as impmain
from komlogd.web import main as webmain

app = None

async def _signal_handler(signame):
    logging.logger.info(signame+' detected.')
    if app:
        await app.stop()

class Application():
    def __init__(self, config_file):
        try:
            config.load_application_config(filename=config_file)
            logging.initialize_logger()
            schmain.initialize_scheduler()
            impmain.load_impulse_files()
            webmain.initialize_komlog_session()
        except Exception as e:
            print ('Error initializing komlogd.')
            if logging.logger is not None and len(logging.logger.handlers)>0:
                print ('See log file: '+logging.logger.handlers[0].baseFilename)
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
        schmain.start_scheduler()
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

def start_application(config_file=None):
    global app
    app = Application(config_file=config_file)
    try:
        loop.run_until_complete(app.start())
    except Exception as e:
        print ('Exception running komlogd.')
        if logging.logger:
            print ('See log file: '+logging.logger.handlers[0].baseFilename)
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
    finally:
        logging.logger.debug('Closing loop.')
        loop.close()


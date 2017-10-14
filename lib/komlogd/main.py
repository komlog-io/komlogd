'''

Main application

'''

import os
import sys
import signal
import asyncio
import argparse
import functools
import subprocess
import traceback
from stat import S_ISFIFO, S_ISREG
from komlogd.api.model import transactions
from komlogd.base import config, logging, packages, session

if not sys.platform == 'win32':
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), lambda: asyncio.ensure_future(_signal_handler(signame)))

async def _signal_handler(signame):
    logging.logger.info(signame+' detected.')
    if app:
        await app.stop()

class Application:

    def __init__(self, config_file, uri, venv):
        self.config_file = config_file
        self.uri = uri
        self.venv = venv
        self.venvs = []
        self.process_name = venv if venv else 'main'
        self._input_detected = True if S_ISFIFO(os.fstat(0).st_mode) or S_ISREG(os.fstat(0).st_mode) else False
        self._stdin_mode = True if uri else False
        try:
            config.initialize_config(filename=self.config_file)
            logging.initialize_logging(self.process_name)
            if self._input_detected != self._stdin_mode:
                if self._stdin_mode:
                    raise RuntimeError('uri parameter found, but no input detected')
                else:
                    raise RuntimeError('stdin data detected, but no uri parameter found')
            self.session = session.initialize_komlog_session()
        except Exception as e:
            sys.stderr.write('Error initializing komlogd.\n')
            sys.stderr.write(str(e)+'\n')
            if logging.logger is not None and len(logging.logger.handlers)>0:
                sys.stderr.write('Log info: '+logging.logger.handlers[0].baseFilename+'\n')
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
            await session.send_stdin(self.session, uri=self.uri)
        else:
            await self.session.login()
            if self.venv == None:
                venvs = packages.create_venvs()
                for env in venvs:
                    env['config'] = self.config_file
                    p = packages.boot_venv(env)
                    env['proc'] = p
                    self.venvs.append(env)
            else:
                if not (packages.load_venv_packages(self.venv) and await packages.load_entry_points()):
                    logging.logger.error('Packages load failed. Exiting virtualenv '+self.venv)
                    return await self.stop()
            logging.logger.debug('Initialization done, joining session')
            await self.session.join()

    async def stop(self):
        logging.logger.debug('Stopping komlogd.')
        for env in self.venvs:
            env['proc'].send_signal(signal.SIGTERM)
        for env in self.venvs:
            try:
                env['proc'].wait(10)
            except subprocess.TimeoutExpired:
                logging.logger.error('Timeout expired waiting for virtualenv process exit: '+env['name'])
                logging.logger.error('Terminating virtualenv process: '+env['name'])
                env['proc'].kill()
        await self.session.close()

app = None

def menu():
    parser = argparse.ArgumentParser(description='Komlog agent')
    parser.add_argument('-c','--config', required=False, help='Indicates the configuration file to use. Must be the absolute file path', default=config.get_default_config_file())
    parser.add_argument('-u','--uri', required=False, help='Uri to upload data to', default=None)
    parser.add_argument('-v', '--venv', required=False, help='boot virtualenv')
    args = parser.parse_args()
    return args

def main():
    global app
    args = menu()
    app = Application(config_file=args.config, uri=args.uri, venv=args.venv)
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


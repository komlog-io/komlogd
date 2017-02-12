import traceback
from komlogd.api import session
from komlogd.base import crypto, config, logging, exceptions
from komlogd.base.settings import options

KomlogSession = None

def initialize_komlog_session():
    global KomlogSession
    username = config.config.get_entries(entryname=options.KOMLOG_USERNAME)
    privkey = crypto.get_private_key()
    if len(username)==0:
        raise exceptions.BadParametersException('Set username in configuration file.')
    elif len(username)>1:
        raise exceptions.BadParametersException('More than one username found in configuration file. Keep only one.')
    KomlogSession = session.KomlogSession(username=username[0], privkey=privkey)

async def start_komlog_session():
    await KomlogSession.login()
    await KomlogSession.t_loop

async def stop_komlog_session():
    await KomlogSession.close()

def send_sample(sample):
    try:
        KomlogSession.send_samples(samples=[sample])
    except Exception:
        logging.logger.error('Exception sending job output')
        ex_info=traceback.format_exc().splitlines()
        for line in ex_info:
            logging.logger.error(line)


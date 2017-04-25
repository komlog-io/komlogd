import asyncio
import dateutil.tz
import sys
import pandas as pd
from komlogd.api import session
from komlogd.api.protocol.model.types import Datasource, Sample
from komlogd.base import crypto, config, logging, exceptions
from komlogd.base.settings import options

KomlogSession = None

def initialize_komlog_session(load_tm=True):
    global KomlogSession
    username = config.config.get_entries(entryname=options.KOMLOG_USERNAME)
    privkey = crypto.get_private_key()
    if len(username)==0:
        raise exceptions.BadParametersException('Set username in configuration file.')
    elif len(username)>1:
        raise exceptions.BadParametersException('More than one username found in configuration file. Keep only one.')
    KomlogSession = session.KomlogSession(username=username[0], privkey=privkey, load_tm=load_tm)

async def start_komlog_session():
    await KomlogSession.login()
    await KomlogSession.join()

async def start_komlogd_stdin_mode(uri):
    now = pd.Timestamp('now', tz=dateutil.tz.tzlocal())
    data = sys.stdin.read()
    sample = Sample(metric=Datasource(uri), ts=now, data=data)
    await KomlogSession.login()
    result = await KomlogSession.send_samples([sample])
    if not result['success']:
        sys.stderr.write('Error sending data to Komlog.\n')
        sys.stderr.write(result['error']+'\n')
    await KomlogSession.close()

async def stop_komlog_session():
    await KomlogSession.close()


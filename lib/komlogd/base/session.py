import asyncio
import sys
from komlogd.api import session
from komlogd.api.common.timeuuid import TimeUUID
from komlogd.api.model.metrics import Datasource, Sample
from komlogd.api.protocol.processing import procedure as prproc
from komlogd.base import crypto, config, logging, exceptions
from komlogd.base.settings import options


def initialize_komlog_session():
    username = config.config.get_entries(entryname=options.KOMLOG_USERNAME)
    privkey = crypto.get_private_key()
    if len(username)==0:
        raise exceptions.BadParametersException('Set username in configuration file.')
    elif len(username)>1:
        raise exceptions.BadParametersException('More than one username found in configuration file. Keep only one.')
    return session.KomlogSession(username=username[0], privkey=privkey)

async def send_stdin(s, uri):
    data = sys.stdin.read()
    sample = Sample(metric=Datasource(uri, session=s), t=TimeUUID(), value=data)
    await s.login()
    result = await prproc.send_samples([sample])
    if not result['success']:
        sys.stderr.write('Error sending data to Komlog.\n')
        for err in result['errors']:
            sys.stderr.write(str(err['error'])+'\n')
    await s.close()


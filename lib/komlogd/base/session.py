import asyncio
import sys
from komlogd.api import session
from komlogd.api.common.timeuuid import TimeUUID
from komlogd.api.model.metrics import Datasource, Sample
from komlogd.api.protocol.processing import procedure as prproc
from komlogd.base import crypto, config


def initialize_komlog_session():
    privkey = crypto.get_private_key()
    username = config.config.username
    return session.KomlogSession(username=username, privkey=privkey)

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


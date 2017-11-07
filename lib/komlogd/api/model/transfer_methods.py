'''

Transfer Methods

'''

import asyncio
import time
import pandas as pd
from komlogd.api.common import logging, exceptions, timeuuid
from komlogd.api.model import schedules

class TransferMethodsIndex:

    def __init__(self):
        self._enabled_methods={}
        self._disabled_methods = {}

    def add_tm(self, tm):
        if self.get_tm_info(tm.mid):
            return False
        self._disabled_methods[tm.mid]={'tm':tm, 'first':None}
        return True

    async def enable_tm(self, mid):
        tm_info = self._disabled_methods.pop(mid, None)
        if tm_info:
            logging.logger.debug('enabling tm '+mid.hex)
            try:
                logging.logger.debug('tm activation metrics: '+str([m.uri for m in tm_info['tm'].schedule.activation_metrics]))
                for metric in tm_info['tm'].schedule.activation_metrics:
                    result = await metric.session.store.hook(metric=metric)
                    if result['hooked'] == False:
                        logging.logger.error('Error syncing metric {}. Aborting tm initialization'.format(metric.uri))
                        return False
            except (exceptions.SessionException, exceptions.SessionNotFoundException) as e:
                logging.logger.error('Error syncing metric {}. Aborting tm initialization'.format(metric.uri))
                logging.logger.error('Error: {}.'.format(e.msg))
                self._disabled_methods[mid]=tm_info
                if tm_info['first'] == None:
                    asyncio.ensure_future(self._retry_failed())
                return False
            else:
                if tm_info['first'] == None:
                    now = pd.Timestamp('now', tz='utc')
                    tm_info['first'] = now
                    if tm_info['tm'].schedule.exec_on_load:
                        t = timeuuid.TimeUUID()
                        asyncio.ensure_future(tm_info['tm'].run(t=t, metrics=[]))
            self._enabled_methods[mid] = tm_info
            if isinstance(tm_info['tm'].schedule, schedules.CronSchedule):
                asyncio.ensure_future(self._periodic_transfer_method_call(mid))
            return True
        elif mid in self._enabled_methods:
            logging.logger.debug('tm already enabled '+mid.hex)
            return False
        else:
            logging.logger.debug('tm not found '+mid.hex)
            return False

    def disable_tm(self, mid):
        tm_info = self._enabled_methods.pop(mid, None)
        if tm_info:
            logging.logger.debug('disabling tm '+mid.hex)
            self._disabled_methods[mid] = tm_info
            return True
        elif mid in self._disabled_methods:
            logging.logger.debug('tm already disabled '+mid.hex)
            return False
        else:
            logging.logger.debug('tm not found '+mid.hex)
            return False

    def delete_tm(self, mid):
        self._enabled_methods.pop(mid,None)
        self._disabled_methods.pop(mid,None)
        return True

    def disable_all(self):
        mids = list(self._enabled_methods.keys())
        for mid in mids:
            self.disable_tm(mid)
        return True

    async def enable_all(self):
        mids = list(self._disabled_methods.keys())
        for mid in mids:
            await self.enable_tm(mid)
        return True

    def get_tm_info(self, mid):
        if mid in self._enabled_methods:
            tm_info = self._enabled_methods[mid]
            return {'enabled':True, 'tm':tm_info['tm']}
        elif mid in self._disabled_methods:
            tm_info = self._disabled_methods[mid]
            return {'enabled':False, 'tm':tm_info['tm']}
        else:
            return None

    def metrics_updated(self, t, metrics, irt):
        tms = self._get_tms_activated_with(metrics)
        for tm in tms:
            logging.logger.debug('Requesting execution of tm: '+ tm.mid.hex)
            asyncio.ensure_future(tm.run(t=t, metrics=metrics, irt=irt))

    def _get_tms_activated_with(self, metrics, enabled=True):
        if enabled:
            all_tms = [tm_info['tm'] for tm_info in self._enabled_methods.values()]
        else:
            all_tms = [tm_info['tm'] for tm_info in self._disabled_methods.values()]
        tms =  []
        for tm in all_tms:
            for m in metrics:
                if m in tm.schedule.activation_metrics:
                    tms.append(tm)
                    break
        return tms

    def _get_tms_that_meet(self, t, enabled=True):
        tms=[]
        if enabled:
            for tm_info in self._enabled_methods.values():
                if tm_info['tm'].schedule.meets(t):
                    tms.append(tm_info['tm'])
        else:
            for tm_info in self._disabled_methods.values():
                if tm_info['tm'].schedule.meets(t):
                    tms.append(tm_info['tm'])
        return tms

    async def _periodic_transfer_method_call(self, mid):
        t = timeuuid.TimeUUID()
        await asyncio.sleep(60-t.timestamp%60)
        t=timeuuid.TimeUUID(t=t.timestamp+(60-t.timestamp%60))
        localtime = time.localtime(t.timestamp)
        tm_info = self.get_tm_info(mid)
        if (tm_info and tm_info['enabled']):
            if tm_info['tm'].schedule.meets(t=localtime):
                logging.logger.debug('periodic_transfer_method_call '+mid.hex)
                await tm_info['tm'].run(t=t, metrics=[])
            asyncio.ensure_future(self._periodic_transfer_method_call(mid))

    async def _retry_failed(self, sleep=5):
        if getattr(self, '_retry_task', None) != None and self._retry_task.done() == False:
            return False
        self._retry_task = asyncio.Task.current_task()
        await asyncio.sleep(sleep)
        mids = list(self._disabled_methods.keys())
        for mid in mids:
            tm_info = self._disabled_methods.get(mid,{})
            if 'first' in tm_info and tm_info['first'] == None:
                if not await self.enable_tm(mid):
                    # enable_tm should have generated another retry_task
                    return False
        return True


tmIndex = TransferMethodsIndex()

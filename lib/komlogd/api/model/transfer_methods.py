'''

Transfer Methods

'''

import asyncio
import uuid
from komlogd.api import logging, uri
from komlogd.api.protocol.model import validation
from komlogd.api.protocol.model.schedules import OnUpdateSchedule, CronSchedule

class TransferMethodsIndex:

    def __init__(self, owner):
        self.owner = owner
        self._enabled_methods={}
        self._disabled_methods = {}
        self.uri_transfer_methods={}
        self.metrics={}
        self.on_update_uri_transfer_methods={}
        self.on_update_metrics={}

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        if value is None:
            self._owner = None
        else:
            validation.validate_username(value)
            self._owner = value.lower()

    def add_transfer_method(self, transfer_method, enabled=False):
        if self.get_transfer_method_info(transfer_method.mid):
            return False
        if self.owner:
            transfer_method._initialize(owner = self.owner)
            for m in transfer_method.m_in:
                guri = uri.get_global_uri(m, owner=self.owner)
                if guri not in self.metrics:
                    self.metrics[guri]=m
                try:
                    self.uri_transfer_methods[guri].add(transfer_method.mid)
                except KeyError:
                    self.uri_transfer_methods[guri]={transfer_method.mid}
            if isinstance(transfer_method.schedule, OnUpdateSchedule):
                for m in transfer_method.schedule.metrics:
                    guri = uri.get_global_uri(m, owner=self.owner)
                    if guri not in self.on_update_metrics:
                        self.on_update_metrics[guri]=m
                    try:
                        self.on_update_uri_transfer_methods[guri].add(transfer_method.mid)
                    except KeyError:
                        self.on_update_uri_transfer_methods[guri]={transfer_method.mid}
        if enabled:
            self._enabled_methods[transfer_method.mid]=transfer_method
        else:
            self._disabled_methods[transfer_method.mid]=transfer_method
        return True

    def enable_transfer_method(self, mid):
        if mid in self._disabled_methods:
            logging.logger.debug('enabling disabled method '+str(mid))
            self._enabled_methods[mid]=self._disabled_methods.pop(mid)
            return True
        elif mid in self._enabled_methods:
            logging.logger.debug('enabling already enabled method '+str(mid))
            return True
        else:
            return False

    def disable_transfer_method(self, mid):
        if mid in self._enabled_methods:
            logging.logger.debug('disabling enabled method '+str(mid))
            self._disabled_methods[mid]=self._enabled_methods.pop(mid)
            return True
        elif mid in self._disabled_methods:
            logging.logger.debug('disabling already disabled method '+str(mid))
            return True
        else:
            return False

    def delete_transfer_method(self, mid):
        self._enabled_methods.pop(mid,None)
        self._disabled_methods.pop(mid,None)
        for guri, mids in self.uri_transfer_methods.items():
            mids.pop(mid,None)
        return True

    def disable_all(self):
        for mid in list(self._enabled_methods.keys()):
            logging.logger.debug('Disabling transfer method '+str(mid))
            self._disabled_methods[mid]=self._enabled_methods.pop(mid)
        return True

    def get_transfer_method_info(self, mid):
        if mid in self._enabled_methods:
            return {'enabled':True, 'tm':self._enabled_methods[mid]}
        elif mid in self._disabled_methods:
            return {'enabled':False, 'tm':self._disabled_methods[mid]}
        else:
            return None

    def get_transfer_methods(self, metrics=None, enabled=True):
        mids=[]
        if metrics is None:
            if enabled:
                transfer_methods = list(self._enabled_methods.values())
            else:
                transfer_methods = list(self._disabled_methods.values())
        else:
            for m in metrics:
                if self.owner:
                    guri = uri.get_global_uri(m, owner=self.owner)
                else:
                    guri = m.uri
                if guri in self.metrics and self.metrics[guri].m_type is None:
                    self.metrics[guri]=m
                if guri in self.uri_transfer_methods:
                    for mid in self.uri_transfer_methods[guri]:
                        mids.append(mid)
            mids=list(set(mids))
            if enabled:
                transfer_methods=[self._enabled_methods[mid] for mid in mids if mid in self._enabled_methods]
            else:
                transfer_methods=[self._disabled_methods[mid] for mid in mids if mid in self._disabled_methods]
        return transfer_methods

    def get_on_update_transfer_methods(self, metrics):
        mids=[]
        for m in metrics:
            if self.owner:
                guri = uri.get_global_uri(m, owner=self.owner)
            else:
                guri = m.uri
            if guri in self.on_update_metrics and self.on_update_metrics[guri].m_type is None:
                self.on_update_metrics[guri]=m
            if guri in self.on_update_uri_transfer_methods:
                for mid in self.on_update_uri_transfer_methods[guri]:
                    mids.append(mid)
        mids=list(set(mids))
        transfer_methods=[self._enabled_methods[mid] for mid in mids if mid in self._enabled_methods]
        return transfer_methods

    def get_cron_transfer_methods(self, t):
        transfer_methods=[]
        for tm in self._enabled_methods.values():
            if isinstance(tm.schedule, CronSchedule) and tm.schedule.meets(t):
                transfer_methods.append(tm)
        return transfer_methods


anon_transfer_methods = TransferMethodsIndex(owner=None)

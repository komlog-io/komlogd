'''

Transfer Methods

'''

import asyncio
import uuid
from komlogd.api import logging, uri
from komlogd.api.protocol.model import validation

class TransferMethodsIndex:

    def __init__(self, owner=None):
        self.owner = owner
        self.uri_transfer_methods={}
        self._enabled_methods={}
        self._disabled_methods = {}
        self.metrics={}

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
        if self.get_transfer_method(transfer_method.mid):
            return False
        for m in transfer_method.m_in:
            if self.owner:
                guri = uri.get_global_uri(m, owner=self.owner)
            else:
                guri = m.uri
            if guri not in self.metrics:
                self.metrics[guri]=m
            try:
                self.uri_transfer_methods[guri].add(transfer_method.mid)
            except KeyError:
                self.uri_transfer_methods[guri]={transfer_method.mid}
        if enabled:
            self._enabled_methods[transfer_method.mid]=transfer_method
        else:
            self._disabled_methods[transfer_method.mid]=transfer_method
        return True

    def enable_transfer_method(self, mid):
        if mid in self._disabled_methods:
            self._enabled_methods[mid]=self._disabled_methods.pop(mid)
            return True
        elif mid in self._enabled_methods:
            return True
        else:
            return False

    def disable_transfer_method(self, mid):
        if mid in self._enabled_methods:
            self._disabled_methods[mid]=self._enabled_methods.pop(mid)
            return True
        elif mid in self._disabled_methods:
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
            self._disabled_methods[mid]=self._enabled_methods.pop(mid)
            return True

    def get_transfer_method(self, mid):
        if mid in self._enabled_methods:
            return self._enabled_methods[mid]
        elif mid in self._disabled_methods:
            return self._disabled_methods[mid]
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

static_transfer_methods = TransferMethodsIndex()

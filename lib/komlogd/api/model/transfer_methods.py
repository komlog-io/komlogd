'''

Transfer Methods

'''

import asyncio
import uuid
from komlogd.api import logging
from komlogd.api.model import validation, orm

class TransferMethodsIndex:

    def __init__(self, owner=None):
        self.owner = owner
        self.uri_transfer_methods={}
        self.transfer_methods={}
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

    def set_transfer_method(self, transfer_method):
        for metric in transfer_method.metrics:
            if self.owner:
                uri = orm.get_global_uri(metric, owner=self.owner)
            else:
                uri = metric.uri
            if uri not in self.metrics:
                self.metrics[uri]=metric
            try:
                self.uri_transfer_methods[uri].append(transfer_method.lid)
            except KeyError:
                self.uri_transfer_methods[uri]=[transfer_method.lid]
        self.transfer_methods[transfer_method.lid]=transfer_method
        return True

    def get_transfer_methods(self, metrics=None):
        lids=[]
        transfer_methods=[]
        if metrics is None:
            for lid,method in self.transfer_methods.items():
                transfer_methods.append(method)
        else:
            for metric in metrics:
                if self.owner:
                    uri = orm.get_global_uri(metric, owner=self.owner)
                else:
                    uri = metric.uri
                if uri in self.metrics and self.metrics[uri].m_type is None:
                    self.metrics[uri]=metric
                if uri in self.uri_transfer_methods:
                    for lid in self.uri_transfer_methods[uri]:
                        lids.append(lid)
            lids=list(set(lids))
            for lid in lids:
                try:
                    transfer_methods.append(self.transfer_methods[lid])
                except KeyError:
                    logging.logger.error('Error retrieving transfer method info '+lid.hex)
        return transfer_methods


static_transfer_methods=TransferMethodsIndex()

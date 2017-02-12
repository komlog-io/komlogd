'''

Impulses

'''

import asyncio
import uuid
from komlogd.api import logging
from komlogd.api.model import validation, orm

class Impulses:

    def __init__(self, owner=None):
        self.owner = owner
        self.impulses={}
        self.impulse_methods={}
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

    def set_impulse(self, impulse_method):
        for metric in impulse_method.metrics:
            if self.owner:
                uri = orm.get_global_uri(metric, owner=self.owner)
            else:
                uri = metric.uri
            if uri not in self.metrics:
                self.metrics[uri]=metric
            try:
                self.impulses[uri].append(impulse_method.lid)
            except KeyError:
                self.impulses[uri]=[impulse_method.lid]
        self.impulse_methods[impulse_method.lid]=impulse_method
        return True

    def get_impulses(self, metrics=None):
        lids=[]
        impulses=[]
        if metrics is None:
            for lid,method in self.impulse_methods.items():
                impulses.append(method)
        else:
            for metric in metrics:
                if self.owner:
                    uri = orm.get_global_uri(metric, owner=self.owner)
                else:
                    uri = metric.uri
                if uri in self.metrics and self.metrics[uri].m_type is None:
                    self.metrics[uri]=metric
                if uri in self.impulses:
                    for lid in self.impulses[uri]:
                        lids.append(lid)
            lids=list(set(lids))
            for lid in lids:
                try:
                    impulses.append(self.impulse_methods[lid])
                except KeyError:
                    logging.logger.error('Error retrieving impulse info '+lid.hex)
        return impulses


static_impulses=Impulses()

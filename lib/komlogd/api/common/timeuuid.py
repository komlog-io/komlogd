import os
import sys
import time
import uuid
import datetime

class TimeUUID(uuid.UUID):

    def __init__(self, t=None, s=None, random=True, highest=False, lowest=False):
        if s != None:
            u = uuid.UUID(s)
            if u.version == 1:
                super().__init__(
                    fields=u.fields,
                    version=1)
            else:
                raise ValueError('Invalid UUID type')
        else:
            ts = t if t != None else time.time()
            us = int(ts * 1e6) # we could store 10x more precision, but tricky to get timestamp back
            ep = int(us * 10) + 0x01b21dd213814000
            time_low = ep & 0xffffffff
            time_mid = (ep >> 32) & 0xffff
            time_hi_version = (ep >> 48) & 0x0fff
            if highest:
                cs = 0x3f7f
                node = 0x7f7f7f7f7f7f
            elif lowest:
                cs = 0x80
                node = 0x808080808080
            elif random:
                r = int.from_bytes(os.urandom(8), sys.byteorder) #r is 64bits
                cs = r & 0x3fff #cs is 14bits
                node = (r >> 14) & 0xffffffffffff #node is 48bits
            else:
                cs = 0
                node = 0
            clock_seq_low = cs & 0xff
            clock_seq_hi_variant = (cs >> 8) & 0x3f
            super().__init__(
                fields=(time_low, time_mid, time_hi_version, clock_seq_hi_variant, clock_seq_low, node),
                version=1)

    def __cmp__(self, other):
        if self.time > other.time:
            return 1
        elif self.time < other.time:
            return -1
        else:
            x_b = self.bytes[8:]
            y_b = other.bytes[8:]
            for i in range(1,9):
                x_v = x_b[i-1]
                y_v = y_b[i-1]
                if i == 1:
                    x_v = x_v & 0x3f
                    y_v = y_v & 0x3f
                if x_v > 127:
                    x_v = x_v - 256
                if y_v > 127:
                    y_v = y_v - 256
                if x_v > y_v:
                    return 1
                elif x_v < y_v:
                    return -1
            return 0

    def __lt__(self, other):
        if not isinstance(other, uuid.UUID):
            return super().__lt__(other)
        if self.version == 1 and other.version == 1:
            return self.__cmp__(other) == -1
        else:
            return super().__lt__(other)

    def __le__(self, other):
        if not isinstance(other, uuid.UUID):
            return super().__le__(other)
        if self.version == 1 and other.version == 1:
            return self.__cmp__(other) < 1
        else:
            return super().__le__(other)

    def __gt__(self, other):
        if not isinstance(other, uuid.UUID):
            return super().__gt__(other)
        if self.version == 1 and other.version == 1:
            return self.__cmp__(other) == 1
        else:
            return super().__gt__(other)

    def __ge__(self, other):
        if not isinstance(other, uuid.UUID):
            return super().__ge__(other)
        if self.version == 1 and other.version == 1:
            return self.__cmp__(other) > -1
        else:
            return super().__ge__(other)

    @property
    def timestamp(self):
        return (self.time - 0x01b21dd213814000) / 1e7

    @property
    def datetime(self):
        return  datetime.datetime.fromtimestamp(self.timestamp, tz=datetime.timezone.utc)


MIN_TIMEUUID = TimeUUID(s='00000000-0000-1000-8080-808080808080')
MAX_TIMEUUID = TimeUUID(s='ffffffff-ffff-1fff-bf7f-7f7f7f7f7f7f')


import uuid
import unittest
import time
import os
from komlogd.api.common import timeuuid

class ApiCommonTimeUUIDTest(unittest.TestCase):

    def test_create_timeuuid_defaults(self):
        ''' creating a TimeUUID object with defaults should generate an object with the current
        timestamp, and randomized '''
        for i in range(1,1000):
            now = time.time()
            t = timeuuid.TimeUUID()
            self.assertTrue(isinstance(t, timeuuid.TimeUUID))
            # if this fails, you won the lottery
            self.assertFalse(t.clock_seq == 0 and t.node == 0)
            self.assertFalse(t.clock_seq == 0x80 and t.node == 0x808080808080)
            self.assertFalse(t.clock_seq == 0x3f7f and t.node == 0x7f7f7f7f7f7f)

    def test_create_timeuuid_with_timestamp(self):
        ''' creating a TimeUUID object with a timestamp and randomized '''
        for i in range(1,1000):
            now = time.time()
            t = timeuuid.TimeUUID(t=now)
            self.assertTrue(isinstance(t, timeuuid.TimeUUID))
            self.assertEqual(t.timestamp, int(now*1e6)/1e6)
            # if this fails, you won the lottery
            self.assertFalse(t.clock_seq == 0 and t.node == 0)
            self.assertFalse(t.clock_seq == 0x80 and t.node == 0x808080808080)
            self.assertFalse(t.clock_seq == 0x3f7f and t.node == 0x7f7f7f7f7f7f)

    def test_create_timeuuid_with_string(self):
        ''' creating a TimeUUID with a hex uuid1 should succeed '''
        for i in range(1,1000):
            u = uuid.uuid1()
            t = timeuuid.TimeUUID(s=u.hex)
            self.assertEqual(t,u)
            t = timeuuid.TimeUUID(s=str(u))
            self.assertEqual(t,u)

    def test_create_timeuuid_with_uuid4_string_should_fail(self):
        ''' creating a TimeUUID with a hex uuid4 should fail'''
        for i in range(1,100):
            u = uuid.uuid4()
            with self.assertRaises(ValueError) as cm:
                t = timeuuid.TimeUUID(s=u.hex)
            self.assertEqual(str(cm.exception), 'Invalid UUID type')
        for fn in [uuid.uuid3, uuid.uuid5]:
            for i in range(1,100):
                u = fn(uuid.NAMESPACE_DNS,str(os.urandom(10)))
                with self.assertRaises(ValueError) as cm:
                    t = timeuuid.TimeUUID(s=u.hex)
                self.assertEqual(str(cm.exception), 'Invalid UUID type')

    def test_create_timeuuid_with_timestamp_highest(self):
        ''' creating a TimeUUID object with a timestamp and highest possible value '''
        for i in range(1,1000):
            now = time.time()
            t = timeuuid.TimeUUID(t=now, highest=True)
            self.assertTrue(isinstance(t, timeuuid.TimeUUID))
            self.assertEqual(t.timestamp, int(now*1e6)/1e6)
            self.assertFalse(t.clock_seq == 0 and t.node == 0)
            self.assertFalse(t.clock_seq == 0x80 and t.node == 0x808080808080)
            self.assertTrue(t.clock_seq == 0x3f7f and t.node == 0x7f7f7f7f7f7f)

    def test_create_timeuuid_with_timestamp_lowest(self):
        ''' creating a TimeUUID object with a timestamp and lowest possible value '''
        for i in range(1,1000):
            now = time.time()
            t = timeuuid.TimeUUID(t=now, lowest=True)
            self.assertTrue(isinstance(t, timeuuid.TimeUUID))
            self.assertEqual(t.timestamp, int(now*1e6)/1e6)
            self.assertFalse(t.clock_seq == 0 and t.node == 0)
            self.assertTrue(t.clock_seq == 0x80 and t.node == 0x808080808080)
            self.assertFalse(t.clock_seq == 0x3f7f and t.node == 0x7f7f7f7f7f7f)

    def test_create_timeuuid_with_timestamp_no_random(self):
        ''' creating a TimeUUID object with a timestamp and a predictable value '''
        for i in range(1,1000):
            now = time.time()
            t = timeuuid.TimeUUID(t=now, random=False)
            self.assertTrue(isinstance(t, timeuuid.TimeUUID))
            t2 = timeuuid.TimeUUID(t=now, random=False)
            self.assertTrue(isinstance(t2, timeuuid.TimeUUID))
            self.assertEqual(t,t2)
            self.assertEqual(t.timestamp, int(now*1e6)/1e6)
            self.assertTrue(t.clock_seq == 0 and t.node == 0)
            self.assertFalse(t.clock_seq == 0x80 and t.node == 0x808080808080)
            self.assertFalse(t.clock_seq == 0x3f7f and t.node == 0x7f7f7f7f7f7f)

    def test_lowest_uuid_comparisson(self):
        ''' a TimeUUID object with lowest flag should never be greater than other with same t '''
        now = time.time()
        lowest = timeuuid.TimeUUID(t=now, lowest=True)
        # some random tests
        for i in range(1,10000):
            t2 = timeuuid.TimeUUID(t=now)
            self.assertTrue(lowest <= t2)
        # prepared tests
        lowest = timeuuid.TimeUUID(s='00000000-0000-1000-8080-808080808080')
        comparing_bytes = [
            timeuuid.TimeUUID(s='00000000-0000-1000-8081-808080808080'),
            timeuuid.TimeUUID(s='00000000-0000-1000-8080-818080808080'),
            timeuuid.TimeUUID(s='00000000-0000-1000-8080-808180808080'),
            timeuuid.TimeUUID(s='00000000-0000-1000-8080-808081808080'),
            timeuuid.TimeUUID(s='00000000-0000-1000-8080-808080818080'),
            timeuuid.TimeUUID(s='00000000-0000-1000-8080-808080808180'),
            timeuuid.TimeUUID(s='00000000-0000-1000-8080-808080808081')
        ]
        for other in comparing_bytes:
            self.assertTrue(lowest < other)

    def test_highest_uuid_comparisson(self):
        ''' a TimeUUID object with highest flag should never be less than other with same t '''
        now = time.time()
        highest = timeuuid.TimeUUID(t=now, highest=True)
        # some random tests
        for i in range(1,10000):
            t2 = timeuuid.TimeUUID(t=now)
            self.assertTrue(highest >= t2)
        # prepared tests
        highest = timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7f7f7f7f7f7f')
        comparing_bytes = [
            timeuuid.TimeUUID(s='00000000-0000-1000-bffe-7f7f7f7f7f7f'),
            timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7e7f7f7f7f7f'),
            timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7f7e7f7f7f7f'),
            timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7f7f7e7f7f7f'),
            timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7f7f7f7e7f7f'),
            timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7f7f7f7f7e7f'),
            timeuuid.TimeUUID(s='00000000-0000-1000-bfff-7f7f7f7f7f7e'),
        ]
        for other in comparing_bytes:
            self.assertTrue(highest > other)

    def test_magic_methods_comparisson(self):
        ''' Check magic methods comparisson works '''
        for i in range(1,10000):
            t1 = timeuuid.TimeUUID(t=i, random=False)
            t2 = timeuuid.TimeUUID(t=i, random=False)
            t3 = timeuuid.TimeUUID(t=i+1, random=False)
            t4 = timeuuid.TimeUUID(t=i+1, random=False)
            self.assertTrue(t1<=t2)
            self.assertFalse(t1<t2)
            self.assertTrue(t1<t3)
            self.assertTrue(t1<=t3)
            self.assertTrue(t3>t2)
            self.assertTrue(t3>=t2)
            self.assertTrue(t3>=t4)
            self.assertFalse(t3>t4)

    def test_TimeUUID_get_timestamp(self):
        ''' timestamp attribute should return the timestamp. We only keep microseconds precision '''
        for i in range(1,10000):
            t = time.time()
            tu = timeuuid.TimeUUID(t)
            self.assertEqual(tu.timestamp, int(t*1e6)/1e6)

    def test_TimeUUID_modify_timestamp_should_fail(self):
        ''' timestamp attribute cannot be modified '''
        t = time.time()
        tu = timeuuid.TimeUUID(t=t)
        with self.assertRaises(TypeError) as cm:
            tu.timestamp = 4
        self.assertEqual(tu.timestamp, int(t*1e6)/1e6)


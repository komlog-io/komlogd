import unittest
import uuid
import asyncio
import pandas as pd
from komlogd.api import session
from komlogd.api.common import crypto, exceptions
from komlogd.api.model.session import sessionIndex

class ApiSessionTest(unittest.TestCase):

    def test_komlogsession_creation_failure_invalid_username(self):
        ''' creating a KomlogSession object should fail if user is invalid '''
        username = 'Invalid username'
        privkey=crypto.generate_rsa_key()
        with self.assertRaises(exceptions.BadParametersException) as cm:
            session.KomlogSession(username=username, privkey=privkey)
        self.assertEqual(cm.exception.msg, 'Invalid username {}'.format(str(username)))

    def test_komlogsession_creation_username_modification_not_allowed(self):
        ''' username cannot be modified on la KomlogSession object '''
        username = 'username'
        privkey=crypto.generate_rsa_key()
        s = session.KomlogSession(username=username, privkey=privkey)
        with self.assertRaises(exceptions.BadParametersException) as cm:
            s.username = 'new_username'
        self.assertEqual(cm.exception.msg, 'username modification not allowed')

    def test_komlogsession_creation_failure_invalid_privkey(self):
        ''' creating a KomlogSession object should fail if privkey is invalid '''
        username = 'username'
        privkey='privkey'
        with self.assertRaises(exceptions.BadParametersException) as cm:
            session.KomlogSession(username=username, privkey=privkey)
        self.assertEqual(cm.exception.msg, 'Invalid private key')

    def test_komlogsession_creation_privkey_modification_not_allowed(self):
        ''' username cannot be modified on la KomlogSession object '''
        username = 'username'
        privkey=crypto.generate_rsa_key()
        s = session.KomlogSession(username=username, privkey=privkey)
        with self.assertRaises(exceptions.BadParametersException) as cm:
            s.privkey = crypto.generate_rsa_key()
        self.assertEqual(cm.exception.msg, 'private key modification not allowed')

    def test_komlogsession_creation_success(self):
        username = 'username'
        privkey=crypto.generate_rsa_key()
        s = session.KomlogSession(username=username, privkey=privkey)
        s2 = sessionIndex.get_session(sid=s.sid)
        self.assertEqual(s,s2)

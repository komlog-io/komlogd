import unittest
import uuid
from komlogd.api import session
from komlogd.api.common import crypto
from komlogd.api.model.session import sessionIndex

class ApiModelSessionTest(unittest.TestCase):

    def test_register_session_automatically_when_session_is_created(self):
        ''' creating a session should add it to the sessionIndex automatically '''
        username = 'username'
        privkey = crypto.generate_rsa_key()
        s = session.KomlogSession(username, privkey)
        self.assertTrue(s in sessionIndex.sessions)

    def test_register_session_only_add_the_session_once(self):
        ''' creating a session should add it to the sessionIndex automatically '''
        username = 'username'
        privkey = crypto.generate_rsa_key()
        sessionIndex.sessions = []
        s = session.KomlogSession(username, privkey)
        self.assertTrue(s in sessionIndex.sessions)
        self.assertEqual(len(sessionIndex.sessions),1)
        self.assertTrue(sessionIndex.register_session(s))
        self.assertTrue(s in sessionIndex.sessions)
        self.assertEqual(len(sessionIndex.sessions),1)

    def test_unregister_session(self):
        ''' unregistering a session should add the session to the session list '''
        username = 'username'
        privkey = crypto.generate_rsa_key()
        s = session.KomlogSession(username, privkey)
        self.assertTrue(s in sessionIndex.sessions)
        self.assertTrue(sessionIndex.unregister_session(s.sid))
        self.assertFalse(s in sessionIndex.sessions)

    def test_get_session_sid_None(self):
        ''' get_session should return the first session if sid is None '''
        username = 'username'
        privkey = crypto.generate_rsa_key()
        sessionIndex.sessions = []
        s1 = session.KomlogSession(username, privkey)
        self.assertTrue(s1 in sessionIndex.sessions)
        s2 = session.KomlogSession(username, privkey)
        self.assertTrue(s2 in sessionIndex.sessions)
        ss = sessionIndex.get_session()
        self.assertEqual(s1,ss)

    def test_get_session_sid(self):
        ''' get_session should return the session with sid '''
        username = 'username'
        privkey = crypto.generate_rsa_key()
        s1 = session.KomlogSession(username, privkey)
        self.assertTrue(s1 in sessionIndex.sessions)
        s2 = session.KomlogSession(username, privkey)
        self.assertTrue(s2 in sessionIndex.sessions)
        ss = sessionIndex.get_session(s1.sid)
        self.assertEqual(ss,s1)
        ss = sessionIndex.get_session(s2.sid)
        self.assertEqual(ss,s2)

    def test_get_session_sid_not_found(self):
        ''' get_session should return None if sid is not found '''
        sid = uuid.uuid4()
        self.assertIsNone(sessionIndex.get_session(sid))


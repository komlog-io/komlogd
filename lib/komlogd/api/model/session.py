import uuid

class SessionIndex:
    def __init__(self):
        self.sessions = []

    def register_session(self, session):
        if session is None:
            return False
        if not session in self.sessions:
            self.sessions.append(session)
        return True

    def unregister_session(self, sid):
        if sid is None or not isinstance(sid, uuid.UUID):
            return False
        to_remove = None
        for session in self.sessions:
            if session.sid == sid:
                to_remove = session
                break
        try:
            self.sessions.remove(to_remove)
        except ValueError:
            pass
        finally:
            return True

    def get_session(self, sid=None):
        ''' if sid is None, return first session '''
        if sid is None:
            for session in self.sessions:
                return session
            return None
        else:
            for session in self.sessions:
                if session.sid == sid:
                    return session
            return None

sessionIndex = SessionIndex()


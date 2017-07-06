import uuid

class SessionIndex:
    def __init__(self):
        self.sessions = {}

    def register_session(self, session):
        if not session:
            return False
        self.sessions[session.sid]=session
        return True

    def unregister_session(self, sid):
        if not sid or not isinstance(sid, uuid.UUID):
            return False
        self.sessions.pop(sid, None)
        return True

    def get_session(self, sid=None):
        if not sid:
            sids = list(self.sessions.keys())
            if len(sids)>0:
                return self.sessions[sids[0]]
            else:
                return None
        else:
            return self.sessions[sid] if sid in self.sessions else None

sessionIndex = SessionIndex()

class BadParametersException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class InvalidMessageException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class LoginException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class WebsocketConnectionException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class SessionException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class SessionNotFoundException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class CryptoException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

class SealedBrokenException(Exception):
    def __init__(self, msg=''):
        self.msg = msg


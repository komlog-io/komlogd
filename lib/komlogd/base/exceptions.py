class ConfigLoadException(Exception):
    def __init__(self, msg=''):
        self.msg= msg

class LoggerException(Exception):
    def __init__(self, msg=''):
        self.msg= msg

class CryptographyException(Exception):
    def __init__(self, msg=''):
        self.msg= msg

class BadParametersException(Exception):
    def __init__(self, msg=''):
        self.msg= msg


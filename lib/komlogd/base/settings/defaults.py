'''
Default values for komlogd configuration parameters

'''

APP_PATH = '.komlogd'
CONFIG_FILE = 'komlogd.yaml'
LOG_FILE='komlogd.log'
LOG_DIR='log/'
LOG_LEVEL='INFO'
LOG_ROTATION='yes'
LOG_MAX_BYTES=10000000
LOG_BACKUP_COUNT=3
LOG_FORMAT='%(asctime)s - %(levelname)s - %(processName)s - %(filename)s:%(lineno)d - %(message)s'
RSA_PRIV_KEY = 'key.priv'
RSA_PUB_KEY = 'key.pub'
PACKAGES_HOME = '.venvs/'
PACKAGES_VENV = 'default'
PACKAGES_ISOLATED = 'isolated'
PACKAGES_ENTRY_POINT = 'komlogd.package'


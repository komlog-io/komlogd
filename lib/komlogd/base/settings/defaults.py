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
LOG_FORMAT='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
EXT_JOBS='no'
RSA_PRIV_KEY = 'key.priv'
RSA_PUB_KEY = 'key.pub'


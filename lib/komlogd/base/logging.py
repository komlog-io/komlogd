'''
logging methods

'''

import os
import logging
from komlogd.base import config, exceptions
from komlogd.base.settings import defaults,options
from logging.handlers import RotatingFileHandler

logger=None

def initialize_logger():
    global logger
    if logger:
        logger=None
    logger=logging.getLogger()
    log_config=config.config.get_logging_entries()
    if len(log_config)==0:
        log_level=defaults.LOG_LEVEL
        rotate_logs=defaults.LOG_ROTATION
        max_bytes=defaults.LOG_MAX_BYTES
        backup_count=defaults.LOG_BACKUP_COUNT
        log_format=defaults.LOG_FORMAT
        log_dir=os.path.join(config.config.root_dir,defaults.LOG_DIR)
        log_file=defaults.LOG_FILE
    else:
        log_level=log_config[0].get(options.LOG_LEVEL,defaults.LOG_LEVEL)
        rotate_logs=log_config[0].get(options.LOG_ROTATION,defaults.LOG_ROTATION)
        max_bytes=log_config[0].get(options.LOG_MAX_BYTES, defaults.LOG_MAX_BYTES)
        backup_count=log_config[0].get(options.LOG_BACKUP_COUNT, defaults.LOG_BACKUP_COUNT)
        log_format=log_config[0].get(options.LOG_FORMAT, defaults.LOG_FORMAT)
        log_dir=log_config[0].get(options.LOG_DIR, os.path.join(config.config.root_dir,defaults.LOG_DIR))
        if not os.path.isabs(log_dir):
            log_dir=os.path.join(config.config.root_dir,log_dir)
        log_file=log_config[0].get(options.LOG_FILE, defaults.LOG_FILE)
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file=os.path.join(log_dir,os.path.split(log_file)[1])
        if rotate_logs is True:
            handler=RotatingFileHandler(log_file,'a',maxBytes=int(max_bytes),backupCount=int(backup_count))
        else:
            handler=logging.FileHandler(log_file)
        formatter=logging.Formatter(log_format)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)
        if not logger:
            raise exceptions.LoggerException()
        if len(log_config)>1:
            logger.info('Found more than one logger configuration, using the first one found.')
    except Exception as e:
        print(str(e))
        raise exceptions.LoggerException()


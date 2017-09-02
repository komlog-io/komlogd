'''
logging methods

'''

import os
import logging
from komlogd.base import config, exceptions
from logging.handlers import RotatingFileHandler

logger=None

def initialize_logging(process_name):
    global logger
    class ProcessFilter(logging.Filter):
        def filter(self, record):
            record.processName = process_name
            return True
    logger=None
    logger=logging.getLogger()
    log_config=config.config.logging
    log_level=log_config['log_level']
    rotate_logs=log_config['rotate_logs']
    max_bytes=log_config['max_bytes']
    backup_count=log_config['backup_count']
    log_format=log_config['log_format']
    log_dir=log_config['log_dir']
    log_file=log_config['log_file']
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file=os.path.join(log_dir,os.path.basename(log_file))
    if rotate_logs is True:
        handler=RotatingFileHandler(log_file,'a',maxBytes=int(max_bytes),backupCount=int(backup_count))
    else:
        handler=logging.FileHandler(log_file)
    formatter=logging.Formatter(log_format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.addFilter(ProcessFilter())
    logger.setLevel(log_level)
    if not logger:
        raise exceptions.LoggerException('Could not create logger')


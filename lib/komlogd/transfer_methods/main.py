'''

transfer methods

'''
import os
import traceback
import importlib.machinery
from komlogd.base import config, logging
from komlogd.base.settings import options

def load_transfer_methods_files():
    entries = config.config.get_entries(entryname=options.ENTRY_TRANSFERS)
    files=[]
    for entry in entries:
        try:
            enabled=entry[options.TRANSFERS_ENABLED]
            filename=entry[options.TRANSFERS_FILE]
            if enabled is True:
                logging.logger.debug('adding transfer methods file: '+str(filename))
                files.append(filename)
        except Exception:
            logging.logger.error('Error loading transfer methods configuration.')
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
    if len(files) == 0:
        logging.logger.info('No transfer methods loaded.')
    for i,item in enumerate(files):
        if not os.path.isabs(item):
            path=os.path.join(config.config.root_dir,item)
        else:
            path=item
        try:
            importlib.machinery.SourceFileLoader(fullname='dynimp'+str(i),path=path).load_module()
        except Exception:
            logging.logger.error('Error loading transfer methods file '+str(item))
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)


'''

impulse methods

'''
import os
import traceback
import importlib.machinery
from komlogd.base import config, logging
from komlogd.base.settings import options

def load_impulse_files():
    entries = config.config.get_entries(entryname=options.ENTRY_IMPULSES)
    files=[]
    for entry in entries:
        try:
            enabled=entry[options.JOB_ENABLED]
            filename=entry[options.IMPULSE_FILE]
            if enabled is True:
                logging.logger.debug('adding impulse file: '+str(filename))
                files.append(filename)
        except Exception:
            logging.logger.error('Error loading impulse configuration.')
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
    if len(files) == 0:
        logging.logger.info('No impulse methods loaded.')
    for i,item in enumerate(files):
        if not os.path.isabs(item):
            path=os.path.join(config.config.root_dir,item)
        else:
            path=item
        try:
            importlib.machinery.SourceFileLoader(fullname='dynimp'+str(i),path=path).load_module()
        except Exception:
            logging.logger.error('Error loading impulse file '+str(item))
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)


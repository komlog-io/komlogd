'''
File that implements configuration classes and methods

'''

import os
import yaml
import traceback
from komlogd.base import exceptions, logging
from komlogd.base.settings import defaults, templates, options

config=None

class Config():
    def __init__(self, filename):
        self.filename=filename
        self.root_dir = os.path.split(filename)[0]
        self.entries=None

    def _load_entries(self, filename=None, entryname=None):
        if filename is None:
            filename=self.filename
        try:
            entries=yaml.safe_load(open(filename,'r'))
        except Exception as e:
            raise exceptions.ConfigLoadException(str(e))
        if entries is None:
            return
        if entryname is None:
            for entry in entries:
                self.entries.append(entry)
        else:
            for entry in entries:
                if entryname in entry:
                    self.entries.append(entry)

    def _load_external_entries(self):
        logging.logger.info('Loading external entries')
        job_files=[]
        ext_jobs_enabled=False
        ext_jobs=self.get_entries(entryname=options.EXT_JOBS)
        if len(ext_jobs)>0:
            if len(ext_jobs)==1:
                ext_jobs_enabled=ext_jobs[0]
            else:
                logging.logger.info(options.EXT_JOBS+' option found more than once, ignoring.')
        if ext_jobs_enabled:
            logging.logger.info('External jobs enabled, loading them.')
            entries=self.get_entries(entryname=options.EXT_JOB_FILE)
            for entry in entries:
                job_files.append(entry)
        else:
            logging.logger.info('External jobs not enabled.')
        for item in job_files:
            logging.logger.info('Loading external jobs from file '+item)
            try:
                self._load_entries(filename=item, entryname=options.ENTRY_JOB)
            except Exception:
                logging.logger.info('Error loading external jobs from file '+item+'. Skipping')
                ex_info=traceback.format_exc().splitlines()
                for line in ex_info:
                    logging.logger.error(line)

    def get_entries(self, entryname):
        if self.entries is None:
            logging.logger.info('Loading configuration file entries')
            self.entries=[]
            self._load_entries()
            self._load_external_entries()
        entries = []
        for entry in self.entries:
            if entryname in entry:
                entries.append(entry[entryname])
        return entries

    def get_logging_entries(self):
        ''' Before loading all configuration entries we initialize logging '''
        try:
            entries=yaml.safe_load(open(self.filename,'r'))
        except Exception as e:
            raise exceptions.ConfigLoadException(str(e))
        config = []
        if entries is None:
            return config
        for entry in entries:
            if options.ENTRY_LOG in entry:
                config.append(entry[options.ENTRY_LOG])
        return config


def load_application_config(filename=None):
    global config
    if not filename:
        filename=get_default_config_file()
        return load_application_config(filename)
    elif os.path.isfile(filename):
        config=Config(filename)
        if config:
            return True
    else:
        default_filename=get_default_config_file()
        if filename != default_filename:
            raise exceptions.ConfigLoadException('File not found: '+str(filename))
        elif _create_config_file(filename):
            return load_application_config(filename)
    raise exceptions.ConfigLoadException()

def get_default_config_file():
    home_dir = os.path.expanduser('~')
    filename = os.path.join(home_dir,defaults.APP_PATH,defaults.CONFIG_FILE)
    if not filename:
        raise exceptions.ConfigLoadException()
    return filename

def _create_config_file(filename):
    global config
    dirname,_ = os.path.split(filename)
    try:
        os.makedirs(dirname)
    except OSError as Err:
        if Err.errno == 17:
            pass
        else:
            print (str(Err))
            return False
    try:
        with open(filename,'wb') as f_out:
            f_out.write(bytes(templates.TEMPLATE_CONFIG_FILE,'UTF-8'))
            f_out.close()
        return True
    except Exception as e:
        ex_info=traceback.format_exc().splitlines()
        for line in ex_info:
            print(line)
        return False


'''
File that implements configuration classes and methods

'''

import os
import hashlib
import yaml
import traceback
from komlogd.base import exceptions, logging
from komlogd.base.settings import defaults, templates, options

config=None

class Config():
    def __init__(self, filename=None):
        self.filename = os.path.abspath(filename) if filename else os.path.abspath(get_default_config_file())
        self.root_dir = os.path.dirname(os.path.abspath(filename))
        with open(self.filename,'r') as cfile:
            config_entries = yaml.safe_load(cfile)
            self._config_entries = config_entries if config_entries != None else []

    @property
    def username(self):
        try:
            return self._username
        except AttributeError:
            items = self._get_entries(options.KOMLOG_USERNAME)
            if len(items)==0:
                raise exceptions.BadParametersException('Set username in configuration file.')
            elif len(items)>1:
                raise exceptions.BadParametersException('More than one username found in configuration file. Keep only one.')
            else:
                self._username = items[0]
            return self._username

    @property
    def key(self):
        try:
            return self._key
        except AttributeError:
            items = self._get_entries(options.KOMLOG_KEYFILE)
            if len(items)==0:
                self._key = os.path.join(self.root_dir,defaults.RSA_PRIV_KEY)
            elif len(items)>1:
                raise exceptions.BadParametersException('More than one key found in configuration file. Keep only one.')
            else:
                self._key = items[0]
            return self._key

    @property
    def logging(self):
        try:
            return self._logging
        except AttributeError:
            logging = {}
            items = self._get_entries(options.ENTRY_LOG)
            if len(items) == 0:
                logging['log_level'] = defaults.LOG_LEVEL
                logging['rotate_logs'] = defaults.LOG_ROTATION
                logging['max_bytes'] = defaults.LOG_MAX_BYTES
                logging['backup_count'] = defaults.LOG_BACKUP_COUNT
                logging['log_format'] = defaults.LOG_FORMAT
                logging['log_dir'] = os.path.join(self.root_dir,defaults.LOG_DIR)
                logging['log_file'] = defaults.LOG_FILE
            else:
                logging['log_level'] = items[0].get(options.LOG_LEVEL,defaults.LOG_LEVEL)
                logging['rotate_logs'] = items[0].get(options.LOG_ROTATION,defaults.LOG_ROTATION)
                logging['max_bytes'] = items[0].get(options.LOG_MAX_BYTES, defaults.LOG_MAX_BYTES)
                logging['backup_count'] = items[0].get(options.LOG_BACKUP_COUNT, defaults.LOG_BACKUP_COUNT)
                logging['log_format'] = items[0].get(options.LOG_FORMAT, defaults.LOG_FORMAT)
                logging['log_dir'] = items[0].get(options.LOG_DIR, os.path.join(self.root_dir,defaults.LOG_DIR))
                if not os.path.isabs(logging['log_dir']):
                    logging['log_dir'] = os.path.join(self.root_dir,logging['log_dir'])
                logging['log_file'] = items[0].get(options.LOG_FILE, defaults.LOG_FILE)
            self._logging = logging
            return self._logging

    @property
    def packages(self):
        try:
            return self._packages
        except AttributeError:
            packages = []
            items = self._get_entries(options.ENTRY_PACKAGE)
            for i,item in enumerate(items):
                try:
                    pkg = item[options.PACKAGE_INSTALL]
                    venv = item[options.PACKAGE_VENV]
                    enabled = item[options.PACKAGE_ENABLED]
                    if pkg and enabled is True:
                        if not venv:
                            venv = defaults.PACKAGES_VENV
                        elif venv == defaults.PACKAGES_ISOLATED:
                            m = hashlib.md5()
                            m.update((str(i)+pkg).encode('utf-8'))
                            venv = m.hexdigest()
                    packages.append({
                        'install':pkg,
                        'venv': venv,
                        'enabled':enabled
                    })
                except Exception:
                    logging.logger.error('Error loading package configuration.')
                    ex_info=traceback.format_exc().splitlines()
                    for line in ex_info:
                        logging.logger.error(line)
            self._packages = packages
            return self._packages

    def _get_entries(self, name):
        entries = []
        for entry in self._config_entries:
            if name in entry:
                entries.append(entry[name])
        return entries

def initialize_config(filename=None):
    global config
    if not filename:
        filename=get_default_config_file()
        return initialize_config(filename)
    elif os.path.isfile(filename):
        config=Config(filename)
        return True
    else:
        default_filename=get_default_config_file()
        if filename != default_filename:
            raise exceptions.ConfigLoadException('File not found: '+str(filename))
        elif _create_config_file(filename):
            return initialize_config(filename)
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


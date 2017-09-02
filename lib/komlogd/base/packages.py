import asyncio
import importlib
import os
import pkg_resources
import shlex
import shutil
import subprocess
import sys
import traceback
import types
import venv
from komlogd import __version__
from komlogd.base import config, logging
from komlogd.base.settings import defaults


class KomlogVEnv(venv.EnvBuilder):

    def __init__(self, preinstalled, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.preinstalled = preinstalled

    def post_setup(self, context):
        for pkg in self.preinstalled:
            install_package(context.env_exe, pkg)

def install_package(py_exec, pkg):
    pkg_args = shlex.split(pkg)
    cwd = os.path.dirname(py_exec)
    args = [py_exec, 'pip', 'install'] + pkg_args
    logging.logger.info('Installing package '+pkg)
    my_env = os.environ.copy()
    my_env["GIT_SSH_COMMAND"] = "ssh -oStrictHostKeyChecking=no -vvv -i "+config.config.key
    result = subprocess.run(args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=cwd, encoding='utf-8', env=my_env)
    try:
        result.check_returncode()
    except subprocess.CalledProcessError:
        logging.logger.error('Error installing package '+pkg)
        logging.logger.error(result.stdout)
        logging.logger.error(result.stderr)
        raise
    else:
        logging.logger.debug(result.stdout)
        logging.logger.info('Package '+pkg+' installed')

def create_venvs():
    packages = config.config.packages
    config_venvs = set([pkg['venv'] for pkg in packages if pkg['enabled']])
    virtualenvs_path = os.path.join(config.config.root_dir,defaults.PACKAGES_HOME)
    venvs = []
    for name in config_venvs:
        dest_path=os.path.join(virtualenvs_path,name)
        env = KomlogVEnv(preinstalled=['komlogd=='+__version__], symlinks=False, with_pip=True)
        try:
            logging.logger.info('Creating virtualenv: '+name)
            env.create(dest_path)
        except subprocess.CalledProcessError:
            logging.logger.error('Aborting virtualenv initialization: '+name)
        else:
            venvs.append({'name':name,'path':dest_path})
    existing_venvs = next(os.walk(virtualenvs_path))[1] if os.path.isdir(virtualenvs_path) else []
    for subdir in existing_venvs:
        if subdir not in config_venvs:
            shutil.rmtree(os.path.join(virtualenvs_path,subdir))
    return venvs

def boot_venv(info):
    python_exec = os.path.join(info['path'],'bin/python')
    komlogd_exec = os.path.join(info['path'],'bin/komlogd')
    bin_path = os.path.join(info['path'],'bin')
    args = [python_exec, komlogd_exec, '-v', info['name'], '-c', info['config']]
    logging.logger.info('Booting virtualenv: '+info['name'])
    p = subprocess.Popen(args, cwd=bin_path, shell=False)
    return p

def load_venv_packages(env_name):
    packages = config.config.packages
    cmds = set([pkg['install'] for pkg in packages if pkg['venv'] == env_name])
    for cmd in cmds:
        try:
            install_package(sys.executable, cmd)
        except subprocess.CalledProcessError:
            return False
    return True

async def load_entry_points():
    importlib.reload(pkg_resources)
    for ep in pkg_resources.iter_entry_points(group=defaults.PACKAGES_ENTRY_POINT):
        logging.logger.info('loading entry_point: '+str(ep))
        try:
            f = ep.load()
            if asyncio.iscoroutinefunction(f):
                await f()
            elif isinstance(f, types.FunctionType):
                f()
        except (ModuleNotFoundError,SyntaxError):
            logging.logger.error('Error loading package entry point.')
            ex_info=traceback.format_exc().splitlines()
            for line in ex_info:
                logging.logger.error(line)
            return False
        else:
            logging.logger.info('entry_point loaded successfully: '+str(ep))
    return True


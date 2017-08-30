'''
Templates file

'''

TEMPLATE_CONFIG_FILE='''#
# komlogd configuration file
#
# User Info
# ---------
#
# Komlog username. Set it and uncomment the following line:
#
#- username: username
#
# Agent private key. Default value relative to configuration directory. Uncomment only if you want to use
# a different private key file.
#
#- keyfile: ./key.priv
#
#
# Packages
# --------
#
# At boot, komlogd will load packages found in `package` blocks. A package block accepts these
# parameters:
#     - install: specifies how to install the package using pip install. It can be a PyPI package name,
#   a repository url, a filesystem path, etc. Anything supported by `pip install` is accepted.
#   You can set options too, like --force-reinstall, --upgrade, and so on, to control if the package
#   will be reinstalled/upgraded on every agent reboot.
#     - enabled: if the package is enabled or not.
#     - venv: the virtual environment where the package will be executed. Komlogd will create virtual environments
#   for its packages to run on. This parameter is optional, if you don't set it, the virtual environment
#   assigned will be the `default` one. `unique` is a reserved value, to specify that the package must run
#   in a virtual environment alone.
#   Virtual environments are created in the directory .venvs inside komlogd configuration directory.
#
# E.g:
#
#- package:
#    install: --upgrade SomePackage
#    enabled: yes
#    venv: my_virtualenv
#
#
# Logging
# -------
#
# logging configuration. Default values are indicated:
#
#- logging:
#    level: INFO
#    rotation: yes
#    max_bytes: 10000000
#    backup_count: 3
#    dirname: log
#    filename: komlogd.log
#
#
'''


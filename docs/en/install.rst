.. _install_and_first_steps:

Install and first steps
=======================

Automatic install
^^^^^^^^^^^^^^^^^

komlogd is on `PyPI <http://pypi.python.org/pypi/komlogd>`_ and can be
installed with ``pip`` or ``easy_install``::

    pip install komlogd



Requirements
^^^^^^^^^^^^

komlogd needs Python 3.5+. It also has some aditional requirements (that
will install automatically with ``pip``):

* `aiohttp <http://pypi.python.org/pypi/aiohttp>`_

* `cryptography <http://pypi.python.org/pypi/cryptography>`_

* `pandas <http://pypi.python.org/pypi/pandas>`_

* `pyyaml <http://pypi.python.org/pypi/pyyaml>`_


.. note::
    To install dependencies, pip will need some of your distribution packages, like ``gcc``, ``libffi-dev``, ``numpy``, etc.
    On slow devices, installing pandas and cryptography can take a lot of time, so maybe you
    prefer to install them directly using your distribution's package manager.


.. _first_execution:

First execution
^^^^^^^^^^^^^^^

Once installed, we can start komlogd with this command::

    komlogd &

During its first execution, komlogd will create and initialize the necesary files and RSA keys
needed for communicating with `Komlog <http://www.komlog.io>`_. After the initialization komlogd will
terminate.

By default komlogd will create a new directory inside user's ``$HOME`` directory called ``.komlogd`` with the following contents::

    .komlogd
    ├── key.priv
    ├── key.pub
    ├── komlogd.yaml
    └── log
        └── komlogd.log


Contents created are:

* **key.priv**: private RSA key file.

* **key.pub**: public RSA key file. This key is the one we have to add at Komlog web to authorize the agent in Komlog.

* **komlogd.yaml**: main configuration file.

* **log/komlogd.log**: log file.

 .. note::
    Private key will never be sent to Komlog.


At this moment, we can start with komlogd :ref:`configuration`.



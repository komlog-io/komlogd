.. title:: Komlogd

Komlogd
=======

Komlogd is the agent for communicating with `Komlog platform <http://www.komlog.io>`_, a distributed
execution platform aimed at visualizing and sharing time series.

Komlogd is built in python and can be used as an independent daemon or as an API,
so you can add Komlog functionality to your own python code.

**Automatic install**::

    pip install komlogd


Komlogd is listed in `PyPI <http://pypi.python.org/pypi/komlogd>`_ so you can install
with ``pip`` or ``easy_install``.

**Requirements**

Komlogd needs python 3.5+. It also has some aditional dependencies (that will
install automatically if you use ``pip``):

* `aiohttp <http://pypi.python.org/pypi/aiohttp>`_

* `cryptography <http://pypi.python.org/pypi/cryptography>`_

* `pandas <http://pypi.python.org/pypi/pandas>`_

* `pyyaml <http://pypi.python.org/pypi/pyyaml>`_

.. note::
    To install dependencies, pip will need some of your distribution packages, like ``gcc``, ``libffi-dev``, ``numpy``, etc.
    On slow devices, installing pandas and cryptography can take a lot of time, so maybe you
    prefer to install them directly using your distribution's package manager.

**Docs**

.. toctree::
   :maxdepth: 3

   install
   configuration
   api



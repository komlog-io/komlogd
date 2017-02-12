.. title:: Komlogd

Komlogd
=======

Komlogd es el agente del servicio web `Komlog <http://www.komlog.io>`_, siendo éste
una plataforma pensada para visualizar y compartir series temporales.

Komlogd está escrito en python y puede utilizarse como aplicación independiente
o como API, para así integrar fácilmente la funcionalidad ofrecida por Komlog
en cualquier aplicación python.

**Instalación automática**::

    pip install komlogd


Komlogd está en el listado de `PyPI <http://pypi.python.org/pypi/komlogd>`_  se puede 
instalar con ``pip`` o ``easy_install``.

**Requisitos**

Komlogd necesita Python 3.5 o superior. Además, necesita los siguientes paquetes
adicionales (que se instalarán con ``pip`` automáticamente):

* `aiohttp <http://pypi.python.org/pypi/aiohttp>`_

* `cryptography <http://pypi.python.org/pypi/cryptography>`_

* `pandas <http://pypi.python.org/pypi/pandas>`_

* `pyyaml <http://pypi.python.org/pypi/pyyaml>`_


.. note::
    Para instalar las dependencias, pip necesitará varios paquetes de tu distribución, entre ellos ``gcc``, ``libffi-dev``,
    ``numpy``, etc. 
    En dispositivos lentos la instalación de pandas y cryptography con ``pip`` puede tardar bastante, por lo que tal
    vez prefieras instalar directamente los paquetes que proporciona tu distribución.


**Documentación**

.. toctree::
   :maxdepth: 3

   install
   configuration
   api


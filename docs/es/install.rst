.. _instalacion_y_primeros_pasos:

Instalación y primeros pasos
============================

Instalación automática
^^^^^^^^^^^^^^^^^^^^^^

Komlogd está en el listado de `PyPI <http://pypi.python.org/pypi/komlogd>`_ y se puede 
instalar con ``pip`` o ``easy_install``::

    pip install komlogd



Requisitos
^^^^^^^^^^

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

.. _primera_ejecucion:

Primera ejecución
^^^^^^^^^^^^^^^^^

Una vez instalado el agente, lo arrancaremos ejecutando el siguiente comando::

    komlogd &

Durante esta primera ejecución, komlogd inicializará los archivos de configuración y creará el par de claves RSA utilizado para la comunicación con `Komlog <http://www.komlog.io>`_.
Debido a que el agente crea el archivo de configuración sin establecer variables necesarias para su funcionamiento, como por ejemplo el usuario de acceso a `Komlog <http://www.komlog.io>`_, el agente terminará su ejecución.

En el directorio ``$HOME`` del usuario que lanzó la ejecución de komlogd se debería haber creado el directorio ``.komlogd`` con la siguiente estructura::

    .komlogd
    ├── key.priv
    ├── key.pub
    ├── komlogd.yaml
    └── log
        └── komlogd.log


Los archivos creados son los siguientes:

* **key.priv**: Es la clave privada del agente.

* **key.pub**: Es la clave pública del agente. Esta clave la añadiremos a través de la web de Komlog para autorizar al agente.

* **komlogd.yaml**: Archivo principal de Komlogd, con toda la configuración necesaria del agente.

* **log/komlogd.log**: Archivo de log del agente.

 .. note::
    La clave privada del agente nunca se transmitirá a Komlog


Una vez llegados a este punto, podemos pasar a la :ref:`configuracion` de komlogd.


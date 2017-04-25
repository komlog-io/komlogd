.. _configuracion:

Configuración
=============

Configuración de acceso a Komlog
--------------------------------

Una vez que tenemos inicializada la estructura básica de ficheros y directorios de komlogd,
como se explica en el punto :ref:`primera_ejecucion`, es el momento de configurar komlogd
para que pueda comunicarse con `Komlog <http://www.komlog.io>`_.

Configuración de usuario y clave RSA
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Para que komlogd tenga acceso a Komlog, debemos indicar el usuario de conexión y la clave
del agente en el archivo de configuración (**komlogd.yaml**).

El usuario lo especificaremos estableciendo la siguiente variable::

    - username: <username>

Sustituyendo *<username>* por nuestro usuario de acceso a Komlog.

La clave del agente por defecto será la generada durante la primera ejecución,
que debe estar en la ruta ``$HOME/.komlogd/key.priv``. En cualquier caso, si quisieramos utilizar
una clave diferente, podríamos establecerla mediante la siguiente variable en el archivo de
configuración::

    - keyfile: <path_to_key.priv>

Sustituyendo *<path_to_key.priv>* por el archivo con nuestra clave RSA privada.

Si no queremos utilizar una clave distinta a la generada automáticamente, no es necesario establecer
esa variable.

.. note::
    La clave privada del agente nunca se transmitirá a Komlog

.. _autorizacion_agente:

Autorización del agente en Komlog
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Para que el agente pueda enviar información a Komlog, es necesario añadir la clave pública
al listado de claves autorizadas. Para ello realizaremos lo siguiente:

* Accederemos a la `página de configuración <https://www.komlog.io/config>`_ de nuestra cuenta de Komlog.
* Vamos al submenú ``Agents`` y hacemos click sobre el botón ``New Agent``.
* Se mostrará un formulario con dos campos, en el primero de ellos introduciremos el
  nombre que queremos dar al agente. En el segundo campo pegaremos el contenido de nuestra
  clave pública, por defecto almacenada en el archivo ``key.pub`` de nuestro directorio de komlogd.
* Por último haremos click en el botón ``Add key``.


.. image:: _static/new_key.png


Enviando datos por primera vez a Komlog
---------------------------------------

Vamos a enviar nuestros primeros datos a Komlog.
Para este ejemplo se enviará la salida del comando *df -k*. En Komlog, el usuario organiza sus datos
como si fuese un sistema de archivos, por lo que a los datos que vamos a subir le asignaremos la **uri**
(equivalente al path de un sistema de archivos) **host.occupation**.

Para subir los datos ejecutamos::

    df -k | komlogd -u host.occupation

Si todo ha ido correctamente, en nuestra `página principal de Komlog <https://www.komlog.io/home>`_ aparecerá
el contenido que acabamos de subir asociado a la
ruta *host.occupation*. Sobre esos datos podemos identificar directamente las variables que queramos
monitorizar, compartir con otros usuarios, etc.

Ahora podemos programar la ejecución del comando anterior para subir los datos periódicamente a Komlog. Esto lo
podemos hacer a través del cron del usuario, de systemd, etc. Cada vez que komlog reciba estos datos identificará
automáticamente las métricas que le hayamos indicado.


Puesto que las métricas en Komlog se almacenan de forma estructurada, como si de un sistema de archivos
se tratase, existe la posibilidad de crear varios niveles de métricas anidadas.
El separador de niveles en Komlog es el punto (.). Por ejemplo, si tenemos las siguientes
uris: *system.info.disk*, *system.info.disk.sda1* y *system.info.disk.sda2*, éstas se anidarían de la siguiente manera::

    system
    └─ info
       └─ disk
          ├─ sda1
          └─ sda2

.. important::
    Una uri puede contener **exclusivamente** los siguientes caracteres:

    * Letras [A-Z] mayúsculas o minúsculas: Caracteres ASCII de la *A* a la *Z* mayúsculas o minúsculas. No son válidos caracteres
      no ASCII como la *ñ*, o acentuados (*á,é*), etc.
    * Números [0-9].
    * Caracteres especiales:

        * Guión (-), underscore (_)
        * Punto (.)

    La uri **no puede empezar** por el carácter especial punto (.).


Configuración de funciones de transferencia
-------------------------------------------

Una *función de transferencia* es una función que se ejecuta cuando se actualizan los
datos de una o varias *uris*.

Esta funcionalidad la podemos utilizar para automatizar tareas, generación de alarmas,
comunicación con servicios externos, análisis de datos en tiempo real y, en definitiva, cualquier tarea
que se nos ocurra que pueda estar asociada a eventos.

Gracias a esta funcionalidad, podemos construir sistemas basados en arquitectura lambda.

En el apartado :ref:`funciones_de_transferencia` se explica cómo crear este tipo de funciones
correctamente.

Una vez que tenemos el archivo con las funciones de transferencia, para añadirlas a la configuración de komlogd
editaríamos el archivo de configuración (**komlogd.yaml**) y añadiríamos un nuevo bloque **transfers**
como el siguiente::

    - transfers:
        enabled: yes
        filename: <path_to_file>

Los parametros del *bloque transfers* son los siguientes:

* **enabled**: Puede tomar los valores *yes* o *no*. Indica si el bloque *transfers* está habilitado.

* **filename**: Ruta del archivo que contiene las *funciones de transferencia*. La ruta puede ser absoluta o relativa al directorio
  de configuración de komlogd.

Se pueden añadir tantos *bloques transfers* como se desee.

Configuración del nivel de log
------------------------------

komlogd permite adaptar algunos de los parámetros de logging en función de nuestras preferencias.

La configuración de logs viene establecida en el bloque *logging* dentro del archivo de configuración
de komlogd (*komlogd.yaml*). Por defecto tiene estos valores::

    - logging:
        level: DEBUG
        rotation: yes
        max_bytes: 10000000
        backup_count: 3
        dirname: log
        filename: komlogd.log

Los parámetros del *bloque logging* son los siguientes:

* **level**: Indica el nivel de log. Los valores posibles son *CRITICAL, ERROR, WARNING,
  INFO, DEBUG, NOTSET*.
* **rotation**: Indica si se rotará el archivo de logs. Los valores posibles son *yes* o *no*.
* **max_bytes**: En caso de rotar el fichero, indica el tamaño en bytes que tiene que alcanzar para que se rote.
* **backup_count**: Indica el número de rotaciones a almacenar del fichero de logs.
* **dirname**: Directorio en el que se almacenará el fichero de log. La ruta puede ser absoluta o relativa al directorio
  de configuración de komlogd.
* **filename**: Nombre del fichero de logs.

Una vez que hayamos configurado komlogd, podemos proceder a su ejecución como ya vimos en el apartado :ref:`instalacion_y_primeros_pasos`::

    komlogd &


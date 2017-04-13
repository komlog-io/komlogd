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


Configuración de tareas programadas
-----------------------------------

komlogd permite la ejecución de tareas programadas y el envío del resultado a Komlog.
Esta funcionalidad te permite visualizar estos resultados via web, identificar variables
directamente en los textos, o compartir el contenido con otros usuarios.

Supongamos que queremos enviar cada hora la salida del comando "*df -k*" a Komlog.

Para ello editaríamos el archivo de configuración de komlogd (**komlogd.yaml**) y añadiríamos
un bloque **job** como el siguiente::

    - job:
        uri: system.info.disk
        command: df -k
        enabled: yes
        schedule:
            - '0 * * * *'

Los parámetros que definen el *bloque job* son los siguientes:

* **uri**: La uri es un identificador único que asignamos a nuestros datos dentro de Komlog.

En Komlog, todos los datos que el usuario sube se organizan en un árbol como si de
un sistema de ficheros se tratara. Cada identificador dentro de este árbol se conoce
como **uri**.

La *uri* identifica de forma unívoca el dato que estamos subiendo, permitiéndonos
identificarlos y localizarlos rápidamente. Al igual que con un sistema de ficheros,
existe la posibilidad de anidar los datos en diferentes niveles. Si en sistemas
*UNIX* se utiliza el carácter */* para ello, en Komlog se utiliza el punto *.*

Por ejemplo, si tenemos las siguientes uris: *system.info.disk* y *system.info.disk.sda1*, éstas se
anidarían de la siguiente manera::
    
    system.info.disk
    └── sda1

.. important::
    Una uri puede contener **exclusivamente** los siguientes caracteres:

    * Letras [A-Z] mayúsculas o minúsculas: Caracteres ASCII de la *A* a la *Z* mayúsculas o minúsculas. No son válidos caracteres
      no ASCII como la *ñ*, o acentuados (*á,é*), etc.
    * Números [0-9].
    * Caracteres especiales:

        * Guión (-), underscore (_)
        * Punto (.)

    La uri **no puede empezar** por el carácter especial punto (.).

* **command**: Es el comando a ejecutar.

Se puede indicar un comando del sistema operativo o cualquier script. La salida por pantalla será lo que se envíe
a Komlog (La salida por *stdout*, la salida *stderr* no se envía).

.. important::
    Hay que tener en cuenta que en el comando a ejecutar no se pueden añadir caracteres especiales como son las **tuberías (|)**, o
    **redirecciones (<,>)**, por lo que si se desean ejecutar comandos enlazados mediantes tuberías o redirecciones habría que
    hacerlo en un script.

* **enabled**: Puede tomar los valores *yes* o *no*. Indica si el *job* está habilitado.

* **schedule**: El schedule determina cuándo se ejecutará el job. Se utiliza el siguiente formato::

         ┌───────────── minutos (0 - 59)
         │ ┌────────────── horas (0 - 23)
         │ │ ┌─────────────── día del mes (1 - 31)
         │ │ │ ┌──────────────── mes (1 - 12)
         │ │ │ │ ┌───────────────── día de la semana (0 - 6) (Domingo a Sábado)
         │ │ │ │ │
         │ │ │ │ │
         │ │ │ │ │
         * * * * *

Además acepta los siguientes caracteres especiales:

* El asterisco (*) para indicar todos los posibles valores de un grupo.
* La coma (,) para indicar varios valores en un grupo.
* El carácter */* para indicar los valores de una división cuyo resto sea 0. Por ejemplo, en lugar de indicar
  los minutos *0,10,20,30,40,50* podemos indicar *\*/10*.

El parámetro schedule permite indicar un listado de ellos, para así poderlo ejecutar en base a diferentes planificaciones.

Se pueden añadir tantos *bloques job* como se desee. Cada uno se lanza en un proceso independiente, por lo que su ejecución no interfiere
con la ejecución de komlogd, tan solo hay que tener en cuenta que para proteger al sistema, **komlogd no planificará la ejecución un job
hasta que la ejecución anterior de ese mismo job haya terminado**. Por ejemplo, si tengo un job cuya ejecución se demora 10 minutos y lo planifico para que
se ejecute cada 5 minutos, komlogd no lo lanzará con la frecuencia configurada.

Carga de jobs desde archivo externo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

En algunas ocasiones nos puede interesar que komlogd cargue los jobs a ejecutar desde un archivo
externo, en lugar de añadirlos directamente en el archivo *komlogd.yaml*

Para ello, editamos el archivo *komlogd.yaml* y realizamos lo siguiente:

1. Habilitamos la opción que permite cargar jobs desde un archivo externo::

    - allow_external_jobs: yes

2. Por cada fichero de jobs, añadimos lo siguiente::

    - external_job_file: <path_to_file>

sustituyendo *<path_to_file>* por la ruta del archivo que contiene el listado de *bloques job*
que queremos ejecutar.

Podemos añadir tantos bloques *external_job_file* al archivo *komlogd.yaml* como queramos.

Configuración de funciones de transferencia
-------------------------------------------

Una *función de transferencia* es una función que se ejecuta cuando se actualizan los
datos de una o varias *uris*.

Esta funcionalidad la podemos utilizar para automatizar tareas, generación de alarmas,
comunicación con servicios externos, análisis de datos en tiempo real y, en definitiva, cualquier tarea
que se nos ocurra que pueda estar asociada a eventos.

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

Al igual que en el caso de los jobs, se pueden añadir tantos *bloques transfers* como se desee.

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


.. _api:

API
===

Komlogd puede utilizarse como librería en otras aplicaciones, para así
integrar en ellas la funcionalidad ofrecida por Komlog.

.. note::

   komlogd utiliza `asyncio <https://docs.python.org/3/library/asyncio.html>`_ internamente,
   por lo que es necesario que la aplicación donde se integre corra un loop de
   asyncio para su funcionamiento.

Inicio de sesión en Komlog
--------------------------

Para establecer una sesión con komlog necesitamos:

* Un usuario de acceso.
* Una clave RSA.

.. note::

    Recuerda que komlogd durante su :ref:`primera_ejecucion` crea la clave RSA
    para la comunicación con Komlog.
    En caso de querer utilizar otra clave RSA, ésta debe ser mayor o igual a
    4096 bytes.

Para inicializar una sesión en Komlog bastaría con realizar lo siguiente:

.. code:: python

    import asyncio
    from komlogd.api import session, crypto

    loop = asyncio.get_event_loop()

    async def example():
        username='username'
        privkey=crypto.load_private_key('/path/to/key.priv')
        komlog_session=session.KomlogSession(username=username, privkey=privkey)
        await komlog_session.login()
        # En este punto la sesión estaría establecida.
        # Si necesitamos que nuestra tarea espere mientras
        # la sesión está establecida:
        # await komlog_session.t_loop

        # Para cerrar la sesión:
        await komlog_session.close()

    try:
        loop.run_until_complete(example())
    except:
        loop.stop()
    loop.close()


Los pasos son:

* Establecemos el usuario de la sesión.
* Cargamos la clave privada del agente, llamando a la función *crypto.load_private_key()*
  pasándole como parámetro la ruta del archivo que la contiene.
* Inicializamos un objeto *KomlogSession*, que será el encargado de contener nuestra
  sesión con Komlog, pasándole como parámetros el usuario y la clave privada.
* Llamamos al método *login()*. Este método crea una tarea en el loop de asyncio que será
  la encargada de realizar login en el servidor y mantener nuestra sesión con Komlog.
  Hasta que no se ejecute el loop de asyncio no se realizará login en Komlog.
* Por último arrancamos el loop de asyncio. En este momento se ejecutará la tarea
  de login en Komlog.

A partir de ese momento, siempre y cuando el login haya sido correcto,
tendríamos una sesión establecida con Komlog, y estaríamos
preparados para enviar y recibir información.

Internamente, komlogd establece con el servidor una conexión mediante *web sockets*, por lo
que la sesión se mantiene establecida en todo momento. En caso de recibir una desconexión,
la propia librería se encarga de restablecerla.

.. important::
    Para que el login sea correcto, hay que autorizar previamente
    la clave pública de komlogd, como se explica en :ref:`autorizacion_agente`.

    La clave pública se puede obtener a partir de la clave privada, ese es el motivo
    por el cual nunca tienes que introducir la clave pública como parámetro, sino la privada,
    pero komlogd **nunca** enviará la clave privada a Komlog.

    Ante un inicio de sesión fallido porque la clave pública no ha sido autorizada, komlogd
    la muestra en los logs, para que el usuario vea cual debe autorizar.

Envío de datos a Komlog
-----------------------

En Komlog el usuario organiza sus métricas en una estructura en árbol en la que cada métrica
está identificada unívocamente por lo que llamamos *uri*. A esta estructura la llamamos *el modelo
de datos del usuario*. A los diferentes valores que puede tomar una métrica a lo
largo del tiempo se les conoce como *samples*.

En el modelo de datos del usuario se pueden crear métricas de los siguientes tipos:

* **Datasource**
* **Datapoint**

Komlog crea las métricas automáticamente la primera vez que subimos datos de ellas.
El tipo de datos de una métrica se establece cuando ésta se crea, y no hay posibilidad de cambiarlo
mientras exista. Para modificar el tipo de dato habría que borrar la métrica (lo que implicaría
borrar todas las muestras de datos recibidas de ella) y volverla a crear con un tipo diferente.

**Métricas de tipo Datasource**

Una métrica de tipo Datasource se utiliza para almacenar textos. Podemos subir cualquier texto
de una longitud máxima de 130KB.

Para enviar a Komlog un sample de un Datasource se podría hacer de la siguiente manera:

.. code:: python

    import asyncio
    import pandas as pd
    from komlogd.api import session, crypto
    from komlogd.api.model.orm import Datasource, Sample

    loop = asyncio.get_event_loop()

    async def send_datasource_sample():
        # establecemos la sessión
        username='username'
        privkey=crypto.load_private_key('/path/to/key.priv')
        komlog_session=session.KomlogSession(username=username, privkey=privkey)
        await komlog_session.login()

        # preparamos el sample
        uri='my_datasource'
        data='Datasource content'
        ts = pd.Timestamp('now',tz='Europe/Madrid')
        metric = Datasource(uri=uri)
        sample = Sample(metric=metric, ts=ts, data=data)

        # enviamos sample y cerramos sessión
        komlog_session.send_samples(samples=[sample])
        await komlog_session.close()

    try:
        loop.run_until_complete(send_datasource_sample())
    except:
        loop.stop()
    finally:
        loop.close()


**Métricas de tipo Datapoint**

Una métrica de tipo Datapoint se utiliza para almacenar valores numéricos.
Se aceptar variables de tipo int, float o `Decimal <https://docs.python.org/3/library/decimal.html>`_ (de estas últimas sólamente las que tienen representación numérica, es decir, no se aceptan valores como *infinity*, *-infinity*, *NaN*, etc).

En el siguiente ejemplo se muestra como enviar un par de samples asociados a dos métricas
de tipo Datapoint:

.. code:: python

    import asyncio
    import pandas as pd
    from komlogd.api import session, crypto
    from komlogd.api.model.orm import Datapoint, Sample

    loop = asyncio.get_event_loop()

    async def send_datapoint_samples():
        # establecemos la sesión
        username='username'
        privkey=crypto.load_private_key('/path/to/key.priv')
        komlog_session=session.KomlogSession(username=username, privkey=privkey)
        await komlog_session.login()

        # preparamos samples
        samples = []
        ts = pd.Timestamp('now',tz='Europe/Berlin')
        metric1 = Datapoint(uri='cpu.system')
        metric2 = Datapoint(uri='cpu.user')
        samples.append(Sample(metric=metric1, ts=ts, data=14.63))
        samples.append(Sample(metric=metric2, ts=ts, data=28.5))

        # enviamos samples y cerramos sesión
        komlog_session.send_samples(samples=samples)
        await komlog_session.close()

    try:
        loop.run_until_complete(send_datapoint_samples())
    except:
        loop.stop()
    finally:
        loop.close()


Komlog considera las diferentes métricas del modelo de datos del usuario como series temporales independientes, por lo que cuando subimos datos a alguna de nuestras métricas siempre hay que asociarle un *timestamp*.
**El usuario es el encargado de establecer el timestamp, por lo que
el valor del timestamp no tiene por qué coincidir con el del momento en el que se envían los datos.**

El timestamp que asociamos al contenido de una métrica puede ser de los siguientes tipos:

* tipo *pandas.Timestamp*
* tipo *datetime.datetime*
* tipo string en formato ISO8601

Hay que tener en cuenta que **es necesario incluir la zona horaria y que la
precisión máxima aceptada es de milisegundos**.


.. _funciones_de_transferencia:

Funciones de transferencia
--------------------------

En komlogd existe la posibilidad de ejecutar una función cada vez que llegue una muestra de una métrica. A este
tipo de funciones les denominamos *funciones de transferencia*.

Para crear una función de transferencia simplemente hay que aplicarle el decorador *@transfermethod*. Este decorador admite los
siguientes parámetros:

* **uris**: lista con las uris de las métricas a las que queremos suscribirnos.
* **data_reqs**: objeto de tipo DataRequirements, donde le indicamos los requisitos a nivel de datos que tiene la función para
  su correcta ejecución.
* **min_exec_delta**: objecto tipo pandas.Timedelta. Este parámetro indica el periodo mínimo entre ejecuciones de la función. Por defecto, komlogd ejecutará
  la función de transferencia cada vez que se reciban muestras en los métricas suscritas, sin embago, este comportamiento puede no siempre
  ser el deseado, por lo que este parámetro indica a komlogd que entre ejecución y ejecución al menos debe haber pasado el tiempo especificado.

El siguiente código muestra como se crearía una función de transferencia:

.. code:: python

    from komlogd.api.transfer_methods import transfermethod

    @transfermethod(uris=['cpu.system','cpu.user'])
    async def example():
        print('hello komlog.')


En el ejemplo anterior, cada vez que se actualicen las métricas *cpu.system* y *cpu.user* komlogd ejecutaría la función *example*.
Como se puede ver, example es una corrutina. **El decorador @transfermethod puede aplicarse tanto a funciones normales o como a corrutinas**.


Una función de transferencia puede provocar la actualización de métricas en nuestro modelo de datos. Para ello debe devolver
una lista con los samples que se deben enviar a Komlog:

.. code:: python

    from komlogd.api.transfer_methods import transfermethod
    from komlogd.api.model.orm import Datasource, Sample

    @transfermethod(uris=['cpu.system','cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    def summary(ts, updated, data, others):
        result={'samples':[]}
        for metric in updated:
            int_data=data[metric][ts-pd.Timedelta('60 min'):ts]
            info=str(int_data.describe())
            stats = Datasource(uri='.'.join((metric.uri,'last_60min_stats')))
            sample = Sample(metric=stats, data=info, ts=ts)
            result['samples'].append(sample)
        return result

En el ejemplo anterior nos suscribimos a las métricas *cpu.system* y *cpu.user* y realizamos una serie de cálculos estadísticos sobre
sus datos de la última hora. Posteriormente se escriben los resultados en las métricas *cpu.system.last_60min_stats* y *cpu.user.last_60min_stats*.
La función se ejecutará cada vez que se reciban datos.

A continuación la comentamos en detalle.

* En primer lugar aplicamos a la función *summary* el decorador *@transfermethod* con los siguientes parámetros:
    * **uris=['cpu.system','cpu.user']**. Con este parámetro indicamos que la función se suscribe a las métricas *cpu.system* y
      *cpu.user*.
    * **data_reqs=DataRequirements(past_delta=pd.Timedelta('1h'))**. Aquí le indicamos que la función necesita 1 hora de datos
      para su correcta ejecución.
* La función *summary* recibe una serie de parámetros (Es opcional que nuestra función reciba estos parámetros. Si no los va a necesitar no hace falta que los definamos):
    * **ts**: objecto pandas.Timestamp. Es el timestamp de los samples que provocaron la ejecución de la función.
    * **updated**: Lista de métricas actualizadas en esta ejecución.
    * **data**: Diccionario que contiene una key por cada una de las métricas suscritas. El valor es un objeto tipo pandas.Series, con los datos de la métrica.
    * **others**: Lista con métricas a los que está suscrita la función pero que no han provocado la ejecución actual.
* En la primera línea de la función *summary* declaramos el diccionario result. Éste contiene la clave *samples* que será la que almacene los samples a enviar a Komlog.
* A continuación, por cada una de las métricas que se han actualizado hacemos lo siguiente:
    * Obtenemos los datos de la última hora.
    * Obtenemos el resultado al aplicarles la función *describe()* (Esta es una función del módulo pandas que obtiene una serie de valores estadísticos sobre una serie).
    * Creamos un Datasource cuya *uri* será la de la métrica + *.last_60min_stats*, es decir, crearíamos **cpu.system.last_60_min_stats** y **cpu.user.last_60_min_stats**.
    * Creamos un Sample del datasource y establecemos los datos y el timestamp.
    * Añadimos el sample al listado de samples del diccionario result.
* Por último la función devuelve el diccionario *result*, con los samples a enviar a Komlog.

Se puede aplicar el decorador *transfermethod* a una función tantas veces como se necesite, simplemente apilando las llamadas al mismo. Por ejemplo, en la función anterior, si quisiésemos aplicar la función a dos grupos de métricas diferentes, bastaría con aplicar el decorador a la función una vez por cada grupo de métricas en lugar de crear dos funciones para la misma operación.

.. code:: python

    @transfermethod(uris=['host1.cpu.system','host1.cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    @transfermethod(uris=['host2.cpu.system','host2.cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    def summary(ts, updated, data, others):
        ...

Trabajando con métricas remotas
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Komlog permite compartir partes del modelo de datos con otros usuarios.

.. note::

    Esta funcionalidad está accesible desde el menú de configuración web de Komlog. Hay que tener
    en cuenta que los datos **siempre se comparten en modo de sólo lectura y de forma recursiva**,
    es decir, si comparto la métrica *cpu.system* estaría compartiendo dicha métrica y todas sus
    métricas anidadas en el modelo de datos del usuario, sin importar si ya existían o no en el
    momento de compartirla.

    Al compartir las métricas en modo solo lectura, si una *función de transferencia* trata de actualizar
    una métrica remota, dicha actualización fallará. El usuario **sólo puede modificar su
    modelo de datos**.

Esta funcionalidad permite la creación de aplicaciones que utilicen modelos de datos distribuidos.
La forma para indicar una métrica remota es anteponer el usuario al nombre de la uri::

    uri_remota = 'user:uri'

Por ejemplo, si el usuario *my_friend* nos compartiese la uri *host1.cpu*, podríamos crear
una función de transferencia que se suscribiese a *host1.cpu.system* y *host1.cpu.user* de la siguiente forma:

.. code:: python

    @transfermethod(uris=['my_friend:host1.cpu.system','my_friend:host1.cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    def summary(ts, updated, data, others):
        ...

Un transfer method se puede suscribir a métricas propias y remotas a la vez:

.. code:: python

    uris = [
        'my_friend:host1.cpu.system',
        'my_friend:host1.cpu.user',
        'host1.cpu.system',
        'host1.cpu.user'
    ]

    @transfermethod(uris=uris, data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    def summary(ts, updated, data, others):
        ...


.. _api:

API
===

komlogd can be used as a library to add Komlog functionalities into your applications.

.. note::

   komlogd uses `asyncio <https://docs.python.org/3/library/asyncio.html>`_ internally,
   so your application must run an asyncio loop to be able to use it.

Session initialization
----------------------

To stablish a Komlog session we need:

* Komlog user.
* RSA key.

.. note::

    komlogd creates a RSA key during its :ref:`first_execution`.

To open a session with Komlog, create a KomlogSession object with your username
and private key and call the *login* method:

.. code:: python

    import asyncio
    from komlogd.api import session, crypto

    loop = asyncio.get_event_loop()

    async def example():
        username='username'
        privkey=crypto.load_private_key('/path/to/key.priv')
        komlog_session=session.KomlogSession(username=username, privkey=privkey)
        await komlog_session.login()
        # At this point session is stablished.
        # If you need this task to wait in the loop while session is open:
        # await komlog_session.t_loop

        # To close the session:
        await komlog_session.close()

    try:
        loop.run_until_complete(example())
    except:
        loop.stop()
    loop.close()

komlogd stablishes a web sockets connection with Komlog and internally manages reconnections.

.. important::
    You need to authorize your agent previously on Komlog web, as explained
    in :ref:`agent_authorization`.


Sending data to Komlog
----------------------

Every metric you upload to Komlog is identified with its *uri*. Based on this uri, Komlog
organize your metrics in a tree like structure. We call this structure *the user's data model*.
Every metric in your data model can take different values at different timestamps. We call each of
these values a *sample*.

You can create two types of metrics in Komlog:

* **Datasource**
* **Datapoint**

A metric is created automatically the first time you upload data to it and its type is stablished at this moment too.
You cannot modify the metric type. If you need to modify it, you have to delete the metric
and create it again with a different type.

**Datasource metrics**

A datasource is used to store texts. You can upload any text whose length is less or equal 130KB.

In this example you can see how to send a sample of a datasource metric:

.. code:: python

    import asyncio
    import pandas as pd
    from komlogd.api import session, crypto
    from komlogd.api.model.orm import Datasource, Sample

    loop = asyncio.get_event_loop()

    async def send_datasource_sample():
        # connect to Komlog
        username='username'
        privkey=crypto.load_private_key('/path/to/key.priv')
        komlog_session=session.KomlogSession(username=username, privkey=privkey)
        await komlog_session.login()

        # prepare datasource sample
        uri='my_datasource'
        data='Datasource content'
        ts = pd.Timestamp('now',tz='Europe/Madrid')
        metric = Datasource(uri=uri)
        sample = Sample(metric=metric, ts=ts, data=data)

        # send sample and exit
        komlog_session.send_samples(samples=[sample])
        await komlog_session.close()

    try:
        loop.run_until_complete(send_datasource_sample())
    except:
        loop.stop()
    finally:
        loop.close()


**Datapoint metrics**

A datapoint metric is used to store numerical values. You can use any type of numerical variables,
like *int*, *float* or `Decimal <https://docs.python.org/3/library/decimal.html>`_ (we don't
support values without numerical representation like *infinity*, *NaN*, etc).

In this fragment you can see how to send two samples associated to a pair of datapoint metrics:

.. code:: python

    import asyncio
    import pandas as pd
    from komlogd.api import session, crypto
    from komlogd.api.model.orm import Datapoint, Sample

    loop = asyncio.get_event_loop()

    async def send_datapoint_samples():
        # connect to Komlog
        username='username'
        privkey=crypto.load_private_key('/path/to/key.priv')
        komlog_session=session.KomlogSession(username=username, privkey=privkey)
        await komlog_session.login()

        # prepare datapoint samples
        samples = []
        ts = pd.Timestamp('now',tz='Europe/Berlin')
        metric1 = Datapoint(uri='cpu.system')
        metric2 = Datapoint(uri='cpu.user')
        samples.append(Sample(metric=metric1, ts=ts, data=14.63))
        samples.append(Sample(metric=metric2, ts=ts, data=28.5))

        # send samples and exit
        komlog_session.send_samples(samples=samples)
        await komlog_session.close()

    try:
        loop.run_until_complete(send_datapoint_samples())
    except:
        loop.stop()
    finally:
        loop.close()


Every metric in your *data model* is considered an independent time serie, so every sample you upload
must be associated with a timestamp. **The timestamp is set by the user, so you can upload samples
associated with a timestamp in the past or in the future.**

The timestamp can be any of these types:

* A *pandas.Timestamp* object.
* A *datetime.datetime* object.
* A ISO8601 formatted string.


**It is mandatory to include the time zone and maximum precision is milliseconds**.

.. _transfer_methods:

Transfer methods
---------------

You can execute a function every time a metric is updated. We call this type of functions *transfer methods*.

To create transfer methods, simply add the *@transfermethod* decorator to the function. You can pass the following
parameters to the decorator:

* **uris**: list with metrics uris we want to subscribe our method to.
* **data_reqs**: it needs a DataRequirements object. With this parameter you set the method data requirements by uri.
* **min_exec_delta**: min time between execs. By default, komlogd will run the method every time a metric is updated. With this parameter you can tell
  komlogd to run it at most once in *min_exec_delta* interval. The parameter needs a pandas.Timedelta object.

Here you can see how to create an transfer method:

.. code:: python

    from komlogd.api.transfer_methods import transfermethod

    @transfermethod(uris=['cpu.system','cpu.user'])
    async def example():
        print('hello komlog.')


In this example, every time metrics *cpu.system* and *cpu.user* are updated, komlogd will run the function *example*.
**You can apply the @transfermethod decorator to normal functions or coroutines**.

A transfer method can fire an update to any metric in our data model. For this, the method must return a dictionary
with the samples to send to Komlog:

.. code:: python

    from komlogd.api.transfer_methods import transfermethod
    from komlogd.api.model.orm import DataRequirements, Datasource, Sample

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

In this example we subscribe our summary function to metrics *cpu.system* and *cpu.user* and make some operations over their
last hour data. Finally, we fire updates to metrics *cpu.system.last_60min_stats* and *cpu.user.last_60min_stats* with the data obtained
from that operations. This method will run every time *cpu.system* or *cpu.user* are updated.

Here, we explain the last example in detail:

* First, we apply to our *summary* function the decorator *@transfermethod* with these parameters:
    * **uris=['cpu.system','cpu.user']**. We set the metrics the method is subscribed to.
    * **data_reqs=DataRequirements(past_delta=pd.Timedelta('1h'))**. Here we indicate the method needs a data interval of 1h for each metric.
* *summary* function receives some parameters (These parameters are **optional**. We have to declare them only if we need them.):
    * **ts**: pd.Timestamp object. The timestamp of the sample that fired the transfer method execution.
    * **updated**: list of metrics updated.
    * **data**: A dictionary whose keys are the subscribed metrics. Each value of the data dictionary is a pandas.Series object with the values of the metric.
    * **others**: list with metrics subscribed but not updated in this execution. updated + others will be all subscribed metrics.
* In the first method line, we declare variable result. this variable is a dict with the key *samples*. In this key we will store the samples to fire after the method execution.
* Next, for every updated metric we do:
    * Get the data from the last hour.
    * Store the result of applying the *describe* function to the data.(This function is from pandas module. It calculates some statistical values from a pandas.Series object).
    * Create a datasource whose *uri* is metric uri + *.last_60min_stats*, so we are creating **cpu.system.last_60_min_stats** and **cpu.user.last_60_min_stats**
    * Create a datasource Sample and set data and timestamp.
    * Append the sample to the samples key in the result dictionary.
* Return the result dictionary with the samples to send to Komlog.


You can stack the *transfermethod* decorator as many times as you need. A transfer method will be created
each time the decorator is applied. So for the previous example, if we want to create transfer methods for two
groups of metrics, we can decorate the function once per group instead of creating two functions for the same
operation.

.. code:: python

    @transfermethod(uris=['host1.cpu.system','host1.cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    @transfermethod(uris=['host2.cpu.system','host2.cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    def summary(ts, updated, data, others):
        ...

Working with remote uris
^^^^^^^^^^^^^^^^^^^^^^^^

Users can share metrics through Komlog in real time with other users.

.. note::

    You can share metrics through your `Komlog configuration page <https://www.komlog.io/config>`_.
    Keep in mind that metrics **will always be shared read only and recursively**, this means that
    if you share metric *cpu.system* every nested metric in the data model tree will
    be shared too, no matter if them already existed or not when the root metric was
    shared.

    Sharing metrics read only means a *transfer method* cannot modify any remote metric, so
    if they return samples to update remote metrics, they will be dropped. Users **can only
    modify their own data model.**

With this functionality you can create applications based on distributed data models. The way to
tell komlogd you want to subscribe to a remote uri is prepending the username to the local uri name::

    remote_uri = 'user:uri'

For example, if user *my_friend* were sharing metrics *host1.cpu*, we could subscribe a
transfer method to *host1.cpu.system* and *host1.cpu.user* this way:

.. code:: python

    @transfermethod(uris=['my_friend:host1.cpu.system','my_friend:host1.cpu.user'], data_reqs=DataRequirements(past_delta=pd.Timedelta('1h')))
    def summary(ts, updated, data, others):
        ...

A transfer method can subscribe to both local and remote metrics:

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


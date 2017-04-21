komlogd
=======

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/komlog_/komlog
   :target: https://gitter.im/komlog_/komlog?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Komlogd is the agent for communicating with `Komlog <http://www.komlog.io>`_,
a SaaS tool for time series visualization and analysis.

Komlog's goal is to help SREs and Devops:

- Create graphs quickly from plain texts (forget about parsing commands or scripts outputs).
- Share data easy and precisely (avoid sharing data through emails, csv files, etc).
- Monitor and analyze data, building powerful real-time applications adapted to your needs and easily extendable.

.. image:: https://cloud.githubusercontent.com/assets/2930882/25127033/a6a66c14-2434-11e7-9852-b5bac6cd38dc.png
   :alt: Komlog home page
   :target: https://cloud.githubusercontent.com/assets/2930882/25127033/a6a66c14-2434-11e7-9852-b5bac6cd38dc.png


Getting started
---------------


1. Install komlogd
^^^^^^^^^^^^^^^^^^

First step is installing komlogd on your host. You can install it with pip:

.. code::

    pip install komlogd

komlogd depends on `python pandas <http://pandas.pydata.org/>`_.
It is easier to install pandas using your distribution package manager,
so maybe you prefer to do it that way. If you want to install everything with pip,
just run the previous command and `all dependencies will be installed automatically <http://komlogd.readthedocs.io/en/latest/install.html#automatic-install>`_.

2. Initialize komlogd
^^^^^^^^^^^^^^^^^^^^^

Once installed, proceed to `run komlogd for the first time <http://komlogd.readthedocs.io/en/latest/install.html#first-execution>`_. During this run, it will
create the configuration file, the private and public keys and it will exit.

.. code::

    komlogd


The **configuration file** created should be stored at *$HOME/.komlogd/komlogd.yaml* and the **public key** at *$HOME/.komlogd/key.pub*

3. Add your username to the configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Edit your configuration file and set your `Komlog <https://www.komlog.io>`_ username.

.. code::

   - username: my_username


4. Add the public key to Komlog authorized keys
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Go to your `Komlog Configuration Page <https://www.komlog.io/config>`_ and add the contents of
your recently created public key file to the `authorized list <http://komlogd.readthedocs.io/en/latest/configuration.html#komlogd-authorization>`_.


5. Sending data
^^^^^^^^^^^^^^^

**Sending data is as simple as passing any file or command output to komlogd stdin.**

Choose the command you want (*df -k* in this example), and redirect its output to komlogd.

.. code::

    df -k | komlogd -u host.occupation

The -u parameter is mandatory, it associates the data sent to a *path* in your user's data model.

At this point, you should see the data at your `Komlog Home Page <https://www.komlog.io/home>`_,
where you can identify variables, share it with other users, create charts, etc.

.. image:: https://cloud.githubusercontent.com/assets/2930882/25123424/96c7121e-2428-11e7-8db2-7cdcd75345dc.png
   :alt: Identify Datapoint Image
   :target: https://cloud.githubusercontent.com/assets/2930882/25123424/96c7121e-2428-11e7-8db2-7cdcd75345dc.png


Aditional functionality
-----------------------

komlogd can run in daemon mode. In this mode, komlogd offers additional functionality:

- Configure and schedule script or command executions, and send their outputs to Komlog.

- Subscribe to elements in your data model, and execute user defined functions
  when your data model changes. This can be used, for example,
  for alerting, anomaly detection, external service notification, automate reports generation, etc.

komlogd can also be used as a module. Import komlogd in your applications and add its functionality to them.

Check out the documentation for more info and examples about how to use komlogd in these scenarios:

- `English docs <https://komlogd.readthedocs.io>`_.
- `Spanish docs <https://komlogd-es.readthedocs.io>`_.

Contact us by `chat <https://gitter.im/komlog_/komlog?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge>`_ if you need
help.


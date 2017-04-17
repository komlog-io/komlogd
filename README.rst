komlogd
=======

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/komlog_/komlog
   :target: https://gitter.im/komlog_/komlog?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

Komlogd is the agent for communicating with `Komlog platform <http://www.komlog.io>`_, an event-based
execution platform aimed at time series visualization and analysis.

Komlog is built for SREs and Devops who love working with command line interface tools,
but who need something graphical to:

- Create graphs quick and easily from plain texts (Forget about parsing command or script outputs).
- Share or use data from others easy and precisely (No more excels, mails, csvs, etc for sharing data).
- Monitor and analyze data, building powerful real-time applications adapted to their needs and easily extendable.


Getting started
---------------

Lets send our first data to Komlog.

1. Install komlogd and dependencies.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can install komlogd with pip:

.. code::

    pip install komlogd

komlogd depends on `python pandas <http://pandas.pydata.org/>`_.
It is easier to install pandas using your distribution package manager,
so maybe you prefer to do it that way. If you want to install everything with pip,
just run the previous command and `all dependencies will be installed automatically <http://komlogd.readthedocs.io/en/latest/install.html#automatic-install>`_.

2. Create configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once installed, proceed to `run komlogd for the first time <http://komlogd.readthedocs.io/en/latest/install.html#first-execution`_. During this run, it will
create configuration file, private and public keys and finally it will exit.

.. code::

    komlogd


Configuration file created should be at *$HOME/.komlogd/komlogd.yaml* and public key at *$HOME/.komlogd/key.pub*

3. Edit configuration file and add your username
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Edit configuration file and modify *username* block with your username.

.. code::

   - username: my_username


4. Add komlogd public key to Komlog authorized keys
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Go to `Komlog Configuration Page <https://www.komlog.io/config>_` and add the contents of
your recently created public key file to the `authorized list <http://komlogd.readthedocs.io/en/latest/configuration.html#komlogd-authorization>`_.


5. Start sending data
^^^^^^^^^^^^^^^^^^^^^

Choose the command you want (*df -k* in this example), and redirect its output to komlogd.
The -u parameter associates the content to a *path* in your user's data model.

.. code::

    df -k | komlogd -u host.occupation

At this point, you can see the content at your `Komlog Home Page <https://www.komlog.io/home>`_.
Now, you can identify variables on it, share it with other users, create charts, etc.

Use Cases
---------

komlogd has some common use cases:

- Run komlogd in daemon mode, schedule periodic command executions, and send their outputs to Komlog.
- Subscribe to elements in your data model, and build an event-based system,
  executing functions when your data model changes.
- Import komlogd as a module in your applications and add its functionality to them.

Documentation
-------------

Check out the documentation for more info:

- `English docs <https://komlogd.readthedocs.io>`_.
- `Spanish docs <https://komlogd-es.readthedocs.io>`_.

or contact us at our `chat <https://gitter.im/komlog_/komlog?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge>`_.


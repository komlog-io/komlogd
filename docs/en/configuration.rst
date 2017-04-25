.. _configuration:

Configuration
=============

Komlog Access Configuration
---------------------------

Once komlogd file and directory structure is initialized, as explained at
:ref:`first_execution`, is time to proceed to komlogd configuration so it can communicate
with `Komlog <http://www.komlog.io>`_.

User and RSA Key Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To access Komlog we need to set username and RSA key in komlogd configuration file (**komlogd.yaml**).

The username is set with this key::

    - username: <username>

Replacing *<username>* with our Komlog username.

komlogd RSA key is generated during the first execution, and placed in ``$HOME/.komlogd/key.priv`` by default. komlogd will use this key for the authentication process. However, if we want to store the key in another file, or use another key, we should set the following key::

    - keyfile: <path_to_key.priv>

Replacing *<path_to_key.priv>* with the filename and full path of our key.

.. note::
    komlogd private key will never be sent to Komlog.

.. _agent_authorization:

komlogd authorization
^^^^^^^^^^^^^^^^^^^^^

You have to add komlogd's public key to the list of authorized keys so that komlogd can succeed
in the authentication process. To do it, follow these steps:

* Access `configuration page <https://www.komlog.io/config>`_ with your Komlog user.
* Go to ``Agents`` menu and click ``New Agent`` button.
* A form will be shown with two fields. The first one is to set the name of the agent, and the second one to paste the contents of its public key. You can find the public key in file ``$HOME/.komlogd/key.pub``.


.. image:: _static/new_key.png

Sending data to Komlog for the first time
-----------------------------------------

Let's send data to Komlog for the first time.
In this example, we are going to send the *df -k* command's output, so we can monitor
disk occupation in our host later. In Komlog, users organize their data in a tree like structure called
the **data model**. Every element in the user's data model is identified by its **uri**.
In this example, we are going to identify our occupation data with the uri *host.occupation*.

To upload the data, we execute the following::

    df -k | komlogd -u host.occupation

If everything went right, we should see the data in our `Komlog home page <https://www.komlog.io/home>`_,
associated to the uri *ĥost.occupation*
in our data model. Now, we can identify variables, send the content to other users, automate other
tasks based on this content, etc.

We can now schedule the previous command to execute periodically in cron, systemd or in our favourite scheduler,
and metrics will be identified automatically on every sample we upload.

We can nest our metrics in different levels using the dot character (.) For example, if we have uris *system.info.disk*, *system.info.disk.sda1* and *system.info.disk.sda2*, Komlog will nest them this way::

    system
    └─ info
       └─ disk
          ├─ sda1
          └─ sda2

.. important::
    An uri can be formed **only** with this characters:

    * Capital or lowercase letters [A-Za-z]: ASCII characters from *A* to *z*.
    * Numbers [0-9].
    * Special characters:

        * Hyphen (-), underscore (_)
        * Dot (.)

    An uri **cannot** start with dot (.).

Transfer methods configuration
------------------------------

Komlog allows users to subscribe to any *uri* from their data model and execute a function when
new data is received or updated. We call this functions **transfer methods**.

With *transfer methods* you can automate tasks, generate alarms, communicate with external
services, analyze data in real time, and basically any task associated to events.

This functionality allows you to build a lambda-based systems architecture.

On chapter :ref:`transfer_methods` we explain how to create this type of functions.

To add a file with transfer methods to komlogd configuration we use the **transfers** block::

    - transfers:
        enabled: yes
        filename: <path_to_file>

The *transfers block* parameters are:

* **enabled**: To enable or disable the block. Can take values *yes* or *no*.
* **filename**: Path to the transfer methods file. Path can be absolute or relative to the komlogd configuration directory.

You can add as many *transfers blocks* as you need.

Log configuration
-----------------

komlogd logs configuration is stablished with the *logging* block in the configuration file.
It has these default values::

    - logging:
        level: INFO
        rotation: yes
        max_bytes: 10000000
        backup_count: 3
        dirname: log
        filename: komlogd.log

*logging block* parameters are:

* **level**: Sets the log level. Posible values are *CRITICAL, ERROR, WARNING,
  INFO, DEBUG, NOTSET*.
* **rotation**: Indicates if log file will be rotated when its size reaches *max_bytes* bytes. It accepts values *yes* or *no*.
* **max_bytes**: If log rotation is enabled, log file will be rotated when it reaches the size in bytes indicated by this parameter.
* **backup_count**: Number of log rotated files to keep on disk.
* **dirname**: Log file directory. Path can be absolute or relative to komlogd configuration directory.
* **filename**: log file name.

Once komlogd is configured, we can start it by executing the following::

    komlogd &



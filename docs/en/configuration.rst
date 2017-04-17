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

If everything went right, we should see the data in Komlog web, associated to the uri *ĥost.occupation*
in our data model. Now, we can identify variables, send the content to other users, automate other
tasks based on this content, etc.


Scheduled jobs configuration
----------------------------

komlogd lets you schedule command executions and send their outputs to Komlog.
This functionality allows you to periodically send commands or scripts outputs to Komlog.

Suppose we want to send to Komlog the result of executing the command **df -k** every hour.

To accomplish that, we should add to komlogd's configuration file (**komlogd.yaml**) the
following **job** block::

    - job:
        uri: system.info.disk
        command: df -k
        enabled: yes
        schedule:
            - '0 * * * *'

A *job block* is defined with the following parameters:

* **uri**: the identifier associated with this job's data.

Its like a path in user's data. Every user in Komlog can organize her time series in a tree like structure, that we call the *data model* (like a file system). Every element in the data model is identified by the **uri** (like the element path).

We can nest our information in different levels using the dot character (.) For example, if we have uris *system.info.disk*, *system.info.disk.sda1* and *system.info.disk.sda2*, Komlog will nest them this way::

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

* **command**: The command to execute.

It can be an operating system command or a script. komlogd will send the command/script standard output to Komlog.

.. important::
    The command parameter cannot contain special command line characters like **pipes (|)** or **redirections (<,>)**.
    If you need them, create a script and include the command there.

* **enabled**: To enable or disable the job. Can take values *yes* or *no*.

* **schedule**: Sets the job execution schedule. Uses the classical UNIX cron format::

         ┌───────────── minutes (0 - 59)
         │ ┌────────────── hours (0 - 23)
         │ │ ┌─────────────── day of month (1 - 31)
         │ │ │ ┌──────────────── month (1 - 12)
         │ │ │ │ ┌───────────────── day of week (0 - 6) (sunday - saturday)
         │ │ │ │ │
         │ │ │ │ │
         │ │ │ │ │
         * * * * *

It accepts these special characters:

* Asterisk (*) to set every possible value of a group.
* Comma (,) to enumerate different values in a group.
* Slash (/) to set values of a division with zero remainder. So, for example, insted of setting
  minutes to *0,10,20,30,40,50* you can set *\*/10*.

The schedule parameter accepts as many elements as you need.


Every *job block* creates an independent process to manage the job execution, so they don't block each other. However, for
security reasons, **komlogd will not execute more than one instance of each job in parallel**, so if you have a job that takes
10 minutes to complete and it is scheduled to execute every 5 minutes, the schedule will not be fulfilled.

Loading jobs from external files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can tell komlogd to load the jobs configuration from an external file instead of adding them
directly to *komlogd.yaml*.

To achieve this:

1. Enable the external load option in *komlogd.yaml*::

    - allow_external_jobs: yes

2. For each file, add an entry in *komlogd.yaml* like this one::

    - external_job_file: <path_to_file>

Replacing *<path_to_file>* with the file's path.
You can add as many *external_job_file* statements as you need.

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



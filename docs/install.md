## Prerequisites

To install and configure komlogd you will need:

* A Komlog account. You can create one [here](https://www.komlog.io/signup)
* A host with [python 3.5](https://www.python.org) or greater and [pip package](https://pypi.python.org/pypi/pip) installed.

## Installation

komlogd can be easily installed with pip typing:

```
> pip install komlogd
```

## Configuration

After installing komlogd, run it for the first time, so it can create the configuration
file template and the agent public and private keys.

```
> komlogd
```

The agent will exit with an exit message like this:

> Error initializing komlogd.
> Set username in configuration file.
> Log info: ~/.komlogd/log/komlogd.log

During the first execution, komlogd will create a new directory inside user's *$HOME* directory called *.komlogd* with this structure:

```
    .komlogd
    ├── key.priv
    ├── key.pub
    ├── komlogd.yaml
    └── log
        └── komlogd.log
```

Files created are:

* **key.priv**: private RSA key file.

* **key.pub**: public RSA key file.

* **komlogd.yaml**: main configuration file.

* **log/komlogd.log**: log file.

### Add your username to komlogd configuration file

Edit komlogd configuration file (*komlogd.yaml*) and set your username in *username* key:

```
    - username: <username>
```

### Authorize komlogd

We must authorize the agent in our Komlog account. To do it, access your [Komlog configuration page](https://www.komlog.io/config)
and in the *Agents* section create a new agent and paste the contents of your *key.pub* file.

[!New key](img/new_key.png)

> komlogd **private key** will never be sent to Komlog. Keep it safe.




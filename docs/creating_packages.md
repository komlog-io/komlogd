# Creating packages

komlogd packages are developed in [Python](https://www.python.org) and are installed with
[pip](https://pypi.python.org/pypi/pip), so they are like any python package with some peculiarities
that make them pluggable into komlogd.

# Package structure

To illustrate the process of creating a komlogd package, we are going to study an already existing one,
the [official package for monitoring Linux servers](https://github.com/komlog-io/kpack_linux) we have already
used in previous examples.

This package has the same structure as a typical python package. Ignoring the documentation related files, the
package structure is as follows:

```
├── kpack_linux
│   ├── __init__.py
│   ├── load.py
│   └── settings.py
└── setup.py
```

As with every python package, we can differentiate two parts in the previous structure:
* File ***setup.py***, corresponding to the package definition.
* Directory ***kpack_linux***, corresponding to the package code.

## Directory kpack_linux

The package code has three files:

* **__init__.py**: This file contains all the relevant code regarding package functionality, ie the code
for extracting information from the linux server and sending it to Komlog.
In section [design principles](design.md) we explain komlogd SDK in detail.

* **load.py**: This file is the entry point when the package loads. It is executed by komlogd when it loads
the package. The file name (*load.py*) is not mandatory, but it makes easier to analyze the code by third parties.

* **settings.py**: This file stores some package settings. We separate settings from code so you can fork the
project and adapt it to your needs.

## File setup.py

The contents of setup.py (as of v0.1 of the package) are:

```
from setuptools import setup

setup(
    name = 'kpack_linux',
    license = 'Apache Software License',
    packages = ['kpack_linux'],
    version = '0.1',
    entry_points = {
        'komlogd.package': 'load = kpack_linux.load'
    }
)
```

**The reason this package is a komlogd package is the *entry_points* keyword**. In this keyword we specify
the key *komlogd.package* and the value *load = kpack_linux.load*.

* Key *komlogd.package* tells komlogd that kpack_linux is a komlogd package.
* Value *'load = kpack_linux.load'* tells komlogd what to import to load it.

Your komlogd package must implement the previous *machinery* to be a valid one.

You can learn more about the *entry_points* keyword in the
[setuptools documentation](http://setuptools.readthedocs.io/en/latest/setuptools.html#dynamic-discovery-of-services-and-plugins)


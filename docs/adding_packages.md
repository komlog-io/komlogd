# Adding third party packages

komlogd users can add extra functionality to the agent by creating their own packages or using the ones
distributed by other users. If you want to learn how to create a komlogd package, go to the section [Creating packages](creating_packages.md).

In this section we will explain how we can add packages to our agents configuration.

To illustrate this example, we will configure the Komlog official package for monitoring Linux servers: [kpack_linux](https://github.com/komlog-io/kpack_linux).

To add it to komlogd, just edit your komlogd configuration file (*komlogd.yaml*) and add a package block like this one:

```
- package:
    install: https://github.com/komlog-io/kpack_linux/archive/master.zip
    enabled: yes
    venv: default
```

Then, reboot your agent for changes to take effect.

That's all. At this point your agent should be sending data to Komlog regarding your linux server. Check out [kpack_linux page](https://github.com/komlog-io/kpack_linux) if you want to know the data it will send.

## Package block explained

The parameters used to define a package are:

* **install**. This parameter tells komlogd how to install a package. komlogd packages are python packages and,
internally, komlogd uses *pip* to install them. With this parameter, we set the arguments that komlogd will pass
to the command *pip install*, so we can install packages from PyPI, a version control system or anything accepted
by pip. It also accepts any parameter *pip install* does.
Check [pip install documentation](https://pip.pypa.io/en/stable/reference/pip_install/) for additional information.
* **enabled**. This parameter is used to disable the package without removing the entire block.
Set it to *no* to disable it.
* **venv**. komlogd will create a python virtualenv for each package.
This option allows you to isolate your packages one from another, so nothing will interfere between them.
It is usefull if you want to test different versions of the same package too, or create multiple instances
of the same package. This parameter has some *"reserved"* values:
    * *default*. This is the default value too. If *default* is specified, all packages will be installed in a
    virtualenv with name *default*.
    * *unique*. Set venv to *unique* if you want your package to be in an isolated virtualenv, ensuring no other
    package will share virtualenv with it.

